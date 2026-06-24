# -*- coding: utf-8 -*-
"""
thesis_tracker.py — 持仓 thesis 归档 (2026-06-24 新增, 第三层, 可降级)

定位:
  第一层(各 stream)入库 events,第二层(llm_enhancer)写 摘要/情绪/severity,
  本模块是第三层:对"有 thesis 定义的 symbol"的 events,用 LLM 判定
  它命中哪条假设(支柱)+ 支持/削弱/中性 + 一句话理由,写入 thesis_links。

降级:
  - LLM 未配置 → 跳过(同 llm_enhancer 逻辑)
  - 单条失败 → 记 __none__? 不,失败不写,留待下轮重试
  - 与所有假设无关 → 写 assumption_id='__none__' 标记已处理,避免反复调用

版本:
  thesis_version = 某 symbol 假设集的哈希。改了 theses.py 里的假设,
  版本变化 → 该 symbol 的 events 会被自动重判(覆盖旧 link)。

只读配置复用 llm_enhancer 的 .env(LLM_PROVIDER / LLM_API_KEY / ...)。
"""
import os
import json
import time
import hashlib
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional

import requests

from config import PORTFOLIO
from models import get_db
from theses import THESES, get_thesis

# 复用 llm_enhancer 的 LLM 配置(保持单一真源)
from llm_enhancer import (
    is_available,
    LLM_PROVIDER, LLM_API_KEY, LLM_BASE_URL, LLM_MODEL,
    _DEFAULT_MODELS, _DEFAULT_ENDPOINTS,
)

NONE_MARKER = '__none__'   # 已处理但与任何假设无关


# ── thesis 版本(假设集哈希)─────────────────────────────────

def thesis_version(symbol: str) -> str:
    """某 symbol 假设集的短哈希;假设内容变 → 版本变 → 自动重判。"""
    th = THESES.get(symbol)
    if not th:
        return ''
    basis = json.dumps(
        [(a.get('id'), a.get('statement', ''), a.get('breaks_if', ''))
         for a in th.get('assumptions', [])],
        ensure_ascii=False, sort_keys=True,
    )
    return hashlib.sha1(basis.encode('utf-8')).hexdigest()[:12]


# ── symbol 别名(events.symbol 形式不一,做容错映射到 thesis 主键)──

def event_symbol_aliases(canonical: str) -> List[str]:
    """
    某 thesis 主键 → 可能出现在 events.symbol 里的所有写法。
    覆盖: 主键本身 / yf_symbol / akshare_symbol / 去 .HK / 港股前导零变体。
    """
    aliases = {canonical}
    cfg = PORTFOLIO.get(canonical, {})
    for k in ('yf_symbol', 'akshare_symbol'):
        if cfg.get(k):
            aliases.add(cfg[k])
    # 去 .HK 基码
    if canonical.endswith('.HK'):
        base = canonical[:-3]
        aliases.add(base)
        aliases.add(base.lstrip('0'))                 # 09992 → 9992
        aliases.add(base.lstrip('0') + '.HK')         # 9992.HK
        aliases.add(base.zfill(4))                    # 700 → 0700
        aliases.add(base.zfill(4) + '.HK')
    return [a for a in aliases if a]


# ── LLM 调用(thesis 归档专用 prompt)────────────────────────

def _model_and_url():
    model = LLM_MODEL or _DEFAULT_MODELS.get(LLM_PROVIDER, 'glm-4-flash')
    url = LLM_BASE_URL or _DEFAULT_ENDPOINTS.get(LLM_PROVIDER, _DEFAULT_ENDPOINTS['glm'])
    return model, url


def _build_prompt(name: str, symbol: str, assumptions: List[Dict],
                  title: str, body: str) -> str:
    lines = []
    for a in assumptions:
        lines.append(
            f"[{a['id']}] {a['label']} — {a.get('statement','')}"
            f" (被推翻条件: {a.get('breaks_if','无')})"
        )
    assumptions_block = "\n".join(lines)
    return f"""你是价值投资分析助手。下面是某持仓的"投资逻辑假设"清单,以及一条与它相关的新闻/公告。
判断这条信息最相关的是哪一条假设,以及它对该假设是 支持(support) / 削弱(weaken) / 中性(neutral)。
重点对照每条假设的"被推翻条件":若信息指向被推翻条件描述的方向,通常判 weaken;
若强化了假设成立,判 support;只是相关但不改变判断,判 neutral。

持仓: {name} ({symbol})
假设清单:
{assumptions_block}

信息:
标题: {title}
摘要/正文: {body}

输出严格 JSON(不要 markdown,不要解释):
- assumption_id: 上面某个方括号里的 id;若与所有假设都无实质关系,填 "none"
- stance: "support" | "weaken" | "neutral"
- confidence: 0.0~1.0
- rationale: 不超过 30 字中文,说明判断理由
只输出 JSON。"""


def _call_llm(prompt: str) -> Optional[Dict]:
    if not is_available():
        return None
    model, url = _model_and_url()
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {LLM_API_KEY}',
    }
    body = {
        'model': model,
        'messages': [
            {'role': 'system', 'content': '你只输出 JSON.'},
            {'role': 'user', 'content': prompt},
        ],
        'temperature': 0.1,
    }
    try:
        r = requests.post(url, headers=headers, json=body, timeout=20)
        if r.status_code != 200:
            print(f"  [thesis_tracker] {LLM_PROVIDER} HTTP {r.status_code}: {r.text[:160]}")
            return None
        text = r.json()['choices'][0]['message']['content'].strip()
        if text.startswith('```'):
            text = text.strip('`').strip()
            if text.lower().startswith('json'):
                text = text[4:].strip()
        return json.loads(text)
    except Exception as e:
        print(f"  [thesis_tracker] {LLM_PROVIDER} 失败: {type(e).__name__}: {e}")
        return None


def classify_event(symbol: str, title: str, body: str) -> Optional[Dict]:
    """
    对单条 event 做 thesis 归档。返回 {assumption_id, stance, confidence, rationale}
    或 None(LLM 失败,留待重试)。无关时 assumption_id 归一为 NONE_MARKER。
    """
    th = get_thesis(symbol)
    if not th:
        return None
    assumptions = th.get('assumptions', [])
    valid_ids = {a['id'] for a in assumptions}

    prompt = _build_prompt(th['name'], symbol, assumptions, title or '', (body or '')[:800])
    raw = _call_llm(prompt)
    if raw is None:
        return None

    aid = str(raw.get('assumption_id', 'none')).strip()
    if aid not in valid_ids:
        aid = NONE_MARKER
    stance = str(raw.get('stance', 'neutral')).lower().strip()
    if stance not in ('support', 'weaken', 'neutral'):
        stance = 'neutral'
    try:
        conf = float(raw.get('confidence', 0.5))
    except (TypeError, ValueError):
        conf = 0.5
    conf = max(0.0, min(1.0, conf))
    rationale = str(raw.get('rationale', ''))[:120]

    return {
        'assumption_id': aid,
        'stance': stance if aid != NONE_MARKER else 'neutral',
        'confidence': conf,
        'rationale': rationale if aid != NONE_MARKER else '',
    }


def _upsert_link(event_id: int, symbol: str, result: Dict, version: str):
    conn = get_db()
    c = conn.cursor()
    c.execute('''
        INSERT OR REPLACE INTO thesis_links
          (id, event_id, symbol, assumption_id, stance, confidence, rationale,
           method, thesis_version, created_at)
        VALUES (
          (SELECT id FROM thesis_links WHERE event_id = ?),
          ?, ?, ?, ?, ?, ?, 'llm', ?, ?)
    ''', (
        event_id,
        event_id, symbol, result['assumption_id'], result['stance'],
        result['confidence'], result['rationale'], version,
        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    ))
    conn.commit()
    conn.close()


# ── 批处理:逐 symbol 扫未归档 / 版本过期的 events ───────────

def run_thesis_tracker(batch_size: int = 30) -> Dict:
    """
    对所有有 thesis 的 symbol,找未归档 (或版本过期) 的 events,逐条归档。
    batch_size 是本轮总预算(跨 symbol 共享)。
    返回 stats: {scanned, linked, none, failed, disabled}
    """
    if not is_available():
        return {'scanned': 0, 'linked': 0, 'none': 0, 'failed': 0, 'disabled': True}

    stats = {'scanned': 0, 'linked': 0, 'none': 0, 'failed': 0, 'disabled': False}
    budget = batch_size

    for symbol in THESES.keys():
        if budget <= 0:
            break
        version = thesis_version(symbol)
        aliases = event_symbol_aliases(symbol)
        placeholders = ','.join('?' * len(aliases))

        try:
            conn = get_db()
            c = conn.cursor()
            rows = c.execute(f'''
                SELECT e.id, e.title, e.content, e.llm_summary
                FROM events e
                LEFT JOIN thesis_links tl ON tl.event_id = e.id
                WHERE e.symbol IN ({placeholders})
                  AND (tl.id IS NULL OR tl.thesis_version IS NULL OR tl.thesis_version != ?)
                ORDER BY e.id DESC
                LIMIT ?
            ''', (*aliases, version, min(budget, batch_size))).fetchall()
            conn.close()
        except Exception as e:
            print(f"  [thesis_tracker] 读 {symbol} events 失败: {e}")
            continue

        for row in rows:
            if budget <= 0:
                break
            ev_id, title, content, summary = row
            stats['scanned'] += 1
            # 优先用 LLM 摘要(更短更准),回退正文
            body = summary or content or ''
            result = classify_event(symbol, title or '', body)
            if result is None:
                stats['failed'] += 1
            else:
                try:
                    _upsert_link(ev_id, symbol, result, version)
                    if result['assumption_id'] == NONE_MARKER:
                        stats['none'] += 1
                    else:
                        stats['linked'] += 1
                except Exception as e:
                    print(f"  [thesis_tracker] 写回 event#{ev_id} 失败: {e}")
                    stats['failed'] += 1
            budget -= 1
            time.sleep(0.3)   # 限速,别打爆 provider

    return stats


if __name__ == '__main__':
    if is_available():
        print(f"LLM 已配置: {LLM_PROVIDER}")
        print("thesis symbols:", list(THESES.keys()))
        for s in THESES:
            print(f"  {s} version={thesis_version(s)} aliases={event_symbol_aliases(s)}")
        print(run_thesis_tracker(batch_size=5))
    else:
        print("LLM 未配置 — thesis_tracker 降级跳过")
