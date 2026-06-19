"""
llm_enhancer.py — LLM 增强 (第二层, 可降级)

定位: 第一层 (announcement_stream / yf_news_stream) 入库后,
本模块对未增强的 events 调 LLM, 写回 llm_summary / sentiment / severity.

降级策略:
  - 未配置 LLM_API_KEY → 不调, 标记 'llm_disabled'
  - 调失败 → 写日志, 不影响第一层推送
  - 同一 title 只调一次 (title_hash 缓存)

支持的 provider (2026-06-19):
  - glm (智谱)         - GLM-4-Flash    便宜中文
  - qwen (通义千问)    - qwen-turbo     备选
  - openai (兼容协议)  - 任何 OpenAI 协议端点

.env 配置:
  LLM_PROVIDER=glm
  LLM_API_KEY=xxx
  LLM_MODEL=glm-4-flash          (可选, 默认按 provider 选)
  LLM_BASE_URL=                  (可选, 自定义端点)
"""
import os
import json
import hashlib
import time
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional

import requests

from config import DB_PATH
from models import get_db


# ── 配置 ─────────────────────────────────────────────────────

LLM_PROVIDER = os.getenv('LLM_PROVIDER', '').lower()  # glm | qwen | openai | ''
LLM_API_KEY   = os.getenv('LLM_API_KEY', '')
LLM_BASE_URL  = os.getenv('LLM_BASE_URL', '')
LLM_MODEL     = os.getenv('LLM_MODEL', '')

_DEFAULT_MODELS = {
    'glm':    'glm-4-flash',
    'qwen':   'qwen-turbo',
    'openai': 'gpt-4o-mini',
}
_DEFAULT_ENDPOINTS = {
    'glm':  'https://open.bigmodel.cn/api/paas/v4/chat/completions',
    'qwen': 'https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions',
    'openai': 'https://api.openai.com/v1/chat/completions',
}

PROMPT_TEMPLATE = """你是一个 A 股 / 港美股 投资助理。请对下面这条公告/新闻做结构化解读, 输出严格 JSON (不要 markdown code block, 不要解释):

标题: {title}
正文: {content}
关联标的: {symbol}

字段:
- summary: 一句话中文解读, 不超过 50 字
- sentiment: float, -1.0 (极负面) 到 1.0 (极正面)
- themes: list[str], 涉及的主题标签, 不超过 3 个 (如 "5G建设", "央企重组", "美联储加息")
- severity: "high" | "medium" | "low", 按对股价的潜在影响程度判断

只输出 JSON."""


# ── 健康检查 ─────────────────────────────────────────────────

def is_available() -> bool:
    return bool(LLM_PROVIDER and LLM_API_KEY)


# ── 缓存 (events 表的 title_hash 复用 llm_cache 表) ─────────

def _title_hash(title: str) -> str:
    return hashlib.sha1(title.encode('utf-8')).hexdigest()[:16]


def _get_cached(title: str) -> Optional[Dict]:
    h = _title_hash(title)
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute('SELECT summary, sentiment, themes, severity FROM llm_cache WHERE title_hash=?', (h,))
        row = c.fetchone()
        conn.close()
        if row:
            return {
                'summary':   row[0],
                'sentiment': row[1],
                'themes':    json.loads(row[2]) if row[2] else [],
                'severity':  row[3],
                'cached':    True,
            }
    except Exception:
        pass
    return None


def _put_cache(title: str, result: Dict):
    h = _title_hash(title)
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute('''INSERT OR REPLACE INTO llm_cache
                     (title_hash, title, summary, sentiment, themes, severity, cached_at)
                     VALUES (?, ?, ?, ?, ?, ?, ?)''',
                  (h, title[:500], result.get('summary'),
                   result.get('sentiment'),
                   json.dumps(result.get('themes', []), ensure_ascii=False),
                   result.get('severity'),
                   datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"  [llm_enhancer] 写缓存失败: {e}")


# ── LLM 调用 ────────────────────────────────────────────────

def _call_llm(title: str, content: str, symbol: str = '') -> Optional[Dict]:
    """调一次 LLM, 失败返回 None. 失败不抛."""
    if not is_available():
        return None

    model = LLM_MODEL or _DEFAULT_MODELS.get(LLM_PROVIDER, 'glm-4-flash')
    url   = LLM_BASE_URL or _DEFAULT_ENDPOINTS.get(LLM_PROVIDER, _DEFAULT_ENDPOINTS['glm'])

    prompt = PROMPT_TEMPLATE.format(
        title=title[:200], content=(content or '')[:800], symbol=symbol or 'N/A')

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {LLM_API_KEY}',
    }
    body = {
        'model': model,
        'messages': [
            {'role': 'system', 'content': '你只输出 JSON.'},
            {'role': 'user',   'content': prompt},
        ],
        'temperature': 0.1,
    }

    try:
        r = requests.post(url, headers=headers, json=body, timeout=20)
        if r.status_code != 200:
            print(f"  [llm_enhancer] {LLM_PROVIDER} HTTP {r.status_code}: {r.text[:200]}")
            return None
        data = r.json()
        text = data['choices'][0]['message']['content'].strip()
        # 偶尔 LLM 仍包 ```json ... ```, 兜一下
        if text.startswith('```'):
            text = text.strip('`').strip()
            if text.lower().startswith('json'):
                text = text[4:].strip()
        result = json.loads(text)
        # 字段兜底
        return {
            'summary':   str(result.get('summary', ''))[:200],
            'sentiment': float(result.get('sentiment', 0)),
            'themes':    list(result.get('themes', []))[:3],
            'severity':  str(result.get('severity', 'low')).lower(),
            'cached':    False,
        }
    except Exception as e:
        print(f"  [llm_enhancer] {LLM_PROVIDER} 失败: {type(e).__name__}: {e}")
        return None


def enhance(title: str, content: str = '', symbol: str = '') -> Dict:
    """
    增强入口. 返回 {summary, sentiment, themes, severity, cached, status}.
    status ∈ {ok, cached, disabled, failed}
    """
    if not is_available():
        return {'status': 'disabled', 'severity': 'low', 'summary': '', 'sentiment': 0, 'themes': []}

    # 1. 缓存命中
    cached = _get_cached(title)
    if cached:
        cached['status'] = 'cached'
        return cached

    # 2. 调 LLM
    result = _call_llm(title, content, symbol)
    if result is None:
        return {'status': 'failed', 'severity': 'low', 'summary': '', 'sentiment': 0, 'themes': []}

    # 3. 写缓存
    _put_cache(title, result)
    result['status'] = 'ok'
    return result


# ── 批处理 (从 events 表找未增强的, 逐条增强) ───────────────

def enhance_pending_events(batch_size: int = 30) -> Dict:
    """
    从 events 找 llm_cached_at IS NULL 的, 逐条增强并写回.
    返回 stats: {scanned, enhanced, cached, failed, disabled}
    """
    if not is_available():
        return {'scanned': 0, 'enhanced': 0, 'cached': 0, 'failed': 0, 'disabled': True}

    try:
        conn = get_db()
        c = conn.cursor()
        c.execute('''SELECT id, title, content, symbol FROM events
                     WHERE llm_cached_at IS NULL
                     ORDER BY id DESC LIMIT ?''', (batch_size,))
        rows = c.fetchall()
        conn.close()
    except Exception as e:
        print(f"  [llm_enhancer] 读 events 失败: {e}")
        return {'scanned': 0, 'enhanced': 0, 'cached': 0, 'failed': 0, 'error': str(e)}

    stats = {'scanned': len(rows), 'enhanced': 0, 'cached': 0, 'failed': 0}
    for row in rows:
        ev_id, title, content, symbol = row
        r = enhance(title or '', content or '', symbol or '')
        if r.get('status') in ('ok', 'cached'):
            try:
                conn = get_db()
                c = conn.cursor()
                c.execute('''UPDATE events SET
                    llm_summary=?, llm_sentiment=?, llm_themes=?, llm_severity=?, llm_cached_at=?
                    WHERE id=?''', (
                    r.get('summary'), r.get('sentiment'),
                    json.dumps(r.get('themes', []), ensure_ascii=False),
                    r.get('severity', 'low'),
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    ev_id,
                ))
                conn.commit(); conn.close()
                if r['status'] == 'ok':
                    stats['enhanced'] += 1
                else:
                    stats['cached'] += 1
            except Exception as e:
                print(f"  [llm_enhancer] 写回 event#{ev_id} 失败: {e}")
                stats['failed'] += 1
        else:
            stats['failed'] += 1

        # 限速, 别打爆 provider
        time.sleep(0.3)

    return stats


if __name__ == '__main__':
    if is_available():
        print(f"LLM 已配置: {LLM_PROVIDER} / {LLM_MODEL or _DEFAULT_MODELS.get(LLM_PROVIDER)}")
    else:
        print("LLM 未配置 (LLM_PROVIDER / LLM_API_KEY 缺一) — 降级为不增强")
    print(enhance_pending_events(batch_size=5))
