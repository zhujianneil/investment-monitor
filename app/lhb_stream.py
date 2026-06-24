"""
lhb_stream.py — 广发证券龙虎榜异动监控 (2026-06-22 新增, 第一层, 无 LLM 依赖)

数据源: 广发证券官方 MCP 接口 (mcp-api.gf.com.cn/gf-skills)
        service_name=lhb, tool_name=lhb_aborttrade_market_date_get
        文档: ~/.hermes/skills/devops/gf-lhb-list/SKILL.md

设计目标:
  1. 每个交易日收盘后 (16:00) 拉一次 sh + sz 当日龙虎榜
  2. 跟 PORTFOLIO 18 只持仓做精确 symbol 匹配
  3. 命中即推飞书红色告警 (官方异动阈值, 比手算 ±5% 更准)
  4. 全市场龙虎榜清单入 events 表 (source='gf_lhb') 供回看 + 趋势分析

关键时间:
  - A 股收盘 15:00
  - 上交所/深交所 15:30 公布当日龙虎榜
  - 广发接口 15:45 起数据稳定
  - 本 skill 跑 16:00 是稳定 + 不与收盘监控冲突

P0 错误防御 (2026-06-22):
  - 8-variant 协议探针已验证 (见 SKILL.md Pitfall 8)
  - retcode=0 = 业务成功 (data.data 可能为空 — 当日无异动)
  - retcode=20002 = transport 错 (检查 Bearer token 是否被屏蔽)
  - retcode=10002 = 业务参数错 (基本不会发生)
"""
import os
import json
import urllib.request
import urllib.error
from datetime import datetime
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import PORTFOLIO
from models import get_db, record_source_failure
from announcement_stream import save_event, mark_pushed
from feishu_push import send_lhb_alert
from load_secrets import decode_secrets

# 2026-06-22: 每次模块加载时解码 secrets (覆盖手动 docker exec 场景)
# scheduler 启动时 main.py 也调过, 这里重复调是 idempotent
decode_secrets()


# ── 广发接口配置 ─────────────────────────────────────────────
GF_API_URL = 'https://mcp-api.gf.com.cn/gf-skills/skills/mcp/call'
GF_SERVICE_NAME = 'lhb'
GF_TOOL_NAME = 'lhb_aborttrade_market_date_get'
SOURCE_TAG = 'gf_lhb'  # events.source 标识
GF_HARD_TIMEOUT = 15   # 单 market 拉取硬超时 (秒, 实际 ~200ms)


# ── 抓取 ─────────────────────────────────────────────────────

def _fetch_single_market(market: str, date: int) -> List[Dict]:
    """
    拉单个市场的龙虎榜清单. 由外层 ThreadPool.result 控超时.

    返回 [{trdCode, secuSht, clsPrc, dayChgRat, tnvVol, tnvVal,
           items[].rsnSht, items[].rsnCode, items[].beginDate, items[].endDate,
           date, market, updateTime}, ...]
    """
    apikey = os.environ.get('GF_SKILLS_APIKEY')
    if not apikey:
        raise RuntimeError(
            'GF_SKILLS_APIKEY env var not set. '
            '~/.gfkey_b64.sh + ~/.bashrc 自动 source 应已配置; '
            '参考 ~/.hermes/skills/devops/gf-lhb-list/SKILL.md Pitfall 11'
        )

    payload = {
        'service_name': GF_SERVICE_NAME,
        'tool_name':    GF_TOOL_NAME,
        'args':         {'date': date, 'market': market},
    }
    body = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(
        GF_API_URL, data=body,
        headers={
            'Content-Type':  'application/json',
            'Authorization': f'Bearer {apikey}',
        },
    )
    with urllib.request.urlopen(req, timeout=GF_HARD_TIMEOUT) as r:
        resp = json.loads(r.read().decode('utf-8'))

    # 业务校验
    if resp.get('retcode') != 0:
        raise RuntimeError(f"广发接口 retcode={resp.get('retcode')}: {resp.get('msg','')[:200]}")

    data = resp.get('data', {}) or {}
    items = data.get('data', [])
    if not items:
        return []

    # 标准化字段 (广发原始字段保留, 加标准化 date/market 便于后续分析)
    out = []
    for it in items:
        out.append({
            'trdCode':   it.get('trdCode', ''),
            'secuSht':   it.get('secuSht', ''),
            'clsPrc':    it.get('clsPrc'),
            'dayChgRat': it.get('dayChgRat'),
            'tnvVol':    it.get('tnvVol'),
            'tnvVal':    it.get('tnvVal'),
            'items':     it.get('items', []),   # 上榜原因列表 (可多个)
            'date':      it.get('date', date),
            'market':    it.get('market', market.upper()),
            'updateTime': it.get('updateTime'),
        })
    return out


def fetch_lhb_for_date(date: Optional[int] = None) -> List[Dict]:
    """
    拉指定日期 sh + sz 全市场龙虎榜, 并行 fetch (实测各 ~200ms).

    date: YYYYMMDD int, 默认今天
    返回 sh + sz 合并后的全市场清单 (不区分市场 — 后续 match 时不强求).

    2026-06-22 加固: ThreadPool 并行, 单 market 硬超时 15s.
    """
    if not date:
        date = int(datetime.now().strftime('%Y%m%d'))

    all_items = []
    with ThreadPoolExecutor(max_workers=2) as ex:
        fut_map = {ex.submit(_fetch_single_market, m, date): m for m in ('sh', 'sz')}
        for fut in as_completed(fut_map):
            m = fut_map[fut]
            try:
                items = fut.result(timeout=GF_HARD_TIMEOUT)
                all_items.extend(items)
            except Exception as e:
                print(f"  [龙虎榜] market={m} 失败: {type(e).__name__}: {e}")
                record_source_failure(f'gf_lhb_{m}', e)
    return all_items


# ── 持仓匹配 ─────────────────────────────────────────────────

def match_holdings_lhb(item: Dict) -> List[tuple]:
    """
    持仓精确匹配 (不靠关键词 — 龙虎榜本身就是硬命中).

    规则:
      - PORTFOLIO 中 akshare_symbol (A 股 6 位代码) == item.trdCode
      - 跳过 EXIT_PENDING 持仓 (海尔智家/福耀玻璃 已剔除监控)

    返回 [(symbol, cfg), ...] 命中的持仓
    """
    code = str(item.get('trdCode', '')).strip()
    if not code:
        return []

    hits = []
    for sym, cfg in PORTFOLIO.items():
        if cfg.get('monitor_type') == 'EXIT_PENDING':
            continue
        # 龙虎榜是 A 股数据, 只匹配 CN 市场持仓
        if cfg.get('market') != 'CN':
            continue
        akshare_sym = str(cfg.get('akshare_symbol', sym)).strip()
        if akshare_sym == code:
            hits.append((sym, cfg))
    return hits


# ── 推送去重 (P0: 防止日终 cron 重启时重复推) ───────────────

def _is_already_pushed_today(symbol: str, trd_code: str, date_str: str) -> bool:
    """
    检查 (symbol, trdCode, date) 当日是否已推过.
    用 events 表 source_id 格式: f"{date}_{trdCode}_{symbol}"
    """
    try:
        source_id = f"{date_str}_{trd_code}_{symbol}"
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT pushed FROM events
            WHERE source=? AND source_id=? AND pub_date=?
            LIMIT 1
        ''', (SOURCE_TAG, source_id, date_str))
        row = cursor.fetchone()
        conn.close()
        return bool(row and row[0])
    except Exception as e:
        print(f"  [龙虎榜] _is_already_pushed_today 失败: {e}")
        return False


# ── 主流程 ───────────────────────────────────────────────────

def run_lhb_stream(date: Optional[int] = None, dry_run: bool = False) -> Dict:
    """
    一轮龙虎榜监控: 拉 sh+sz → 持仓匹配 → 入 events → 推飞书.

    date: YYYYMMDD int, 默认今天 (盘后 16:00 跑就是当天数据).
          手工跑可指定历史日.

    返回 stats dict (供 scheduler 写 monitor_runs).
    """
    started = datetime.now()
    if not date:
        date = int(started.strftime('%Y%m%d'))
    date_str = str(date)  # YYYYMMDD 字符串, 用于 events 表

    print(f"\n{'='*55}")
    print(f"  龙虎榜异动 — {date_str}  dry_run={dry_run}")
    print(f"{'='*55}")

    # 1) 抓 sh + sz
    items = fetch_lhb_for_date(date=date)
    print(f"  [龙虎榜] 抓取 {len(items)} 只 (sh+sz 合并)")

    new_in_db = 0
    pushed = 0
    hits_total = 0

    for it in items:
        trd_code = it['trdCode']
        hits = match_holdings_lhb(it)
        if not hits:
            continue
        hits_total += len(hits)

        # 整理 reason 字段
        reasons = it.get('items', [])
        reason_text = ' / '.join(r.get('rsnSht', '?') for r in reasons) or '(未明)'

        # 写 events 表 (一次一条龙虎榜 + symbol 关联)
        for sym, cfg in hits:
            name = cfg.get('name', sym)
            is_new = save_event(
                source=SOURCE_TAG,
                source_id=f"{date_str}_{trd_code}_{sym}",
                symbol=sym,
                title=f"{name}({trd_code}) 龙虎榜: {reason_text} | 收盘 {it['clsPrc']} 涨跌 {it['dayChgRat']}%",
                content=json.dumps({
                    'trdCode': trd_code,
                    'secuSht': it['secuSht'],
                    'clsPrc': it['clsPrc'],
                    'dayChgRat': it['dayChgRat'],
                    'tnvVol': it['tnvVol'],
                    'tnvVal': it['tnvVal'],
                    'reasons': [{'rsnCode': r.get('rsnCode'), 'rsnSht': r.get('rsnSht'),
                                 'beginDate': r.get('beginDate'), 'endDate': r.get('endDate')}
                                for r in reasons],
                }, ensure_ascii=False),
                url='',  # 广发接口不直接给个股龙虎榜详情 URL
                pub_date=date_str,
                pub_date_precision='day',  # 龙虎榜日级
                relevance='primary',  # 持仓精确匹配, 都是 primary
            )
            if is_new:
                new_in_db += 1

            # 推送去重: 同 (symbol, trdCode, date) 当日已推过则跳过
            if _is_already_pushed_today(sym, trd_code, date_str):
                print(f"    [去重] 跳过: {name}({sym}) {date_str} 当日已推过")
                continue

            if dry_run:
                print(f"    [DRY-RUN] 推 → {name}({sym}) {reason_text} 涨跌 {it['dayChgRat']}%")
            else:
                send_lhb_alert(name, sym, it, reason_text)
                mark_pushed(SOURCE_TAG, f"{date_str}_{trd_code}_{sym}", date_str)
            pushed += 1

    stats = {
        'job_name': 'lhb_stream',
        'status': 'ok' if items is not None else 'failed',
        'symbols_processed': hits_total,
        'symbols_failed': 0 if items else 1,
        'last_error': None if items else 'sh+sz 拉取全失败',
        'started_at': started.strftime('%Y-%m-%d %H:%M:%S'),
        'date': date_str,
        'fetched': len(items),
        'new_in_db': new_in_db,
        'pushed': pushed,
        'holdings_matched': hits_total,
    }
    print(f"  [龙虎榜] 持仓命中 {hits_total} 条, 新入库 {new_in_db}, 推送 {pushed}")
    return stats


# ── 手工命令行 ──────────────────────────────────────────────
# 用法: docker exec investment-monitor python3 lhb_stream.py [--date YYYYMMDD] [--dry-run]

if __name__ == '__main__':
    import sys
    dry = '--dry-run' in sys.argv
    target_date = None
    for i, a in enumerate(sys.argv):
        if a == '--date' and i + 1 < len(sys.argv):
            target_date = int(sys.argv[i + 1])
    stats = run_lhb_stream(date=target_date, dry_run=dry)
    print(stats)
