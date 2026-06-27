"""
sina_global_stream.py — 港美股新闻备源 (新浪财经国际财经 2509 + 时政 2514, 2026-06-19 P1)

为什么需要备用源:
  - 港美股新闻完全依赖 yfinance, yf_news 流 0 条 (字段结构已变, 见 P1 修复)
  - 真正"国际财经"的新浪 lid 编码不是 1686 (那个不存在), 是 2509
  - 2509 = 国际财经 (地缘政治 / 油价 / 利率 / 欧美市场)
  - 2514 = 国际时政 (地缘事件)
  - 通过 PORTFOLIO ticker + 名称关键词命中 → 入 events

调度: 每小时一次, 错开 yf_news_stream (yf 跑在 :45)

2026-06-20 P0 修复: 改 ThreadPool 并行拉 2 lid, 避免单 lid 超时拖垮整轮.
"""
import sqlite3
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import requests

from config import PORTFOLIO
from models import get_db, record_source_failure
from announcement_stream import save_event
from feishu_push import send_keyword_news_alert


# 国际财经 + 国际时政 (新浪 7×24 实际可用的港美股相关 lid)
SINA_GLOBAL_LIDS = [
    ('2509', '国际财经'),
    ('2514', '国际时政'),
]

CN_TICKER_MAP = {}  # 不需要翻译
SINA_API = 'https://feed.mix.sina.com.cn/api/roll/get?pageid=153&lid={lid}&num=50'
SINA_HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0)'}


# ── 抓取 ─────────────────────────────────────────────────────

def fetch_sina_global_lid(lid: str, desc: str, num: int = 50,
                            hard_timeout: int = 20) -> List[Dict]:
    """
    拉单个 lid 的最新 50 条, 转 events 格式.
    返回 [{title, url, pub_date, source_id, lid}, ...]
    """
    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutTimeout

    def _do():
        r = requests.get(SINA_API.format(lid=lid), headers=SINA_HEADERS,
                          params={}, timeout=10)
        r.raise_for_status()
        j = r.json()
        data = j.get('result', {}).get('data', [])
        results = []
        for d in data:
            title = d.get('title', '').strip()
            url = d.get('url', '').strip()
            ctime_ts = d.get('ctime', 0)
            if not title or not url:
                continue
            pub_dt = datetime.fromtimestamp(int(ctime_ts))
            results.append({
                'title': title,
                'url': url,
                'pub_date': pub_dt.strftime('%Y-%m-%d %H:%M:%S'),
                'pub_date_precision': 'datetime',
                'source_id': f'sina_global_{lid}_{ctime_ts}_{hash(title) & 0xFFFF:04x}',
                'lid': lid,
                'lid_desc': desc,
            })
        return results

    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_do)
        try:
            return fut.result(timeout=hard_timeout)
        except FutTimeout:
            record_source_failure(
                source_name=f'sina_global_{lid}',
                error=f'硬超时 {hard_timeout}s'
            )
            print(f"    [sina_global] lid={lid} 硬超时")
            return []


# ── 持仓匹配 ─────────────────────────────────────────────────

def match_holdings_global(item: Dict) -> List[tuple]:
    """
    国际财经流没有明确 symbol, 用 ticker + 公司名 + 关键词模糊匹配.
    修复 2026-06-27: 加同 CN 一致的强约束 —
    ticker 命中或持仓名命中: 算"主体相关", 可标 symbol
    关键词命中 + 持仓名匹配: 主题相关, 可标 (例: 巴菲特 → 伯克希尔)
    """
    title = item.get('title', '')
    summary = ''  # 新浪 7×24 摘要字段在另一处
    text = title + ' ' + summary
    hits = []

    for sym, cfg in PORTFOLIO.items():
        if cfg.get('market') not in ('HK', 'US'):
            continue
        # ticker 直接匹配 (强约束, 必须 word boundary, 避免 "V" 匹配 "NVDA")
        if _ticker_in_text(sym, text):
            hits.append((sym, cfg))
            continue
        # 公司名 (强约束)
        name = cfg.get('name', '')
        if name and name in text:
            hits.append((sym, cfg))
            continue
        # 关键词 (需配合持仓名/ticker, 否则会被行业词误命中)
        kws = cfg.get('news_keywords', [])
        if kws and any(kw in text for kw in kws):
            # 强约束: 持仓名 OR ticker 也必须在
            ticker_variants = [sym]
            if sym.endswith('.HK'):
                ticker_variants.append(sym.split('.')[0])
            ticker_variants.append(sym.upper())
            has_strong = (name and (name in text or (len(name) >= 4 and name[:2] in text))) \
                      or any(_ticker_in_text(t, text) for t in ticker_variants if t)
            if has_strong:
                hits.append((sym, cfg))
    return hits


def _ticker_in_text(ticker: str, text: str) -> bool:
    """
    ticker 必须以 word boundary 出现在 text 中.
    - 'NVDA' 必须作为独立词出现 (不算 "NVDAxx")
    - 'V' 必须作为独立大写词出现 (不算 "NVDA" 中的 'V')
    中文环境: \b 在 re.UNICODE 下不工作 (中文是 \\w), 改用非 \\w 字符作边界
    """
    import re
    t = re.escape(ticker.upper())
    # (?<![A-Za-z0-9]) 前面不是字母数字
    # (?![A-Za-z0-9]) 后面不是字母数字
    return bool(re.search(rf'(?<![A-Za-z0-9]){t}(?![A-Za-z0-9])', text))


# ── 主流程 ───────────────────────────────────────────────────

def run_sina_global_stream(dry_run: bool = False, push: bool = True) -> Dict:
    """
    一轮新浪全球财经流: 拉 2509 + 2514 → 入 events → 持仓命中推送
    2026-06-20 P0: 两 lid 并行拉取, 避免串行超时.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    started = datetime.now()
    print(f"\n{'='*55}")
    print(f"  新浪全球财经 — {started.strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*55}")

    # 2026-06-20 P0: 两 lid 并行, 避免单 lid 慢拖垮整轮
    all_items = []
    with ThreadPoolExecutor(max_workers=len(SINA_GLOBAL_LIDS)) as ex:
        future_map = {ex.submit(fetch_sina_global_lid, lid, desc): (lid, desc)
                      for lid, desc in SINA_GLOBAL_LIDS}
        for fut in as_completed(future_map):
            lid, desc = future_map[fut]
            try:
                items = fut.result(timeout=25)
                print(f"    [sina_global] lid={lid} ({desc}): {len(items)} 条")
                all_items.extend(items)
            except Exception as e:
                print(f"    [sina_global] lid={lid} ({desc}) 失败: {type(e).__name__}: {e}")
    print(f"  [sina_global] 抓取 {len(all_items)} 条")

    new_in_db = 0
    pushed = 0
    from feishu_push import _classify_relevance
    for ev in all_items:
        # 2026-06-20 P0: 提前匹配, 把命中的 symbol 写回 events.symbol
        # 跟 cls_stream 一致 — 之前 sina_global 全部 symbol=空, 没法按 symbol 查
        hits_pre = match_holdings_global(ev)
        sym_field = ','.join(sorted({h[0] for h in hits_pre})) if hits_pre else ''
        # relevance: 取最高
        rel_rank = {'primary': 3, 'thematic': 2, 'weak': 1}
        best_relevance = 'weak'
        for sym, cfg in hits_pre:
            r = _classify_relevance(cfg.get('name', sym), ev['title'], cfg.get('news_keywords', []))
            if rel_rank.get(r, 0) > rel_rank.get(best_relevance, 0):
                best_relevance = r

        is_new = save_event(
            source='sina_global',
            source_id=ev.get('source_id', ''),
            symbol=sym_field,
            title=ev.get('title', ''),
            url=ev.get('url', ''),
            pub_date=ev.get('pub_date', ''),
            pub_date_precision='datetime',
            relevance=best_relevance if hits_pre else None,
        )
        if is_new:
            new_in_db += 1
        if not is_new or not push:
            continue

        hits = match_holdings_global(ev)
        # 2026-06-20 P0: relevance 过滤, 跟 cls_stream 一致 — 只推主体相关,
        # 避免"印度空军坠机"含"印度"二字被推到小米集团这种 thematic 误报.
        # 注: yfinance 错配问题已通过二次校验修, sina_global 标题含 ticker/公司名
        # 即 primary, 仅主题词命中 (如"印度") 是 thematic — 不推.
        PUSH_RELEVANCE_MIN = 'primary'
        for sym, cfg in hits:
            name = cfg.get('name', sym)
            keywords = cfg.get('news_keywords', [])
            relevance = _classify_relevance(name, ev['title'], keywords)
            rel_order = {'primary': 3, 'thematic': 2, 'weak': 1}
            if rel_order.get(relevance, 0) < rel_order.get(PUSH_RELEVANCE_MIN, 3):
                continue  # thematic/weak 不推
            if dry_run:
                print(f"    [DRY-RUN] 推 → {name}({sym}) [{relevance}]: {ev.get('title','')[:50]}")
            else:
                send_keyword_news_alert(name, sym, ev.get('title', ''),
                                         ev.get('url', ''), keywords, relevance=relevance)
                # P1: 推送成功回写 (审计闭环)
                from announcement_stream import mark_pushed
                mark_pushed('sina_global', ev.get('source_id', ''), ev.get('pub_date', ''))
            pushed += 1

    stats = {
        'job_name': 'sina_global_stream',
        'status': 'ok' if all_items else 'failed',
        'symbols_processed': new_in_db,
        'symbols_failed': 0 if all_items else 1,
        'last_error': None if all_items else '新浪国际财经 lid 全部抓取空',
        'started_at': started.strftime('%Y-%m-%d %H:%M:%S'),
        'fetched': len(all_items),
        'new_in_db': new_in_db,
        'pushed': pushed,
    }
    print(f"  [sina_global] 新入库 {new_in_db}, 推送 {pushed}")
    return stats


if __name__ == '__main__':
    s = run_sina_global_stream(dry_run=True)
    print('\n=== stats ===')
    for k, v in s.items():
        print(f'  {k}: {v}')
