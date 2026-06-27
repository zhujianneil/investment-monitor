"""
cls_stream.py — 财联社 / 新浪财经 7×24 实时电报流 (第一层, 无 LLM 依赖)

2026-06-19: 财联社主站 (www.cls.cn) 是 Next.js SPA, 纯 HTTP 抓不到.
退而求其次: 新浪财经 7×24 公开 API (https://feed.mix.sina.com.cn/api/roll/get).
- lid=2515: 科技/AI (英伟达/腾讯/阿里 持仓相关)
- lid=2509/2516/2517/2518: 国际财经 (宏观, 影响 A/H/US)
- lid=2514: 国际时政
- lid=1686: A 股 7x24 (要注册, 暂不可用)

设计:
  1. 每 15 分钟拉一次, 取最近 20-30 条
  2. 去重入 events (source='cls_telegraph', 沿用设计稿命名, 实际是新浪源)
  3. 关键词命中持仓 → 推飞书
  4. 没 LLM 也能用 (与 announcement_stream / yf_news_stream 一致)

2026-06-20 P0 修复 (6 lid 串行超时):
  feed.mix.sina.com.cn 单 lid 3-6s, 6 lid 串行 30s+ 必超时.
  改为 ThreadPool 并行, 单 lid 硬超时 15s.
"""
import time
import requests
from datetime import datetime, timedelta
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import PORTFOLIO
from models import get_db, record_source_failure
from announcement_stream import save_event
from feishu_push import send_keyword_news_alert


# 新浪财经 7×24 公开 API
SINA_API = 'https://feed.mix.sina.com.cn/api/roll/get'
SINA_PAGEID = 153
# 关注的 lid: 国际财经 / 科技 AI (持仓相关)
SINA_LIDS = [2515, 2509, 2516, 2517, 2518, 2514]
FETCH_NUM = 30  # 每次拉多少条
SINA_HARD_TIMEOUT = 15  # 2026-06-20 P0: 单 lid 硬超时 (单 lid 实测 3-6s)


def _fetch_single_lid(l: int, num: int = FETCH_NUM) -> List[Dict]:
    """单 lid 拉取 (无超时, 由外层 ThreadPool.result 控)."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Referer': 'https://finance.sina.com.cn/',
    }
    params = {'pageid': SINA_PAGEID, 'lid': l, 'num': num, 'page': 1}
    r = requests.get(SINA_API, params=params, headers=headers, timeout=10)
    if r.status_code != 200:
        raise Exception(f'HTTP {r.status_code}')
    data = r.json()
    if data.get('result', {}).get('status', {}).get('code') != 0:
        raise Exception(f'业务码 != 0: {data["result"]["status"]}')
    out = []
    for item in data['result'].get('data', []):
        title = (item.get('title') or '').strip()
        url   = (item.get('url')   or '').strip()
        ts    = int(item.get('ctime', 0) or 0)
        if not title or not ts:
            continue
        pub_dt = datetime.fromtimestamp(ts)
        out.append({
            'title':     title,
            'url':       url,
            'pub_date':  pub_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'source_id': f'sina_{l}_{ts}',
            'lid':       l,
        })
    return out


def fetch_sina_telegraph(lid: int = None, num: int = FETCH_NUM) -> List[Dict]:
    """
    拉新浪 7×24 流. lid=None = 拉全部 SINA_LIDS 后合并.
    返回 [{title, url, pub_date, source_id, lid}, ...]

    2026-06-20 P0: 改 ThreadPool 并行 (max_workers=6), 单 lid 硬超时 15s.
    """
    lids = [lid] if lid else SINA_LIDS
    out = []
    # 并行拉取, 单 lid 独立失败处理
    with ThreadPoolExecutor(max_workers=len(lids)) as ex:
        future_map = {ex.submit(_fetch_single_lid, l, num): l for l in lids}
        for fut in as_completed(future_map):
            l = future_map[fut]
            try:
                items = fut.result(timeout=SINA_HARD_TIMEOUT)
                out.extend(items)
            except Exception as e:
                print(f"  [电报流] lid={l} 失败: {type(e).__name__}: {e}")
                record_source_failure(f'sina_telegraph_{l}', e)
    return out


def match_holdings_sina(event: Dict) -> List[tuple]:
    """
    跨市场关键词匹配, 跟 announcement_stream 同步的强约束:
    标到某持仓的硬条件 = (持仓名 OR ticker 出现在 title) AND (关键词命中)
    只有关键词没有持仓名 → 不标 symbol (但保留作为主题信号)
    修复 2026-06-27: 之前只关键词命中就标, 导致 002050「机器人」/688009「中标」/09992「出海」
    等行业泛词把别家新闻错配到持仓
    """
    title = event.get('title', '')
    hits = []
    for sym, cfg in PORTFOLIO.items():
        if cfg.get('monitor_type') == 'EXIT_PENDING':
            continue
        kws = cfg.get('news_keywords', [])
        name = cfg.get('name', '')
        if not kws:
            continue
        # 关键词必须命中
        if not any(kw in title for kw in kws):
            continue
        # 强约束: 持仓名 OR ticker 必须在 title
        # 持仓名支持前 2 字匹配 (例: 持仓名"小米集团"匹配"小米"开头)
        ticker_variants = [sym]
        if sym.endswith('.HK'):
            ticker_variants.append(sym.split('.')[0])
        ticker_variants.append(sym.upper())
        has_strong = (name and (name in title or (len(name) >= 4 and name[:2] in title))) \
                  or any(t in title for t in ticker_variants if t)
        if has_strong:
            hits.append((sym, cfg))
        # 否则: 主题相关, 不直接标 symbol
    return hits


def run_cls_stream(dry_run: bool = False) -> Dict:
    """
    主流程: 拉新浪 7×24 多 lid → 入库 → 关键词命中推送.
    注: source tag 仍用 'cls_telegraph' 保持设计稿命名, 数据实际来自新浪.

    2026-06-19 P0 修复:
      - 单条 7×24 新闻可能涉及多只持仓, 之前 symbol=NULL 没法按 symbol 查
      - 现在: 关键词/名称命中后, 把命中的 symbol 写回 events.symbol
      - 同一条新闻可能多 symbol (逗号分隔), LLM 增强后会被标准化
    """
    started = datetime.now()
    print(f"\n{'='*55}")
    print(f"  7×24 电报流 — {started.strftime('%Y-%m-%d %H:%M')}  lids={SINA_LIDS}")
    print(f"{'='*55}")

    raw = fetch_sina_telegraph()
    print(f"  [电报流] 抓取 {len(raw)} 条 (去重前)")

    # 按 source_id 去重 (同一新闻可能在多 lid 出现)
    seen = set()
    unique = []
    for ev in raw:
        if ev['source_id'] in seen:
            continue
        seen.add(ev['source_id'])
        unique.append(ev)
    print(f"  [电报流] 去重后 {len(unique)} 条")

    # 2026-06-22 P1: 跨 lid 二次去重 (同 title + 同分钟)
    # 不同 lid 可能给不同 source_id (e.g. lid=2515 时 source_id='sina_2515_ts',
    # lid=2509 时是 'sina_2509_ts') → 上面 seen 拦不住
    # 后果: 同一新闻在数据库里出现 2-4 次, 每次都触发推送
    # 修复: 同 (title 前 30 字, pub_date 到分钟) 视为同一新闻, 留 source_id 最小的
    from collections import defaultdict
    title_minute_map = defaultdict(list)
    for ev in unique:
        title_key = (ev.get('title', '') or '')[:30].strip()
        pub_minute = (ev.get('pub_date', '') or '')[:16]  # YYYY-MM-DD HH:MM
        if not title_key:
            continue
        title_minute_map[(title_key, pub_minute)].append(ev)
    deduped = []
    for ev in unique:
        title_key = (ev.get('title', '') or '')[:30].strip()
        pub_minute = (ev.get('pub_date', '') or '')[:16]
        bucket = title_minute_map.get((title_key, pub_minute), [ev])
        if bucket and bucket[0] is ev:
            deduped.append(ev)
    if len(deduped) < len(unique):
        print(f"  [电报流] 跨 lid 标题去重: {len(unique)} → {len(deduped)} 条 (节省 {len(unique)-len(deduped)} 次潜在重复推送)")
    unique = deduped

    new_in_db = 0
    pushed = 0
    # 2026-06-19 P0: 导入 relevance 分类 (避免循环导入延迟到第一次调用)
    from feishu_push import _classify_relevance
    PUSH_RELEVANCE_MIN = 'primary'  # 默认只推主体相关, 主题/弱相关不推 (避免噪声)
    for ev in unique:
        # 先匹配, 拿命中的 symbol 列表
        hits = match_holdings_sina(ev)
        # 2026-06-22 P0 修复: symbol 字段只保留 relevance 达标的持仓
        # 之前: sym_field = 全部命中持仓的并集 → 错配
        #   例: NVDA 主题新闻 + 002050 三花智控 keywords 含"机器人"
        #     → 002050 也被标 symbol, 但 best_relevance=primary 是 NVDA 的
        #     → events.symbol 写成 "002050,NVDA" → 002050 名下出现英伟达新闻
        # 现在: 逐持仓判 relevance, 只把达标的写 events.symbol
        rel_order = {'primary': 3, 'thematic': 2, 'weak': 1}
        min_rank = rel_order.get(PUSH_RELEVANCE_MIN, 3)
        primary_syms = set()
        best_relevance = 'weak'
        per_sym_relevance = {}  # 2026-06-22 记录每个 sym 自己的 relevance, 推送时用
        for sym, cfg in hits:
            r = _classify_relevance(cfg.get('name', sym), ev['title'], cfg.get('news_keywords', []))
            per_sym_relevance[sym] = r
            if rel_order.get(r, 0) >= min_rank:
                primary_syms.add(sym)
            if rel_order.get(r, 0) > rel_order.get(best_relevance, 0):
                best_relevance = r

        # symbol 字段: 只写 relevance 达标的标的 (comma-separated)
        sym_field = ','.join(sorted(primary_syms)) if primary_syms else None

        is_new = save_event(
            source='cls_telegraph',   # 沿用设计稿 tag
            source_id=ev['source_id'] or '',
            symbol=sym_field,         # 2026-06-22 P0: 只写 relevance 达标的 symbol
            title=ev['title'],
            url=ev['url'],
            pub_date=ev['pub_date'] or '',
            pub_date_precision='datetime',  # 新浪是 Unix 时间戳, 精确到秒
            relevance=best_relevance if hits else None,  # 最高 relevance (供显示)
            cross_lid_dedup=True,    # 2026-06-22 P1: 跨 lid 标题去重 (应用层, 兜底 save_event UNIQUE 拦不住的情况)
        )
        if is_new:
            new_in_db += 1

        # 推送去重: 只对"新入库"的事件推送, 已存在不重复推 (2026-06-19 P0)
        # 7×24 流是全市场流, 一条新闻会反复命中同一持仓的关键词, 不去重会刷屏
        if not is_new:
            continue  # 已入库, 跳过推送 (避免噪声)

        # 关键词命中推送 (2026-06-22 P0: 推送 symbol 集合 = primary_syms, 用 per_sym_relevance)
        for sym, cfg in hits:
            if sym not in primary_syms:
                continue  # 2026-06-22 P0: relevance 不达标不推 (即使在 hits 列表里)
            name = cfg.get('name', sym)
            keywords = cfg.get('news_keywords', [])
            relevance = per_sym_relevance.get(sym, 'weak')
            if dry_run:
                print(f"    [DRY-RUN] 推 → {name}({sym}) [{relevance}]: {ev['title'][:50]}")
            else:
                send_keyword_news_alert(name, sym, ev['title'], ev['url'], keywords, relevance=relevance)
                # P1: 推送成功回写 (审计闭环)
                from announcement_stream import mark_pushed
                mark_pushed('cls_telegraph', ev.get('source_id', '') or '', ev.get('pub_date', '') or '')
            pushed += 1

    stats = {
        'job_name': 'cls_stream',
        'status': 'ok' if unique else 'failed',
        'symbols_processed': new_in_db,
        'symbols_failed': 0 if unique else 1,
        'last_error': None if unique else 'sina 7×24 全部 lid 抓取空',
        'started_at': started.strftime('%Y-%m-%d %H:%M:%S'),
        'fetched': len(raw),
        'new_in_db': new_in_db,
        'pushed': pushed,
    }
    print(f"  [电报流] 新入库 {new_in_db}, 推送 {pushed}")
    return stats


if __name__ == '__main__':
    import sys
    dry = '--dry-run' in sys.argv
    stats = run_cls_stream(dry_run=dry)
    print(stats)
