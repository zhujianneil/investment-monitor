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
"""
import time
import requests
from datetime import datetime, timedelta
from typing import List, Dict

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


def fetch_sina_telegraph(lid: int = None, num: int = FETCH_NUM) -> List[Dict]:
    """
    拉新浪 7×24 流. lid=None = 拉全部 SINA_LIDS 后合并.
    返回 [{title, url, pub_date, source_id, lid}, ...]
    """
    lids = [lid] if lid else SINA_LIDS
    out = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Referer': 'https://finance.sina.com.cn/',
    }
    for l in lids:
        params = {'pageid': SINA_PAGEID, 'lid': l, 'num': num, 'page': 1}
        try:
            r = requests.get(SINA_API, params=params, headers=headers, timeout=10)
            if r.status_code != 200:
                print(f"  [电报流] lid={l} HTTP {r.status_code}")
                record_source_failure(f'sina_telegraph_{l}', Exception(f'HTTP {r.status_code}'))
                continue
            data = r.json()
            if data.get('result', {}).get('status', {}).get('code') != 0:
                print(f"  [电报流] lid={l} 业务码 != 0: {data['result']['status']}")
                continue
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
        except Exception as e:
            print(f"  [电报流] lid={l} 失败: {type(e).__name__}: {e}")
            record_source_failure(f'sina_telegraph_{l}', e)
    return out


def match_holdings_sina(event: Dict) -> List[tuple]:
    """和 announcement_stream.match_holdings 类似的关键词匹配, 跨市场"""
    title = event.get('title', '')
    hits = []
    for sym, cfg in PORTFOLIO.items():
        if cfg.get('monitor_type') == 'EXIT_PENDING':
            continue
        kws = cfg.get('news_keywords', [])
        if not kws:
            continue
        # 关键词 OR 名称
        name = cfg.get('name', '')
        if any(kw in title for kw in kws) or (name and name in title):
            hits.append((sym, cfg))
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

    new_in_db = 0
    pushed = 0
    # 2026-06-19 P0: 导入 relevance 分类 (避免循环导入延迟到第一次调用)
    from feishu_push import _classify_relevance
    for ev in unique:
        # 先匹配, 拿命中的 symbol 列表
        hits = match_holdings_sina(ev)
        # symbol 字段: 逗号分隔多标的, 命中为空时留 None (宏观新闻)
        sym_field = ','.join(sorted({h[0] for h in hits})) if hits else None

        # relevance: 7x24 流一条新闻可能命中多持仓, relevance 取最高的
        # (primary 优先, 没人 primary 才取 thematic, 都没就 weak)
        rel_rank = {'primary': 3, 'thematic': 2, 'weak': 1}
        best_relevance = 'weak'
        for sym, cfg in hits:
            r = _classify_relevance(cfg.get('name', sym), ev['title'], cfg.get('news_keywords', []))
            if rel_rank.get(r, 0) > rel_rank.get(best_relevance, 0):
                best_relevance = r

        is_new = save_event(
            source='cls_telegraph',   # 沿用设计稿 tag
            source_id=ev['source_id'] or '',
            symbol=sym_field,         # 2026-06-19 P0: 写回命中的 symbol
            title=ev['title'],
            url=ev['url'],
            pub_date=ev['pub_date'] or '',
            pub_date_precision='datetime',  # 新浪是 Unix 时间戳, 精确到秒
            relevance=best_relevance if hits else None,  # 2026-06-19 P0: 写回 relevance
        )
        if is_new:
            new_in_db += 1

        # 推送去重: 只对"新入库"的事件推送, 已存在不重复推 (2026-06-19 P0)
        # 7×24 流是全市场流, 一条新闻会反复命中同一持仓的关键词, 不去重会刷屏
        if not is_new:
            continue  # 已入库, 跳过推送 (避免噪声)

        # 关键词命中推送 (2026-06-19 P0: 只推 primary, thematic/weak 降级到每日汇总)
        # 阈值可调: PUSH_RELEVANCE_MIN = 'thematic' 推主题+主体; 默认 'primary' 只推主体
        from feishu_push import _classify_relevance
        PUSH_RELEVANCE_MIN = 'primary'  # 默认只推主体相关, 主题/弱相关不推 (避免噪声)
        for sym, cfg in hits:
            name = cfg.get('name', sym)
            keywords = cfg.get('news_keywords', [])
            relevance = _classify_relevance(name, ev['title'], keywords)
            # relevance 强度过滤
            rel_order = {'primary': 3, 'thematic': 2, 'weak': 1}
            if rel_order.get(relevance, 0) < rel_order.get(PUSH_RELEVANCE_MIN, 3):
                continue  # 不达阈值, 跳过
            if dry_run:
                print(f"    [DRY-RUN] 推 → {name}({sym}) [{relevance}]: {ev['title'][:50]}")
            else:
                send_keyword_news_alert(name, sym, ev['title'], ev['url'], keywords, relevance=relevance)
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
