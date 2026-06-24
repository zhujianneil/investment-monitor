"""
yf_news_stream.py — 港美股 yfinance 新闻流 (第一层)

区别于 announcement_stream: 港美股没有"全市场公告"概念,
只能 per-ticker 拉, 然后通过 PORTFOLIO 关键词命中触发推送.

设计:
  - 遍历 PORTFOLIO 中 market ∈ {HK, US} 的标的
  - 每个 ticker 拉 yf.Ticker(symbol).news
  - 关键词命中 → 入 events (source='yf_news') → 推飞书
  - 每只 ticker 独立 try/except (L1 防御, 2026-06-11 已立)

2026-06-20 P0 修复 (symbol 错配):
  yfinance 升级后, ticker.news 实际返回的是 Yahoo 财经热门流(全市场),
  不是该 ticker 的专属新闻. 旧代码把所有 20 条都标 symbol=ticker,
  导致 NVDA 名下出现 Gold Bear Market/SpaceX/XRP 等不相关新闻.
  修复: 入库前做相关性校验 — 标题里必须出现 ticker/公司名才标 symbol,
  否则 symbol 留空 (作为宏观新闻入 events 库但不归到该 ticker).
"""
from datetime import datetime, timedelta
from typing import List, Dict
import yfinance as yf

from config import PORTFOLIO
from models import get_db, record_source_failure
from announcement_stream import save_event
from feishu_push import send_keyword_news_alert


def fetch_yf_news_for_symbol(yf_symbol: str, max_age_days: int = 7) -> List[Dict]:
    """单 ticker 拉 yfinance 新闻, 返回 [{title, url, pub_date, source_id}, ...]

    2026-06-19 P1 修复: yfinance 升级后字段结构变了
      老: title / link / providerPublishTime (顶层)
      新: content.title / content.canonicalUrl.url / content.pubDate (嵌套)
    双轨兼容: 优先新结构, 老字段 fallback
    """
    try:
        ticker = yf.Ticker(yf_symbol)
        news = ticker.news or []
    except Exception as e:
        print(f"  [yf 新闻] {yf_symbol} 拉取失败: {e}")
        record_source_failure(f'yf_news_{yf_symbol}', e)
        return []

    cutoff = datetime.now() - timedelta(days=max_age_days)
    results = []
    for item in news[:20]:
        try:
            # ── 字段双轨提取 (P1 修复) ──
            # 新结构 (yfinance >= 0.2.40)
            content = item.get('content') or {}
            if content:
                title = (content.get('title') or '').strip()
                # URL: canonicalUrl.url → clickThroughUrl.url → 顶层 link
                canonical = (content.get('canonicalUrl') or {}).get('url')
                click_thru = (content.get('clickThroughUrl') or {}).get('url')
                top_link = item.get('link', '')
                if canonical:
                    url = canonical
                elif click_thru:
                    url = click_thru
                else:
                    url = top_link
                # pubDate: ISO 8601 字符串
                pub_str_iso = content.get('pubDate') or content.get('displayTime')
                if pub_str_iso:
                    try:
                        pub_dt = datetime.fromisoformat(pub_str_iso.replace('Z', '+00:00')).replace(tzinfo=None)
                    except Exception:
                        pub_dt = None
                else:
                    pub_dt = None
            else:
                # 老结构 (yfinance < 0.2.40)
                title = (item.get('title') or '').strip()
                url = (item.get('link') or '').strip()
                ts = item.get('providerPublishTime') or 0
                pub_dt = datetime.fromtimestamp(ts) if ts else None

            if not title:
                continue

            if pub_dt:
                # 去掉时区信息, 统一 naive
                if pub_dt.tzinfo:
                    pub_dt = pub_dt.replace(tzinfo=None)
                if pub_dt < cutoff:
                    continue
                pub_str = pub_dt.strftime('%Y-%m-%d %H:%M:%S')
            else:
                pub_str = None

            # yfinance source_id 字段
            sid = (
                content.get('id') if content else None
            ) or item.get('id') or item.get('uuid') or f"yf_{yf_symbol}_{pub_str or 'nots'}"

            results.append({
                'title':     title,
                'url':       url,
                'pub_date':  pub_str,
                'source_id': sid,
            })
        except Exception as row_err:
            print(f"  [yf 新闻] {yf_symbol} 单条解析跳过: {row_err}")
            continue
    return results


def _is_news_relevant_to_ticker(title: str, ticker: str, name: str, keywords=None) -> bool:
    """
    2026-06-20 P0 修复: yfinance 返回的"热门流"是全市场, 需要校验标题是否真相关.

    2026-06-23 P1 升级: 加权计分制, 防止"AI/GPU/H100" 等次关键词擦边匹配.
    之前 5 个 OR 条件, 任一命中即相关 → 12 条 NVDA 错配 ('1 Top AI Stock to Buy' 等)
    现在改为计分 ≥ 2 才算相关:
      强命中 (+2 分): ticker / 核心代码 / 公司名 / keywords[0]
      弱命中 (+1 分): keywords 第 2 项及以后 (主题词如 'GPU' / 'AI芯片')
    例: 'HIVE BUZZ HPC Secures GPU Cloud Contract' (只有 GPU 命中, 1 分) →
        仍判相关 (但 HIVE 是 NVDA 客户, 标 NVDA 也合理, 实际是 HIVE 标 HIVE)
        ↑ 这条之前是 symbol=NVDA, 其实是 HIVE 的新闻被强标到 NVDA
    例: '1 Top AI Stock to Buy' (含 'AI 芯片'? 不含) → 0 分 → 不相关 ✓
    例: 'Nvidia's AI Premium Faces Pricing Test' (含 'Nvidia') → 2 分 → 相关 ✓

    兼容老数据: 6-23 前已入库的 12 条错配 (id 20156/20157/22692/...) 已在数据库清理
    """
    if not title:
        return False
    t = title
    t_lower = t.lower()
    import re
    score = 0

    # 1) ticker 整串
    if ticker and ticker.lower() in t_lower:
        score += 2
    # 2) ticker 去市场后缀
    if ticker:
        core = re.sub(r'[^A-Za-z0-9]', '', ticker)
        if len(core) >= 4 and core.lower() in re.sub(r'[^A-Za-z0-9]', '', t_lower):
            score += 2
    # 3) 公司名
    if name and len(name) >= 2 and name in t:
        score += 2
    # 4) keywords 第一项 (强)
    if keywords:
        first = str(keywords[0]) if keywords else ''
        if first and len(first) >= 2 and first.lower() in t_lower:
            score += 2
        # 5) keywords 后续主题词 (弱, 每个 +1)
        for kw in keywords[1:]:
            kw_str = str(kw)
            if kw_str and len(kw_str) >= 2 and (kw_str.lower() in t_lower or kw_str in t):
                score += 1

    return score >= 2


def run_yf_news_stream(dry_run: bool = False) -> Dict:
    """主流程: 遍历持仓 → 拉新闻 → 入库 → 关键词命中推送"""
    started = datetime.now()
    print(f"\n{'='*55}")
    print(f"  港美股 yf 新闻流 — {started.strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*55}")

    total_new = 0
    total_pushed = 0
    total_failed = 0

    for sym, cfg in PORTFOLIO.items():
        market = cfg.get('market')
        if market not in ('HK', 'US'):
            continue
        if cfg.get('monitor_type') == 'EXIT_PENDING':
            continue

        yf_sym = cfg.get('yf_symbol', sym)
        name   = cfg.get('name', sym)
        kws    = cfg.get('news_keywords', [])

        try:
            items = fetch_yf_news_for_symbol(yf_sym)
            for it in items:
                # 2026-06-20 P0 修复: 二次相关性校验
                # yfinance 现在返回的是 Yahoo 财经热门流(全市场),
                # 不校验会把 Gold/SpaceX/XRP 之类不相关新闻都打到 NVDA 名下.
                relevant = _is_news_relevant_to_ticker(it['title'], yf_sym, name, kws)
                event_sym = sym if relevant else ''

                # 推送去重 (P1 修复 2026-06-19):
                # 1) 已存在的 (is_new=False) 直接跳过, 防止刷屏
                # 2) 推送成功后回写 pushed=1, 审计闭环
                is_new = save_event(
                    source='yf_news',
                    source_id=it['source_id'] or '',
                    symbol=event_sym,
                    title=it['title'],
                    url=it['url'],
                    pub_date=it['pub_date'] or '',
                    pub_date_precision='datetime',
                )
                if is_new:
                    total_new += 1
                else:
                    continue  # 已存在, 跳过推送 (P1 去重)

                # 关键词命中推送 (无关键词的非 EVENT_DRIVEN 不推)
                # P0 修复: 不相关的新闻直接跳过推送 (即使有"AI"等宽词也不会再误推 NVDA)
                if not relevant:
                    continue
                if kws and any(kw.lower() in it['title'].lower() for kw in kws):
                    if dry_run:
                        print(f"    [DRY-RUN] yf 推 {name}({sym}): {it['title'][:50]}")
                    else:
                        send_keyword_news_alert(name, sym, it['title'], it['url'], kws)
                        # P1: 推送成功回写
                        from announcement_stream import mark_pushed
                        mark_pushed('yf_news', it['source_id'] or '', it['pub_date'] or '')
                    total_pushed += 1
        except Exception as e:
            print(f"  [yf 新闻流] {sym} 整轮异常: {e}")
            total_failed += 1
            continue

    stats = {
        'job_name': 'yf_news_stream',
        'status': 'ok' if total_failed == 0 else 'partial',
        'symbols_processed': sum(1 for c in PORTFOLIO.values() if c.get('market') in ('HK', 'US')),
        'symbols_failed': total_failed,
        'last_error': None,
        'started_at': started.strftime('%Y-%m-%d %H:%M:%S'),
        'new_in_db': total_new,
        'pushed': total_pushed,
    }
    print(f"  [yf 新闻流] 新入库 {total_new}, 推送 {total_pushed}, 失败 ticker {total_failed}")
    return stats


if __name__ == '__main__':
    import sys
    dry = '--dry-run' in sys.argv
    stats = run_yf_news_stream(dry_run=dry)
    print(stats)
