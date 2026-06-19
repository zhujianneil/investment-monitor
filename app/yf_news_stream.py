"""
yf_news_stream.py — 港美股 yfinance 新闻流 (第一层)

区别于 announcement_stream: 港美股没有"全市场公告"概念,
只能 per-ticker 拉, 然后通过 PORTFOLIO 关键词命中触发推送.

设计:
  - 遍历 PORTFOLIO 中 market ∈ {HK, US} 的标的
  - 每个 ticker 拉 yf.Ticker(symbol).news
  - 关键词命中 → 入 events (source='yf_news') → 推飞书
  - 每只 ticker 独立 try/except (L1 防御, 2026-06-11 已立)
"""
from datetime import datetime, timedelta
from typing import List, Dict
import yfinance as yf

from config import PORTFOLIO
from models import get_db, record_source_failure
from announcement_stream import save_event
from feishu_push import send_keyword_news_alert


def fetch_yf_news_for_symbol(yf_symbol: str, max_age_days: int = 7) -> List[Dict]:
    """单 ticker 拉 yfinance 新闻, 返回 [{title, url, pub_date, source_id}, ...]"""
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
            title = (item.get('title') or '').strip()
            url   = (item.get('link')  or '').strip()
            ts    = item.get('providerPublishTime') or 0

            if not title:
                continue

            if ts:
                pub_dt = datetime.fromtimestamp(ts)
                if pub_dt < cutoff:
                    continue
                pub_str = pub_dt.strftime('%Y-%m-%d %H:%M:%S')
            else:
                pub_dt = None
                pub_str = None

            # yfinance 给的 source_id 字段 (uuid)
            sid = item.get('uuid') or item.get('id') or f"yf_{yf_symbol}_{ts}"

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
                # 入库 (去重)
                is_new = save_event(
                    source='yf_news',
                    source_id=it['source_id'] or '',
                    symbol=sym,
                    title=it['title'],
                    url=it['url'],
                    pub_date=it['pub_date'] or '',
                    pub_date_precision='datetime',  # yfinance 给的是 publishTime
                )
                if is_new:
                    total_new += 1

                # 关键词命中推送 (无关键词的非 EVENT_DRIVEN 不推)
                if kws and any(kw.lower() in it['title'].lower() for kw in kws):
                    if dry_run:
                        print(f"    [DRY-RUN] yf 推 {name}({sym}): {it['title'][:50]}")
                    else:
                        send_keyword_news_alert(name, sym, it['title'], it['url'], kws)
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
