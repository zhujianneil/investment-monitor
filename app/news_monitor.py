"""
news_monitor.py — 关键词新闻监控

哲学：不是抓所有公告，是针对每个持仓的核心监控变量做关键词过滤。
- EVENT_DRIVEN 持仓：紧密监控（Visa/DOJ、中国移动/政策）
- VALUE_WATCHER / EARNINGS_WAIT：过滤核心关键词，发现才推送
- EXIT_PENDING：不监控
"""
import akshare as ak
import requests
from datetime import datetime, timedelta
from config import PORTFOLIO
from models import save_announcement
from feishu_push import send_keyword_news_alert, send_announcement_alert


# ── A股公告抓取 ──────────────────────────────────────────

def get_cn_announcements(akshare_symbol, name, keywords):
    """
    获取A股公告，按关键词过滤后返回。
    keywords 为空表示捕获所有公告（通常只对 EVENT_DRIVEN 生效）。
    """
    results = []
    try:
        df = ak.stock_notice_report(symbol=akshare_symbol)
        if df.empty:
            return results

        for _, row in df.head(10).iterrows():
            title = str(row.get('公告标题', ''))
            url   = str(row.get('公告链接', ''))
            date  = str(row.get('公告日期', datetime.now().date()))

            # 无关键词 = 全量；有关键词 = 过滤
            if keywords and not any(kw in title for kw in keywords):
                continue

            results.append({'title': title, 'url': url, 'pub_date': date})
    except Exception as e:
        print(f"  [公告] 获取 {name}({akshare_symbol}) 失败: {e}")
    return results


# ── Yahoo Finance 新闻（港股/美股）───────────────────────

def get_yf_news(yf_symbol, name, keywords):
    """
    从 yfinance 获取新闻摘要，按关键词过滤。
    适用于港股、美股。
    """
    results = []
    try:
        import yfinance as yf
        ticker = yf.Ticker(yf_symbol)
        news = ticker.news or []

        for item in news[:15]:
            title   = item.get('title', '')
            url     = item.get('link', '')
            pub_ts  = item.get('providerPublishTime', 0)
            pub_date = datetime.fromtimestamp(pub_ts).strftime('%Y-%m-%d') if pub_ts else str(datetime.now().date())

            # 只看最近 3 天内的新闻
            if pub_ts and (datetime.now() - datetime.fromtimestamp(pub_ts)).days > 3:
                continue

            if keywords and not any(kw.lower() in title.lower() for kw in keywords):
                continue

            results.append({'title': title, 'url': url, 'pub_date': pub_date})
    except Exception as e:
        print(f"  [新闻] 获取 {name}({yf_symbol}) 新闻失败: {e}")
    return results


# ── 核心监控逻辑 ──────────────────────────────────────────

def monitor_news():
    print(f"\n{'='*55}")
    print(f"  新闻 & 公告监控 — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*55}")

    total_new_ref = [0]  # 列表包装便于内层函数修改

    for symbol, cfg in PORTFOLIO.items():
        # 2026-06-11 L1 防御：每只 symbol 独立 try/except，一只崩不连累整轮
        try:
            _process_one_news(symbol, cfg, total_new_ref)
        except Exception as e:
            print(f"  ✗✗ {symbol} 新闻处理异常（已隔离）: {type(e).__name__}: {str(e)[:200]}")
            continue

    print(f"\n  新闻监控完成 — 新内容 {total_new_ref[0]} 条")
    return total_new_ref[0]


def _process_one_news(symbol, cfg, total_new_ref):
    """
    处理单只持仓的新闻/公告流程（2026-06-11 L1 防御抽离）。
    total_new_ref 是 [int] 包装（list 是 Python 跨闭包修改的标准技巧）。
    """
    name         = cfg['name']
    market       = cfg['market']
    monitor_type = cfg['monitor_type']
    keywords     = cfg.get('news_keywords', [])

    # EXIT_PENDING：跳过
    if monitor_type == 'EXIT_PENDING':
        return

    # EARNINGS_WAIT 且无关键词：也跳过日常新闻
    if monitor_type == 'EARNINGS_WAIT' and not keywords:
        return

    print(f"\n  [{monitor_type}] {name}({symbol}) — 关键词: {keywords[:3]}...")

    # ── A 股：公告 + 关键词过滤 ──
    if market == 'CN':
        akshare_sym = cfg.get('akshare_symbol', symbol)
        # EVENT_DRIVEN：不过滤，捕获所有公告
        kws = [] if monitor_type == 'EVENT_DRIVEN' else keywords
        announcements = get_cn_announcements(akshare_sym, name, kws)

        for ann in announcements:
            is_new = save_announcement(symbol, ann['title'], ann['url'], ann['pub_date'])
            if is_new:
                print(f"    ✦ [新公告] {ann['title'][:50]}")
                send_announcement_alert(name, symbol, ann['title'], ann['url'])
                total_new_ref[0] += 1

    # ── 港股 / 美股：yfinance 新闻 ──
    else:
        yf_sym = cfg.get('yf_symbol', symbol)
        # EVENT_DRIVEN：关键词密集，宁滥勿缺
        kws = keywords if monitor_type == 'EVENT_DRIVEN' else keywords
        news_items = get_yf_news(yf_sym, name, kws)

        for item in news_items:
            is_new = save_announcement(symbol, item['title'], item['url'], item['pub_date'])
            if is_new:
                print(f"    ✦ [新闻] {item['title'][:60]}")
                # EVENT_DRIVEN 单独推送，其他持仓汇总到周报
                if monitor_type == 'EVENT_DRIVEN':
                    send_keyword_news_alert(name, symbol, item['title'], item['url'], keywords)
                total_new_ref[0] += 1
