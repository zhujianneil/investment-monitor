"""
news_monitor.py — 关键词新闻监控 (legacy, 2026-06-19 部分弃用)

哲学: 不是抓所有公告, 是针对每个持仓的核心监控变量做关键词过滤.
- EVENT_DRIVEN 持仓: 紧密监控 (Visa/DOJ、中国移动/政策)
- VALUE_WATCHER / EARNINGS_WAIT: 过滤核心关键词, 发现才推送
- EXIT_PENDING: 不监控

2026-06-19 P0 修复:
  原 get_cn_announcements 用错 akshare 接口:
    df = ak.stock_notice_report(symbol=akshare_symbol)
  实际 ak.stock_notice_report 的 symbol 参数是"报告类型" ('全部'/'重大事项'/...),
  不是股票代码. 传股票代码触发 KeyError, 被 try/except 静默吞, 导致
  announcements 表 1.5 月空. 修法: 改用 announcement_stream 里的实现,
  按"重大事项"类全市场抓, 客户端按 symbol 匹配持仓.

注意: 2026-06-19 后, scheduler 的 job_daily_news (每天 9:00) 已由
announcement_stream (15min 一次) + cls_stream (15min 一次) + yf_news_stream
(60min 一次) 接管. 本文件保留仅作回退 + 代码示例, 不再被 scheduler 主动调用.
"""
import akshare as ak
import requests
from datetime import datetime, timedelta
from config import PORTFOLIO
from models import save_announcement
from feishu_push import send_keyword_news_alert, send_announcement_alert


# ── A股公告抓取 (2026-06-19 P0 修复) ─────────────────────

def get_cn_announcements(akshare_symbol, name, keywords):
    """
    获取 A 股公告, 按 symbol 过滤 + 关键词过滤.
    keywords 为空表示捕获所有公告 (通常只对 EVENT_DRIVEN 生效).

    2026-06-19 P1 重构: 废弃 akshare.stock_notice_report (东财接口列名已变, KeyError '代码')
    改用 cninfo_stream (巨潮官方源) 的当日公告数据
    """
    results = []
    try:
        from datetime import datetime as _dt
        today_str = _dt.now().strftime('%Y-%m-%d')
        # 调 cninfo_stream 的 fetch (POST 巨潮, 一次拿到全市场 SSE+Szse 60 条)
        from cninfo_stream import fetch_cninfo_announcements
        items = fetch_cninfo_announcements(date_str=today_str, hard_timeout=20)
        if not items:
            return results

        target_code = str(akshare_symbol).strip()
        for ev in items:
            code  = str(ev.get('code', '')).strip()
            title = str(ev.get('title', '')).strip()
            url   = str(ev.get('url', '')).strip()
            pub   = ev.get('pub_date', today_str)

            if code != target_code:
                continue
            if keywords:
                if not any(kw in title for kw in keywords):
                    continue

            results.append({
                'title': title,
                'url':   url,
                'pub':   pub,
            })
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
            # 2026-06-19 P0 修复: yfinance 0.2.36+ 把字段从 providerPublishTime 改为 publishTime
            # 同时嵌套结构改成了 {'content': {...}, 'contentType': 'STORY'}
            content = item.get('content', item) if isinstance(item, dict) else {}
            title = (
                content.get('title')
                or item.get('title', '')
            )
            # URL 多种来源
            url = (
                content.get('canonicalUrl', {}).get('url') if isinstance(content.get('canonicalUrl'), dict) else None
                or content.get('clickThroughUrl', {}).get('url') if isinstance(content.get('clickThroughUrl'), dict) else None
                or item.get('link', '')
            )
            # 时间戳: pubDate (ISO 字符串) 或 providerPublishTime (旧)
            pub_ts = 0
            pub_date_str = content.get('pubDate') or item.get('pubDate')
            if pub_date_str:
                try:
                    # ISO 格式 "2026-06-19T08:30:00Z"
                    pub_dt = datetime.fromisoformat(pub_date_str.replace('Z', '+00:00'))
                    pub_ts = int(pub_dt.timestamp())
                except (ValueError, AttributeError):
                    pub_ts = 0
            if not pub_ts:
                pub_ts = int(item.get('providerPublishTime', 0) or 0)

            if not title:
                continue

            # 只看最近 3 天内的新闻
            if pub_ts and (datetime.now() - datetime.fromtimestamp(pub_ts)).days > 3:
                continue

            if keywords and not any(kw.lower() in title.lower() for kw in keywords):
                continue

            pub_date = datetime.fromtimestamp(pub_ts).strftime('%Y-%m-%d %H:%M:%S') if pub_ts else str(datetime.now().date())
            results.append({'title': title, 'url': url, 'pub_date': pub_date})
    except Exception as e:
        print(f"  [新闻] 获取 {name}({yf_symbol}) 新闻失败: {e}")
    return results


# ── 核心监控逻辑 ──────────────────────────────────────────

def monitor_news():
    """
    每天 9:00 一次的总入口 (legacy, 2026-06-19 后大部分功能被新流替代).

    2026-06-19 P0: 整体跑一次最多 60s (ThreadPoolExecutor 硬超时), 防止
    东财接口 SSL 抖导致 legacy 卡死影响后续 9:00 任务. 60s 内没完成就
    强制结束当前 batch, 后续持仓下一轮再处理.

    现在的工作:
      - 对每个持仓拉一次"重大事项"公告, 按 symbol + 关键词过滤
      - 关键词命中 → 入 announcements 表 (历史镜像) + 推飞书
      - 港美股 → 拉 yf 新闻
    """
    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutTimeout
    print(f"\n{'='*55}")
    print(f"  [legacy] 关键词新闻监控 — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*55}")

    total_new_ref = [0]
    market_cache = {'fetched': False, 'rows': [], 'date': ''}  # 跨持仓复用

    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_run_monitor_news_inner, total_new_ref, market_cache)
        try:
            fut.result(timeout=60)  # 2026-06-19 60s 硬超时
        except FutTimeout:
            print(f"  [legacy] ⚠ 硬超时 60s 强制退出 (东财接口可能慢, 下次再跑)")
        except Exception as e:
            print(f"  [legacy] ✗ 异常: {type(e).__name__}: {e}")

    print(f"\n  [legacy] 完成 — 新内容 {total_new_ref[0]} 条")
    return total_new_ref[0]


def _run_monitor_news_inner(total_new_ref, market_cache):
    """legacy 主循环, 单独函数便于 ThreadPoolExecutor 包超时"""
    for symbol, cfg in PORTFOLIO.items():
        try:
            _process_one_news(symbol, cfg, total_new_ref, market_cache)
        except Exception as e:
            print(f"  ✗✗ {symbol} 新闻处理异常 (已隔离): {type(e).__name__}: {str(e)[:200]}")
            continue


def _process_one_news(symbol, cfg, total_new_ref, market_cache=None):
    """
    处理单只持仓的新闻/公告流程 (2026-06-19 P0 优化).
    total_new_ref 是 [int] 包装 (list 是 Python 跨闭包修改的标准技巧).
    market_cache: 跨持仓复用"重大事项"全市场公告 ({'rows': [...], 'fetched': bool})
    """
    name         = cfg['name']
    market       = cfg['market']
    monitor_type = cfg['monitor_type']
    keywords     = cfg.get('news_keywords', [])

    # EXIT_PENDING: 跳过
    if monitor_type == 'EXIT_PENDING':
        return

    # EARNINGS_WAIT 且无关键词: 也跳过日常新闻
    if monitor_type == 'EARNINGS_WAIT' and not keywords:
        return

    print(f"\n  [{monitor_type}] {name}({symbol}) — 关键词: {keywords[:3]}...")

    # ── A 股: 复用 cache, 按 symbol + 关键词过滤 ──
    if market == 'CN':
        akshare_sym = cfg.get('akshare_symbol', symbol)
        # EVENT_DRIVEN: 不过滤, 捕获所有公告
        kws = [] if monitor_type == 'EVENT_DRIVEN' else keywords
        announcements = get_cn_announcements_cached(akshare_sym, name, kws, market_cache)

        for ann in announcements:
            is_new = save_announcement(symbol, ann['title'], ann['url'], ann['pub_date'])
            if is_new:
                print(f"    ✦ [新公告] {ann['title'][:50]}")
                send_announcement_alert(name, symbol, ann['title'], ann['url'])
                total_new_ref[0] += 1

    # ── 港股 / 美股: yfinance 新闻 ──
    else:
        yf_sym = cfg.get('yf_symbol', symbol)
        # EVENT_DRIVEN: 关键词密集, 宁滥勿缺
        kws = keywords if monitor_type == 'EVENT_DRIVEN' else keywords
        news_items = get_yf_news(yf_sym, name, kws)

        for item in news_items:
            is_new = save_announcement(symbol, item['title'], item['url'], item['pub_date'])
            if is_new:
                print(f"    ✦ [新闻] {item['title'][:60]}")
                # EVENT_DRIVEN 单独推送, 其他持仓汇总到周报
                if monitor_type == 'EVENT_DRIVEN':
                    send_keyword_news_alert(name, symbol, item['title'], item['url'], keywords)
                total_new_ref[0] += 1


def get_cn_announcements_cached(akshare_symbol, name, keywords, cache: dict):
    """
    2026-06-19 P1 重构: 废弃 akshare.stock_notice_report (列名已变), 改用 cninfo_stream
    cache: {'rows': [...], 'fetched': bool, 'date': str}
    """
    results = []
    try:
        from datetime import datetime as _dt
        today = _dt.now().strftime('%Y-%m-%d')
        # 第一次调用或跨日, 重新拉 (走巨潮官方源, 一次拿 60 条全市场)
        if not cache.get('fetched') or cache.get('date') != today:
            from cninfo_stream import fetch_cninfo_announcements
            items = fetch_cninfo_announcements(date_str=today, hard_timeout=20)
            cache['rows'] = []
            for ev in (items or []):
                cache['rows'].append({
                    'code':  str(ev.get('code', '')).strip(),
                    'title': str(ev.get('title', '')).strip(),
                    'url':   str(ev.get('url', '')).strip(),
                    'pub':   ev.get('pub_date', today),
                })
            cache['fetched'] = True
            cache['date'] = today
            print(f"    [legacy] 已抓 {len(cache['rows'])} 条全市场公告 (巨潮, 复用)")

        # 按 symbol + 关键词过滤
        target_code = str(akshare_symbol).strip()
        for row in cache['rows']:
            if row['code'] != target_code:
                continue
            if keywords and not any(kw in row['title'] for kw in keywords):
                continue
            results.append({'title': row['title'], 'url': row['url'], 'pub_date': row['pub']})
    except Exception as e:
        print(f"  [公告] 获取 {name}({akshare_symbol}) 失败: {e}")
    return results
