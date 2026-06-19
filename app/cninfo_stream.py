"""
cninfo_stream.py — A 股公告流备用源 (巨潮资讯网, 2026-06-19 P1)

为什么需要备用源:
  - 东财 np-anotice-stock 接口多次 SSL EOF / 超时, 公告流单点故障
  - 巨潮 (cninfo.com.cn) 是证监会指定 A 股公告官方源, 数据质量 > 东财
  - POST hisAnnouncement/query 返 JSON, 结构化字段:
    announcementId / secCode / secName / announcementTitle /
    announcementTime (ms timestamp) / adjunctUrl (PDF 路径)

调度: 15 分钟一次, 与 announcement_stream 并行
  - 主源 (东财) 失败时, 巨潮是兜底
  - 巨潮主键 (announcementId) 唯一, 与东财主键不同 → 同一条公告不会被去重冲突
  - 推送逻辑复用 announcement_stream 的 match_holdings (按持仓 symbol + 关键词)

可靠性:
  - requests POST 加重试 (3 次, 退避 2+4+8s)
  - 整体 60s 硬超时 (ThreadPoolExecutor 强制)
  - 失败写 data_source_failures 表, watchdog 检测
"""
import sqlite3
import time
import re
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import requests

from config import PORTFOLIO
from models import get_db, record_source_failure
from announcement_stream import save_event, match_holdings, _normalize_a_share_symbol
from feishu_push import send_announcement_alert, send_keyword_news_alert


# 巨潮 POST 端点
CNINFO_URL = 'https://www.cninfo.com.cn/new/hisAnnouncement/query'
CNINFO_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Origin': 'http://www.cninfo.com.cn',
    'Referer': 'http://www.cninfo.com.cn/new/disclosure/stock?stockCode=600941',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'X-Requested-With': 'XMLHttpRequest',
}

# 两个市场分两次拉 (column + plate 都要设)
MARKETS = [
    {'column': 'sse',  'plate': 'sh', 'name': '上交所'},
    {'column': 'szse', 'plate': 'sz', 'name': '深交所'},
]

# 重试策略
MAX_RETRIES = 3
RETRY_BACKOFF = [2, 4, 8]  # 第一次失败等 2s, 第二次 4s, 第三次 8s


# ── 抓取 ─────────────────────────────────────────────────────

def _fetch_cninfo_one_market(column: str, plate: str, date_str: str,
                              page_size: int = 100, max_pages: int = 3) -> List[Dict]:
    """
    拉单个市场单日所有公告.
    date_str: 'YYYY-MM-DD' 格式
    返回 [{code, name, title, url, pub_date, source_id}, ...]
    """
    results = []
    for page in range(1, max_pages + 1):
        data = {
            'stock': '', 'tabName': 'fulltext',
            'pageSize': str(page_size), 'pageNum': str(page),
            'column': column, 'category': '', 'plate': plate,
            'seDate': f'{date_str}~{date_str}',
            'searchkey': '', 'secid': '',
            'sortName': 'time', 'sortType': 'desc',
            'isHLtitle': 'true',
        }
        r = requests.post(CNINFO_URL, headers=CNINFO_HEADERS, data=data, timeout=12)
        r.raise_for_status()
        j = r.json()
        ann_list = j.get('announcements') or []
        if not ann_list:
            break
        for a in ann_list:
            ts_ms = a.get('announcementTime', 0)
            if ts_ms:
                # ms timestamp → 'YYYY-MM-DD HH:MM:SS'
                pub_dt = datetime.utcfromtimestamp(ts_ms / 1000) + timedelta(hours=8)
                pub_date = pub_dt.strftime('%Y-%m-%d %H:%M:%S')
            else:
                pub_date = date_str + ' 00:00:00'

            # adjunctUrl 是相对路径, 拼完整 URL
            adjunct = a.get('adjunctUrl', '')
            full_url = (f'http://static.cninfo.com.cn/{adjunct}'
                        if adjunct and not adjunct.startswith('http')
                        else adjunct)

            results.append({
                'code': a.get('secCode', ''),
                'name': a.get('secName', ''),
                'title': re.sub(r'<[^>]+>', '', a.get('announcementTitle', '')),  # 去 HTML 标签
                'url': full_url,
                'pub_date': pub_date,
                'pub_date_precision': 'datetime',  # 巨潮精确到秒 (优于东财的 day)
                'source_id': f'cninfo_{a.get("announcementId", "")}',
            })
        if len(ann_list) < page_size:
            break  # 末页
    return results


def fetch_cninfo_announcements(date_str: Optional[str] = None,
                                hard_timeout: int = 60) -> List[Dict]:
    """
    拉指定日期全市场 (SSE + SZSE) 公告, 带重试和硬超时.
    date_str: 'YYYY-MM-DD' 格式; 默认今天
    """
    if not date_str:
        date_str = datetime.now().strftime('%Y-%m-%d')

    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutTimeout

    def _do():
        all_results = []
        for m in MARKETS:
            for attempt in range(MAX_RETRIES):
                try:
                    items = _fetch_cninfo_one_market(m['column'], m['plate'], date_str)
                    all_results.extend(items)
                    print(f"    [cninfo] {m['name']} {date_str}: {len(items)} 条")
                    break
                except Exception as e:
                    wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF)-1)]
                    print(f"    [cninfo] {m['name']} 第{attempt+1}次失败: {type(e).__name__}: {str(e)[:80]}")
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(wait)
                    else:
                        # 最终失败: 写 data_source_failures
                        record_source_failure(
                            source_name='cninfo_hisAnnouncement',
                            error=f"{m['name']} {date_str} 三次重试均失败: {type(e).__name__}: {str(e)[:200]}"
                        )
        return all_results

    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_do)
        try:
            return fut.result(timeout=hard_timeout)
        except FutTimeout:
            record_source_failure(
                source_name='cninfo_hisAnnouncement',
                error=f'硬超时 {hard_timeout}s'
            )
            print(f"    [cninfo] 硬超时 {hard_timeout}s, 返回空")
            return []


# ── 主流程 ───────────────────────────────────────────────────

def run_cninfo_stream(date_str: Optional[str] = None, dry_run: bool = False,
                      push: bool = True) -> Dict:
    """
    一轮巨潮公告流: 拉今日全市场 → 入 events → 持仓命中推送
    与 announcement_stream.run_announcement_stream 完全独立
    """
    started = datetime.now()
    if not date_str:
        date_str = started.strftime('%Y-%m-%d')

    print(f"\n{'='*55}")
    print(f"  巨潮公告流 — {started.strftime('%Y-%m-%d %H:%M')}  date={date_str}")
    print(f"{'='*55}")

    items = fetch_cninfo_announcements(date_str=date_str)
    print(f"  [cninfo] 抓取 {len(items)} 条")

    new_in_db = 0
    pushed = 0
    for ev in items:
        # 巨潮主键已加 'cninfo_' 前缀, 与东财主键不冲突
        is_new = save_event(
            source='cn_announcement_cninfo',  # 与东财 'cn_announcement' 区分
            source_id=ev.get('source_id', ''),
            symbol=ev.get('code', ''),  # 巨潮有明确 secCode
            title=ev.get('title', ''),
            url=ev.get('url', ''),
            pub_date=ev.get('pub_date', ''),
            pub_date_precision='datetime',
        )
        if is_new:
            new_in_db += 1

        if not is_new or not push:
            continue

        # 复用 announcement_stream 的持仓匹配逻辑
        hits = match_holdings(ev)
        for sym, cfg in hits:
            name = cfg.get('name', sym)
            monitor_type = cfg.get('monitor_type', '')
            keywords = cfg.get('news_keywords', [])
            if dry_run:
                print(f"    [DRY-RUN] 推 → {name}({sym}): {ev.get('title','')[:50]}")
            else:
                if monitor_type == 'EVENT_DRIVEN':
                    send_keyword_news_alert(name, sym, ev.get('title',''), ev.get('url',''), keywords)
                else:
                    send_announcement_alert(name, sym, ev.get('title',''), ev.get('url',''))
            pushed += 1

    stats = {
        'job_name': 'cninfo_stream',
        'status': 'ok' if items else 'failed',
        'symbols_processed': new_in_db,
        'symbols_failed': 0 if items else 1,
        'last_error': None if items else '巨潮全市场抓取空 (网络/限流?)',
        'started_at': started.strftime('%Y-%m-%d %H:%M:%S'),
        'fetched': len(items),
        'new_in_db': new_in_db,
        'pushed': pushed,
    }
    print(f"  [cninfo] 新入库 {new_in_db}, 推送 {pushed}")
    return stats


if __name__ == '__main__':
    # 干跑
    s = run_cninfo_stream(dry_run=True)
    print('\n=== stats ===')
    for k, v in s.items():
        print(f'  {k}: {v}')
