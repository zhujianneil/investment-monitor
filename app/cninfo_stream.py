"""
cninfo_stream.py — A 股公告流 (巨潮资讯网, 2026-06-19 P1; 2026-06-25 v2 修)

为什么需要:
  - 东财 np-anotice-stock 接口多次 SSL EOF / 超时, 公告流单点故障
  - 巨潮 (cninfo.com.cn) 是证监会指定 A 股公告官方源, 数据质量 > 东财
  - POST hisAnnouncement/query 返 JSON, 结构化字段:
    announcementId / secCode / secName / announcementTitle /
    announcementTime (ms timestamp) / adjunctUrl (PDF 路径)

调度: 15 分钟一次, 与 announcement_stream (东财) 并行
  - 巨潮主键 (announcementId) 唯一, 与东财主键不同 → 同一条公告不会被去重冲突
  - 推送逻辑复用 announcement_stream 的 match_holdings (按持仓 symbol + 关键词)

可靠性:
  - requests Session 先 GET 拿 JSESSIONID 再 POST (巨潮反爬)
  - 单市场加重试 (3 次, 退避 2+4+8s)
  - 整体 60s 硬超时 (ThreadPoolExecutor 强制)
  - 失败写 data_source_failures 表, watchdog 检测
  - seDate 区间 [昨天~今天], 客户端按 announcementTime 过滤今天
    (原因: 巨潮 seDate 右边界=今天 时永远返 0; 验证见 git log)

v2 修复 (2026-06-25):
  - max_pages 10 → 20 (实际 1219 条 / 全市场 6-25, 20 页覆盖 96.6%, 12.6s)
  - SSE + SZSE 并行 fetch (从 26s 串行 → 12.6s 并行)
  - 客户端时区过滤修复: announcementTime 18:00 之后发的 ts 可能是次日 00:00
    (例: 6-25 18:00 招行股东会决议 ts=6-26 00:00), 改为"看 ts 落在 [date 00:00, date+1 18:00)"
  - 加 ticker-targeted searchkey 兜底: 9 只持仓 ticker 单独 searchkey 查 [date, date+1]
    (因为 stock= 模式巨潮匹配不到, searchkey=公司名 能查到,补全全市场 listing 漏的)
  - 新增 source='cn_announcement_cninfo_ticker' 给兜底流,跨源去重不冲突
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

# v2 (2026-06-25): max_pages 10 → 20 (实际全市场 ~1219 条 / 6-25, 20 页 96.6% 覆盖, 12.6s)
DEFAULT_MAX_PAGES = 20


# ── 抓取 ─────────────────────────────────────────────────────

def _fetch_cninfo_one_market(column: str, plate: str, date_str: str,
                              page_size: int = 30, max_pages: int = DEFAULT_MAX_PAGES) -> List[Dict]:
    """
    拉单个市场单日所有公告.
    date_str: 'YYYY-MM-DD' 格式

    2026-06-20 P0 修复: 巨潮的 seDate='YYYY-MM-DD~YYYY-MM-DD' 当右边界=今天时
    永远返 0 (反爬/数据延迟, 经验证: SSE 区间 [今天,今天] → 0 条;
    [昨天,今天] → 785 条, 首条 = 昨天 00:00). 修复: 查 [昨天~今天] 区间,
    客户端按 announcementTime 过滤只保留今天的公告.

    2026-06-25 v2 修复:
      - max_pages 默认 10 → 20 (1200+ 条全市场 96.6% 覆盖, 12.6s)
      - 客户端时区过滤修复: 巨潮把 18:00 之后发的公告 announcementTime 标为次日 00:00.
        例: 6-25 18:00 招行股东会决议 ts=1782403200000 = 6-26 00:00 CST.
        修法: 接受 ts 落在 [date_str 00:00, date_str+1 18:00) 窗口的公告,
        对外 pub_date 写 ts 实际所在日期 (date_str 或 date_str+1),保留精度.

    返回 [{code, name, title, url, pub_date, source_id, announcementTime}, ...]
    """
    # 查 [昨天~今天] 区间 (客户端过滤)
    target_date = datetime.strptime(date_str, '%Y-%m-%d').date()
    prev_date = (target_date - timedelta(days=1)).strftime('%Y-%m-%d')
    se_date = f'{prev_date}~{date_str}'

    # v2 时区窗口: 接受 [date_str 00:00 CST, date_str+1 18:00 CST) 的公告
    #   - 18:00 CST = 当日交易所收盘时间, 之后发的公告标次日 00:00
    #   - 但我们只想收"date_str 当天"的, 即 18:00 之后发的也属当天发布
    win_start_ts = int(datetime(target_date.year, target_date.month, target_date.day).timestamp() * 1000) - 8*3600*1000
    win_end_ts   = win_start_ts + 42*3600*1000  # 42h = 18h 当日 + 24h 次日
    next_date = (target_date + timedelta(days=1)).strftime('%Y-%m-%d')

    results = []
    # 巨潮 POST 反爬: 同一 session 先 GET 拿 JSESSIONID
    s = requests.Session()
    try:
        s.get('http://www.cninfo.com.cn/new/disclosure/stock?stockCode=600519',
              timeout=10, headers={'User-Agent': CNINFO_HEADERS['User-Agent']})
    except Exception:
        pass

    for page in range(1, max_pages + 1):
        data = {
            'stock': '', 'tabName': 'fulltext',
            'pageSize': str(page_size), 'pageNum': str(page),
            'column': column, 'category': '', 'plate': plate,
            'seDate': se_date,
            'searchkey': '', 'secid': '',
            'sortName': 'time', 'sortType': 'desc',
            'isHLtitle': 'true',
        }
        r = s.post(CNINFO_URL, headers=CNINFO_HEADERS, data=data, timeout=12)
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
                pub_date_iso = pub_dt.strftime('%Y-%m-%d %H:%M:%S')
                pub_date_day = pub_dt.strftime('%Y-%m-%d')
            else:
                pub_date_iso = date_str + ' 00:00:00'
                pub_date_day = date_str

            # v2 时区窗口过滤: 接受 [date_str 00:00 CST, date_str+1 18:00 CST) 的 ts
            # 修法: 18:00 之后发的公告 (服务端标 ts=次日 00:00) 也算"当天发布"
            if ts_ms and not (win_start_ts <= ts_ms < win_end_ts):
                continue  # ts 落在 [date_str 00:00, date_str+1 18:00) 之外, 跳过

            # adjunctUrl 是相对路径, 拼完整 URL
            adjunct = a.get('adjunctUrl', '')
            full_url = (f'http://static.cninfo.com.cn/{adjunct}'
                        if adjunct and not adjunct.startswith('http')
                        else adjunct)

            # v2: 写 ts 实际所在日期 (date_str 当日, 或 18:00 后跨日的次日)
            # 用 ts 实际 day (不强制 =date_str),避免 18:00 后发的被错误归到次日
            actual_pub_day = pub_date_day

            results.append({
                'code': a.get('secCode', ''),
                'name': a.get('secName', ''),
                'title': re.sub(r'<[^>]+>', '', a.get('announcementTitle', '')),  # 去 HTML 标签
                'url': full_url,
                'pub_date': actual_pub_day,  # v2: 用 ts 实际 day, 不强制 =date_str
                'pub_date_precision': 'day',
                'announcementTime': ts_ms,  # 留原始 ms 给 LLM 层用
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

    v2 (2026-06-25): SSE + SZSE 并行 fetch (ThreadPoolExecutor 2 worker)
    实际 6-25: SSE 20 页 = 577 条 (9.6s), SZSE 20 页 = 600 条 (10.2s), 并行 12.6s
    """
    if not date_str:
        date_str = datetime.now().strftime('%Y-%m-%d')

    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutTimeout, as_completed

    def _do_one_market(m: Dict) -> List[Dict]:
        """单市场: 加重试拉取, 内部串行 pageNum"""
        for attempt in range(MAX_RETRIES):
            try:
                items = _fetch_cninfo_one_market(m['column'], m['plate'], date_str)
                print(f"    [cninfo] {m['name']} {date_str}: {len(items)} 条")
                return items
            except Exception as e:
                wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                print(f"    [cninfo] {m['name']} 第{attempt+1}次失败: {type(e).__name__}: {str(e)[:80]}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(wait)
                else:
                    record_source_failure(
                        source_name='cninfo_hisAnnouncement',
                        error=f"{m['name']} {date_str} 三次重试均失败: {type(e).__name__}: {str(e)[:200]}"
                    )
        return []

    def _do() -> List[Dict]:
        all_results = []
        # v2: SSE + SZSE 并行, 不再串行
        with ThreadPoolExecutor(max_workers=len(MARKETS)) as ex:
            futs = {ex.submit(_do_one_market, m): m for m in MARKETS}
            for fut in as_completed(futs):
                m = futs[fut]
                try:
                    items = fut.result()
                    all_results.extend(items)
                except Exception as e:
                    print(f"    [cninfo] {m['name']} future 异常: {type(e).__name__}: {e}")
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


# ── v2: 持仓 ticker-targeted searchkey 兜底 ─────────────────

def _fetch_cninfo_by_searchkey(code: str, name: str, date_str: str,
                                column: str, plate: str) -> List[Dict]:
    """
    巨潮 searchkey 模式: 用公司名查 [date_str, date_str+1] 区间.
    目的: 补全 stock= 模式查不到的 ticker (例: 6-25 18:00 招行股东会决议).
    """
    url = CNINFO_URL
    headers = CNINFO_HEADERS

    target_date = datetime.strptime(date_str, '%Y-%m-%d').date()
    next_date = (target_date + timedelta(days=1)).strftime('%Y-%m-%d')
    se_date = f'{date_str}~{next_date}'

    s = requests.Session()
    try:
        s.get('http://www.cninfo.com.cn/new/disclosure/stock?stockCode=600519',
              timeout=10, headers={'User-Agent': headers['User-Agent']})
    except Exception:
        pass

    results = []
    for page in range(1, 4):  # 单 ticker 最多 3 页 (~90 条) 足够
        data = {
            'stock': '', 'tabName': 'fulltext',
            'pageSize': '30', 'pageNum': str(page),
            'column': column, 'category': '', 'plate': plate,
            'seDate': se_date,
            'searchkey': name, 'secid': '',
            'sortName': 'time', 'sortType': 'desc',
            'isHLtitle': 'true',
        }
        r = s.post(url, headers=headers, data=data, timeout=12)
        r.raise_for_status()
        j = r.json()
        anns = j.get('announcements') or []
        if not anns:
            break
        for a in anns:
            # searchkey 模式不限定 secCode (有 name 同名干扰), 但限制 code 起始
            sec_code = a.get('secCode', '')
            if not sec_code.startswith(code[:3]):  # 模糊匹配
                continue
            ts_ms = a.get('announcementTime', 0)
            if ts_ms:
                pub_dt = datetime.utcfromtimestamp(ts_ms / 1000) + timedelta(hours=8)
                pub_date_day = pub_dt.strftime('%Y-%m-%d')
            else:
                pub_date_day = date_str

            adjunct = a.get('adjunctUrl', '')
            full_url = (f'http://static.cninfo.com.cn/{adjunct}'
                        if adjunct and not adjunct.startswith('http')
                        else adjunct)

            results.append({
                'code': sec_code,
                'name': a.get('secName', ''),
                'title': re.sub(r'<[^>]+>', '', a.get('announcementTitle', '')),
                'url': full_url,
                'pub_date': pub_date_day,
                'pub_date_precision': 'day',
                'announcementTime': ts_ms,
                'source_id': f'cninfo_tk_{a.get("announcementId", "")}',
            })
        if len(anns) < 30:
            break
    return results


def fetch_cninfo_holdings_fallback(date_str: Optional[str] = None,
                                     hard_timeout: int = 30) -> List[Dict]:
    """
    v2 (2026-06-25) 加: 持仓 ticker 兜底查询.
    对 9 只 A 股持仓, 各跑一次 searchkey 模式, 拼回结果.
    用于补全全市场 listing 漏掉的 (例: 18:00 后发的).
    """
    if not date_str:
        date_str = datetime.now().strftime('%Y-%m-%d')

    # 持仓 9 只 A 股 (排除港美股)
    holdings_cn = []
    for sym, cfg in PORTFOLIO.items():
        if cfg.get('market') != 'CN':
            continue
        code = cfg.get('akshare_symbol', sym)
        name = cfg.get('name', '')
        if not (code and name):
            continue
        column = 'sse' if code.startswith('6') or code.startswith('9') else 'szse'
        plate  = 'sh' if code.startswith('6') or code.startswith('9') else 'sz'
        holdings_cn.append((code, name, column, plate))

    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutTimeout, as_completed

    all_results = []
    def _do():
        with ThreadPoolExecutor(max_workers=4) as ex:
            futs = {ex.submit(_fetch_cninfo_by_searchkey, code, name, date_str, col, pl): (code, name)
                    for code, name, col, pl in holdings_cn}
            for fut in as_completed(futs):
                code, name = futs[fut]
                try:
                    items = fut.result(timeout=8)
                    all_results.extend(items)
                    if items:
                        print(f"    [cninfo-fb] {code} {name}: {len(items)} 条")
                except Exception as e:
                    print(f"    [cninfo-fb] {code} {name} 失败: {type(e).__name__}: {e}")
        return all_results

    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_do)
        try:
            return fut.result(timeout=hard_timeout)
        except FutTimeout:
            print(f"    [cninfo-fb] 硬超时 {hard_timeout}s")
            return all_results  # 返已有的, 不扔



# ── 主流程 ───────────────────────────────────────────────────

def run_cninfo_stream(date_str: Optional[str] = None, dry_run: bool = False,
                      push: bool = True) -> Dict:
    """
    一轮巨潮公告流: 拉今日全市场 + 持仓 ticker 兜底 → 入 events → 持仓命中推送
    与 announcement_stream.run_announcement_stream 完全独立

    v2 (2026-06-25):
      - 拉全市场 SSE+SZSE 20 页 (12.6s 并行)
      - 拉持仓 9 只 A 股 searchkey 兜底 (覆盖 18:00 后发的 6-26 公告)
      - 两个流合并去重入 events (source 不同 source_id 天然不同)
    """
    started = datetime.now()
    if not date_str:
        date_str = started.strftime('%Y-%m-%d')

    print(f"\n{'='*55}")
    print(f"  巨潮公告流 v2 — {started.strftime('%Y-%m-%d %H:%M')}  date={date_str}")
    print(f"{'='*55}")

    # 1) 全市场 listing (SSE + SZSE 并行 20 页)
    items_market = fetch_cninfo_announcements(date_str=date_str)
    print(f"  [cninfo-全市场] 抓取 {len(items_market)} 条")

    # 2) 持仓 ticker 兜底 (searchkey 模式, 9 只 A 股)
    items_fb = fetch_cninfo_holdings_fallback(date_str=date_str)
    print(f"  [cninfo-兜底] 抓取 {len(items_fb)} 条")

    # 3) 合并 (跨路径去重: title 前 30 + 日期 + ticker code)
    #    关键: listing 路径 source_id='cninfo_<id>', 兜底路径 source_id='cninfo_tk_<id>',
    #    同一公告这两个 source_id 字符串不同, seen 集合识别不了, 必须用 (title, date, code) 复合键
    seen = set()
    items = []
    for it in (items_market + items_fb):
        dedup_key = (it.get('title', '')[:30], it.get('pub_date', ''), it.get('code', ''))
        if dedup_key in seen:
            continue
        seen.add(dedup_key)
        items.append(it)
    print(f"  [cninfo-合并] 去重后 {len(items)} 条")

    new_in_db = 0
    pushed = 0
    skipped = 0
    for ev in items:
        # 2026-06-23 P0 修复: 公告流全市场抓, 入库前过 match_holdings
        # 只入库命中持仓的公告, symbol 字段写命中持仓的 symbol (逗号分隔)
        # 不命中的直接跳过 (不入库, 不推送, 飞书 + 网页都看不到)
        hits = match_holdings(ev)
        if not hits:
            skipped += 1
            continue

        # symbol 字段: 命中持仓的 symbol 列表 (逗号分隔, 跟 cls 一致)
        hit_syms = ','.join(s for s, _ in hits)
        # v2: source 字段区分两路
        is_from_fb = ev.get('source_id', '').startswith('cninfo_tk_')
        source = 'cn_announcement_cninfo_ticker' if is_from_fb else 'cn_announcement_cninfo'
        is_new = save_event(
            source=source,
            source_id=ev.get('source_id', ''),
            symbol=hit_syms,  # 2026-06-23 P0: 写命中持仓的 symbol, 不是公告 secCode
            title=ev.get('title', ''),
            url=ev.get('url', ''),
            pub_date=ev.get('pub_date', ''),
            pub_date_precision='day',
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
                # 跨源去重 (2026-06-20 P0): 东财可能在最近 30 分钟内推过同一条
                from announcement_stream import is_pushed_duplicate
                if is_pushed_duplicate(sym, ev.get('title', ''), ev.get('pub_date', '')):
                    print(f"    [跨源去重] 跳过 (30min 内 {sym} 已被任一 source 推过): {ev.get('title','')[:50]}")
                    continue
                if monitor_type == 'EVENT_DRIVEN':
                    send_keyword_news_alert(name, sym, ev.get('title',''), ev.get('url',''), keywords)
                else:
                    send_announcement_alert(name, sym, ev.get('title',''), ev.get('url',''))
                # P1: 推送成功回写 (审计闭环)
                from announcement_stream import mark_pushed
                mark_pushed(source, ev.get('source_id', ''), ev.get('pub_date', ''))
            pushed += 1

    stats = {
        'job_name': 'cninfo_stream',
        'status': 'ok' if items else 'failed',
        'symbols_processed': new_in_db,
        'symbols_failed': 0 if items else 1,
        'last_error': None if items else '巨潮全市场抓取空 (网络/限流?)',
        'started_at': started.strftime('%Y-%m-%d %H:%M:%S'),
        'fetched_market': len(items_market),
        'fetched_fallback': len(items_fb),
        'fetched': len(items),
        'new_in_db': new_in_db,
        'pushed': pushed,
        'skipped_non_portfolio': skipped,  # 2026-06-23 P0: 非持仓公告跳过数
    }
    print(f"  [cninfo] 新入库 {new_in_db}, 推送 {pushed}, 跳过(非持仓) {skipped}")
    return stats


if __name__ == '__main__':
    # 干跑
    s = run_cninfo_stream(dry_run=True)
    print('\n=== stats ===')
    for k, v in s.items():
        print(f'  {k}: {v}')
