"""
sina_announcement_stream.py — A 股公告流 备源 (新浪财经 vCB_AllBulletin, 2026-06-25 新增)

为什么需要:
  - 巨潮 cninfo 是 1 号源 (官方, 数据质量最高), 但 18:00 后发的公告服务端标 ts=次日 00:00
    (虽然 v2 用 searchkey 兜底能查到, 但仍依赖 searchkey 命中率, 偏门公司可能漏)
  - 新浪财经 vCB_AllBulletin 端点是 ticker 专属 HTML 页面, 单 ticker 拉历史公告
    URL: https://vip.stock.finance.sina.com.cn/corp/go.php/vCB_AllBulletin/stockid/{code}.phtml
  - HTML 格式稳定, re 解析简单, 字段干净:
    <日期>&nbsp;<a href="...">标题</a><br>
  - 实际 6-26 招行: 巨潮 searchkey 找到 2 条 / 新浪 vCB_AllBulletin 找到 2 条 (一致)
  - 实际 6-17 招行: 巨潮 searchkey 找到 1 条 / 新浪 vCB_AllBulletin 找到 1 条
  - 6-26 通富微电: 巨潮 searchkey 找到 2 条 / 新浪 vCB_AllBulletin 找到 ?

调度: 1 小时一次 (错开 cninfo_stream 的 15min 节奏)
  - 拉 9 只 A 股持仓 ticker 历史公告 (默认查 30 条每只)
  - 按 [date, date] 单日匹配 → 跨源去重 (跟 cninfo + 东财 共用 is_pushed_duplicate)
  - source='sina_announcement' 区分

可靠性:
  - 单 ticker HTTP GET + GBK 解码 (新浪是 GBK)
  - Referer 必填 (Referer: https://finance.sina.com.cn)
  - 持仓 9 只 ticker 串行 ~5s (无反爬, 简单 GET)
  - 失败写 data_source_failures 表, watchdog 检测
  - HTML 解析失败时 record_source_failure 不重试 (结构稳定, 失败 = 真问题)
"""
import re
import time
import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import requests

from config import PORTFOLIO
from models import get_db, record_source_failure
from announcement_stream import save_event, match_holdings
from feishu_push import send_announcement_alert, send_keyword_news_alert


# 新浪 vCB_AllBulletin 端点
SINA_VCB_URL = 'https://vip.stock.finance.sina.com.cn/corp/go.php/vCB_AllBulletin/stockid/{code}.phtml'
SINA_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'https://finance.sina.com.cn/',
    'Accept-Language': 'zh-CN,zh;q=0.9',
}


# 公告详情页 URL 前缀 (公告 id 拼到这里)
SINA_DETAIL_URL_PREFIX = 'https://vip.stock.finance.sina.com.cn/corp/view/vCB_AllBulletinDetail.php?stockid={code}&id={aid}'


# 解析单 ticker 的公告 HTML
# 模式: 2026-06-26&nbsp;<a target='_blank' href='/corp/view/vCB_AllBulletinDetail.php?stockid=600036&id=12412088'>招商银行：北京市君合(深圳)律师事务所...</a>
ANNOUNCEMENT_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2})&nbsp;<a[^>]+href=[\"\\']([^\"\\']+)[\"\\'][^>]*>([^<]+)</a>",
    re.UNICODE,
)


def fetch_sina_vcb_announcements(code: str, name: str,
                                    hard_timeout: int = 15) -> List[Dict]:
    """
    拉单 ticker 的新浪 vCB_AllBulletin 公告 HTML, 解析出 [{date, title, url}, ...].
    返回按日期倒序 (HTML 自然顺序) 的 ~30 条历史公告.

    6-26 招行实测: 30 条 / 53261 bytes / 1.5s
    """
    url = SINA_VCB_URL.format(code=code)
    try:
        r = requests.get(url, timeout=hard_timeout, headers=SINA_HEADERS)
        r.encoding = 'gbk'  # 关键: 新浪 GBK
        r.raise_for_status()
        html = r.text
    except Exception as e:
        record_source_failure(
            source_name=f'sina_vcb_{code}',
            error=f'{type(e).__name__}: {str(e)[:200]}'
        )
        return []

    results = []
    for m in ANNOUNCEMENT_RE.finditer(html):
        date_str, href, title = m.group(1), m.group(2), m.group(3)
        title = re.sub(r'<[^>]+>', '', title).strip()  # 二次清理残留 HTML

        # 拼完整 URL (href 是相对路径)
        if href.startswith('http'):
            full_url = href
        elif href.startswith('/'):
            full_url = 'https://vip.stock.finance.sina.com.cn' + href
        else:
            full_url = 'https://vip.stock.finance.sina.com.cn/' + href

        # 提取 announcement id 拼 source_id (稳定主键)
        aid = ''
        m_id = re.search(r'[?&]id=(\d+)', href)
        if m_id:
            aid = m_id.group(1)

        results.append({
            'code': code,
            'name': name,
            'title': title,
            'url': full_url,
            'pub_date': date_str,
            'pub_date_precision': 'day',
            'source_id': f'sina_{code}_{aid}' if aid else f'sina_{code}_{date_str}_{title[:20]}',
        })
    return results


def fetch_sina_holdings_announcements(date_str: Optional[str] = None,
                                          hard_timeout: int = 60) -> List[Dict]:
    """
    拉所有 A 股持仓 ticker 的新浪 vCB_AllBulletin 公告, 过滤到指定日期当天.
    date_str: 'YYYY-MM-DD' 格式; 默认今天
    """
    if not date_str:
        date_str = datetime.now().strftime('%Y-%m-%d')

    # 持仓 9 只 A 股
    holdings_cn = []
    for sym, cfg in PORTFOLIO.items():
        if cfg.get('market') != 'CN':
            continue
        code = cfg.get('akshare_symbol', sym)
        name = cfg.get('name', '')
        if not (code and name):
            continue
        holdings_cn.append((code, name))

    all_results = []
    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutTimeout, as_completed

    def _do():
        with ThreadPoolExecutor(max_workers=4) as ex:
            futs = {ex.submit(fetch_sina_vcb_announcements, code, name): (code, name)
                    for code, name in holdings_cn}
            for fut in as_completed(futs):
                code, name = futs[fut]
                try:
                    items = fut.result(timeout=12)
                    # 客户端过滤: 只保留 date_str 当天
                    day_items = [it for it in items if it['pub_date'] == date_str]
                    all_results.extend(day_items)
                    if day_items:
                        print(f"    [sina-vcb] {code} {name} {date_str}: {len(day_items)} 条 (全量 {len(items)})")
                except Exception as e:
                    print(f"    [sina-vcb] {code} {name} 失败: {type(e).__name__}: {e}")
        return all_results

    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_do)
        try:
            return fut.result(timeout=hard_timeout)
        except FutTimeout:
            print(f"    [sina-vcb] 硬超时 {hard_timeout}s")
            return all_results


# ── 主流程 ───────────────────────────────────────────────────

def run_sina_announcement_stream(date_str: Optional[str] = None,
                                   dry_run: bool = False,
                                   push: bool = True) -> Dict:
    """
    一轮新浪公告流 (A 股备源): 拉 9 只持仓 ticker 当日公告 → 入 events → 持仓命中推送
    与 cninfo_stream (主) + announcement_stream (东财) 并行

    2026-06-25 v17: 入库前跟巨潮跨源去重 (按 title 前 30 + date + symbol),
    避免新浪 6 条持仓公告跟巨潮 5 条重复入库.
    """
    started = datetime.now()
    if not date_str:
        date_str = started.strftime('%Y-%m-%d')

    print(f"\n{'='*55}")
    print(f"  新浪公告流 (备源) — {started.strftime('%Y-%m-%d %H:%M')}  date={date_str}")
    print(f"{'='*55}")

    items = fetch_sina_holdings_announcements(date_str=date_str)
    print(f"  [sina] 抓取 {len(items)} 条")

    # 2026-06-25 v17: 跨源去重 — 新浪抓的每条先查巨潮/东财是否已有 (title 前 30 + date + symbol)
    # 跳过已被任一 cninfo / eastmoney 源入库的条目,避免 events 表跨源重复
    from announcement_stream import get_db as _get_db
    def _already_in_other_source(title: str, pub_date: str, symbol: str) -> bool:
        conn = _get_db()
        c = conn.cursor()
        c.execute('''
            SELECT 1 FROM events
            WHERE pub_date = ? AND symbol = ? AND substr(title,1,30) = substr(?,1,30)
            AND source IN ('cn_announcement_cninfo', 'cn_announcement_cninfo_ticker', 'cn_announcement')
            LIMIT 1
        ''', (pub_date, symbol, title))
        row = c.fetchone()
        conn.close()
        return bool(row)

    new_in_db = 0
    pushed = 0
    skipped = 0
    cross_source_skipped = 0
    for ev in items:
        # 跟巨潮一致: 入库前过 match_holdings
        hits = match_holdings(ev)
        if not hits:
            skipped += 1
            continue

        hit_syms = ','.join(s for s, _ in hits)

        # 跨源去重: 巨潮/东财已收过则跳过入库 (但 match_holdings 已确认持仓命中, 仍可推)
        # 注意: 跨源去重只在入库侧, 推送走 is_pushed_duplicate 单独判断
        if _already_in_other_source(ev.get('title',''), ev.get('pub_date',''), hit_syms):
            cross_source_skipped += 1
            # 不入库, 但考虑是否推送 (依赖 is_pushed_duplicate 30 分钟窗口)
            if not push:
                continue
            for sym, cfg in hits:
                name = cfg.get('name', sym)
                monitor_type = cfg.get('monitor_type', '')
                keywords = cfg.get('news_keywords', [])
                from announcement_stream import is_pushed_duplicate
                if is_pushed_duplicate(sym, ev.get('title', ''), ev.get('pub_date', '')):
                    continue
                if dry_run:
                    print(f"    [DRY-RUN] 跨源去重但仍可推 → {name}({sym}): {ev.get('title','')[:50]}")
                else:
                    if monitor_type == 'EVENT_DRIVEN':
                        send_keyword_news_alert(name, sym, ev.get('title',''), ev.get('url',''), keywords)
                    else:
                        send_announcement_alert(name, sym, ev.get('title',''), ev.get('url',''))
            continue

        is_new = save_event(
            source='sina_announcement',
            source_id=ev.get('source_id', ''),
            symbol=hit_syms,
            title=ev.get('title', ''),
            url=ev.get('url', ''),
            pub_date=ev.get('pub_date', ''),
            pub_date_precision='day',
        )
        if is_new:
            new_in_db += 1

        if not is_new or not push:
            continue

        hits = match_holdings(ev)
        for sym, cfg in hits:
            name = cfg.get('name', sym)
            monitor_type = cfg.get('monitor_type', '')
            keywords = cfg.get('news_keywords', [])
            if dry_run:
                print(f"    [DRY-RUN] 推 → {name}({sym}): {ev.get('title','')[:50]}")
            else:
                from announcement_stream import is_pushed_duplicate
                if is_pushed_duplicate(sym, ev.get('title', ''), ev.get('pub_date', '')):
                    print(f"    [跨源去重] 跳过 (30min 内 {sym} 已被任一 source 推过): {ev.get('title','')[:50]}")
                    continue
                if monitor_type == 'EVENT_DRIVEN':
                    send_keyword_news_alert(name, sym, ev.get('title',''), ev.get('url',''), keywords)
                else:
                    send_announcement_alert(name, sym, ev.get('title',''), ev.get('url',''))
                from announcement_stream import mark_pushed
                mark_pushed('sina_announcement', ev.get('source_id', ''), ev.get('pub_date', ''))
            pushed += 1

    stats = {
        'job_name': 'sina_announcement_stream',
        'status': 'ok' if items is not None else 'failed',
        'symbols_processed': new_in_db,
        'symbols_failed': 0,
        'last_error': None,
        'started_at': started.strftime('%Y-%m-%d %H:%M:%S'),
        'fetched': len(items),
        'new_in_db': new_in_db,
        'pushed': pushed,
        'skipped_non_portfolio': skipped,
        'cross_source_skipped': cross_source_skipped,  # 2026-06-25 v17: 新增
    }
    print(f"  [sina] 新入库 {new_in_db}, 推送 {pushed}, 跳过(非持仓) {skipped}, 跨源去重 {cross_source_skipped}")
    return stats


if __name__ == '__main__':
    s = run_sina_announcement_stream(dry_run=True)
    print('\n=== stats ===')
    for k, v in s.items():
        print(f'  {k}: {v}')
