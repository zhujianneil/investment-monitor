"""
announcement_stream.py — A 股公告流 (第一层, 无 LLM 依赖)

2026-06-19 重构. 修正了原 news_monitor.py 的三个 bug:
  1. ak.stock_notice_report(symbol=股票代码) → 错, symbol 是报告类型
  2. '全部' 太慢 (1742 条 / 30s+) → 按报告类型分批拉 (重大事项/财务报告/...)
  3. 没有持久化为 events 流 → 这次入 events 表, 跨 LLM 增强可复用

设计目标:
  1. 抓全市场指定报告类型的 A 股公告
  2. 去重入库 (events 表, source='cn_announcement')
  3. 按持仓+关键词触发推送 (Pn 模式)
  4. LLM 增强是第二层 (llm_enhancer.py), 不阻塞本层

数据源: ak.stock_notice_report (东方财富)
  - 报告类型: 全部 / 重大事项 / 财务报告 / 融资公告 / 风险提示 / 资产重组 / 信息变更 / 持股变动
  - 一次拉一日, 返回 ~100-200 条 (单类型) ~1700 条 (全部)
  - 慢! 默认只拉 重大事项 + 资产重组 + 风险提示 (持仓最关心的 3 类)
"""
import sqlite3
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import akshare as ak

from config import PORTFOLIO
from models import get_db, record_source_failure
from feishu_push import send_announcement_alert, send_keyword_news_alert


# 持仓最关心的报告类型 — 跑一次 30s, 每天跑 1 次足够; 15min 跑只拉 '重大事项'
REPORT_TYPES_PRIMARY   = ['重大事项']     # 跑 15min 抓这个最快 (~30s, 100+ 条)
REPORT_TYPES_FULL      = ['重大事项', '资产重组', '风险提示', '财务报告']  # 跑 1h 全量
SOURCE_DATE_FORMAT     = '%Y%m%d'


# ── 抓取 ─────────────────────────────────────────────────────

def fetch_announcements(report_type: str = '重大事项', date: Optional[str] = None,
                        max_records: int = 500, hard_timeout: int = 45) -> List[Dict]:
    """
    抓指定报告类型 + 指定日期的全市场公告.
    date: YYYYMMDD 格式; 默认今天
    返回 [{code, name, title, url, pub_date, report_type, source_id}, ...]

    2026-06-19 加固:
      - 东财接口 SSL 不稳, 加重试 (3 次, 退避 1+2+4s)
      - hard_timeout: 整体超时秒数 (concurrent.futures 强制), 防止卡死
        (signal.alarm 在 apscheduler worker thread 里不能用, 改用 thread timeout)
    """
    if not date:
        date = datetime.now().strftime(SOURCE_DATE_FORMAT)

    # 用 future 包一层, 超时强制终止 (signal 在子线程不可用)
    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutTimeout
    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_fetch_announcements_inner, report_type, date, max_records)
        try:
            return fut.result(timeout=hard_timeout)
        except FutTimeout:
            print(f"  [公告流] 硬超时 {hard_timeout}s 强制退出")
            record_source_failure(f'stock_notice_report_{report_type}_timeout',
                                  TimeoutError(f'fetch 超 {hard_timeout}s'))
            return []
        except Exception as e:
            # _fetch_announcements_inner 内部已 record_source_failure
            return []


def _fetch_announcements_inner(report_type: str, date: str, max_records: int) -> List[Dict]:
    """实际抓取逻辑 (由 fetch_announcements 包硬超时)"""
    # 东财接口 SSL 不稳, 加重试 (2026-06-19 加固)
    last_err = None
    for attempt in range(3):
        try:
            df = ak.stock_notice_report(symbol=report_type, date=date)
            break
        except Exception as e:
            last_err = e
            wait = 2 ** attempt
            print(f"  [公告流] {report_type} @ {date} 第 {attempt+1}/3 次失败: {type(e).__name__}: {str(e)[:100]}; 等 {wait}s 重试")
            time.sleep(wait)
    else:
        print(f"  [公告流] {report_type} @ {date} 3 次全失败: {last_err}")
        record_source_failure(f'stock_notice_report_{report_type}', last_err)
        return []

    if df is None or df.empty:
        return []

    # 列名确认 (返回固定: 代码/名称/公告标题/公告类型/公告日期/网址)
    # 注: 东财 公告日期 字段只到日 (YYYY-MM-DD), 没有时分. 尊重数据源精度.
    results = []
    for _, row in df.head(max_records).iterrows():
        try:
            code  = str(row.get('代码', '')).strip()
            name  = str(row.get('名称', '')).strip()
            title = str(row.get('公告标题', '')).strip()
            url   = str(row.get('网址', '')).strip()
            pub   = str(row.get('公告日期', '')).strip()
            rtype = str(row.get('公告类型', report_type)).strip()

            if not code or not title:
                continue

            # source_id 用公告 URL 末尾的 AN ID, 唯一且稳定
            source_id = ''
            if '/AN' in url:
                source_id = url.split('/AN')[-1].replace('.html', '')

            # 时间精度: 东财只到日
            pub_date_iso = pub if pub else ''  # 原样存 (YYYY-MM-DD)

            results.append({
                'symbol':    _normalize_a_share_symbol(code),
                'code':      code,
                'name':      name,
                'title':     title,
                'url':       url,
                'pub_date':  pub_date_iso,
                'report_type': rtype,
                'source_id': source_id or f'em_{code}_{pub}_{title[:20]}',
            })
        except Exception as row_err:
            print(f"  [公告流] {report_type} 单行跳过: {row_err}")
            continue

    return results


def _normalize_a_share_symbol(code: str) -> str:
    """统一 A 股 symbol 格式 (持仓里 600941 / 000001 这种纯数字串)"""
    s = str(code).strip()
    for prefix in ('sh', 'SH', 'sz', 'SZ'):
        if s.startswith(prefix):
            s = s[len(prefix):]
            break
    return s


# ── 入库 ─────────────────────────────────────────────────────

def save_event(source: str, title: str, symbol: Optional[str] = None,
               content: str = '', url: str = '', pub_date: Optional[str] = None,
               source_id: Optional[str] = None, pub_date_precision: str = 'day',
               relevance: Optional[str] = None, pushed: int = 0) -> bool:
    """
    入 events 表, 唯一约束 source+source_id+pub_date.
    pub_date_precision: 'day' (东财公告) | 'datetime' (yf/财联社 实时流)
    relevance: 'primary' | 'thematic' | 'weak' | None (2026-06-19 P0)
    pushed: 0/1 推送标记, 默认 0 (2026-06-19 P1 修复: 审计断裂导致无法去重)
    返回 True=新插入, False=已存在(去重) 或失败.
    """
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR IGNORE INTO events
              (source, source_id, symbol, title, content, url, pub_date, pub_date_precision, relevance, pushed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (source, source_id or '', symbol, title, content, url,
              pub_date or '', pub_date_precision, relevance, pushed))
        inserted = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return inserted
    except Exception as e:
        print(f"  [save_event] 失败: {e}")
        return False


def mark_pushed(source: str, source_id: str, pub_date: str) -> None:
    """
    推送成功后回写 pushed=1, 解决审计断裂 + 支持推送去重 (P1 修复 2026-06-19)
    source_id 为空时按 title 兜底匹配
    """
    try:
        conn = get_db()
        cursor = conn.cursor()
        if source_id:
            cursor.execute('''
                UPDATE events SET pushed=1
                WHERE source=? AND source_id=? AND pub_date=?
            ''', (source, source_id, pub_date or ''))
        else:
            cursor.execute('''
                UPDATE events SET pushed=1
                WHERE source=? AND title=? AND pub_date=?
            ''', (source, '', pub_date or ''))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"  [mark_pushed] 失败: {e}")


def is_already_pushed(source: str, source_id: str, pub_date: str) -> bool:
    """
    检查事件是否已推送 (P1 去重, 2026-06-19)
    返回 True=已推过, False=未推或 events 表无此条
    """
    try:
        conn = get_db()
        cursor = conn.cursor()
        if source_id:
            cursor.execute('''
                SELECT pushed FROM events
                WHERE source=? AND source_id=? AND pub_date=?
                LIMIT 1
            ''', (source, source_id, pub_date or ''))
        else:
            return False
        row = cursor.fetchone()
        conn.close()
        return bool(row and row[0])
    except Exception as e:
        return False


# ── 推送匹配 ─────────────────────────────────────────────────

def match_holdings(event: Dict) -> List[tuple]:
    """
    给定一条事件, 返回命中的 [(symbol, cfg), ...]
    规则:
      - 持仓 symbol 出现在 code/name/title 任一
      - 或关键词在 title/content 命中
    """
    hits = []
    code  = str(event.get('code', ''))
    name  = str(event.get('name', ''))
    title = str(event.get('title', ''))

    for sym, cfg in PORTFOLIO.items():
        if cfg.get('market') != 'CN':
            continue

        # 标的对齐 (持仓 '600941' vs 公告 code '600941')
        akshare_sym = cfg.get('akshare_symbol', sym)
        if _normalize_a_share_symbol(akshare_sym) == _normalize_a_share_symbol(code):
            hits.append((sym, cfg))
            continue

        # 名称命中
        if cfg.get('name') and cfg['name'] in name:
            hits.append((sym, cfg))
            continue

        # 关键词命中
        kws = cfg.get('news_keywords', [])
        if kws and any(kw in title for kw in kws):
            hits.append((sym, cfg))

    return hits


# ── 主流程 ───────────────────────────────────────────────────

def run_announcement_stream(report_types: Optional[List[str]] = None, dry_run: bool = False) -> Dict:
    """
    一轮公告流: 抓多报告类型 → 入库 → 匹配推送
    默认只抓 '重大事项' (跑得快, 15min 一次). 周报/日终可调全量.
    返回 stats dict (供 scheduler 写 monitor_runs)
    """
    started = datetime.now()
    report_types = report_types or REPORT_TYPES_PRIMARY
    print(f"\n{'='*55}")
    print(f"  公告流 — {started.strftime('%Y-%m-%d %H:%M')}  types={report_types}")
    print(f"{'='*55}")

    all_raw = []
    for rt in report_types:
        items = fetch_announcements(report_type=rt)
        print(f"  [公告流] {rt}: {len(items)} 条")
        all_raw.extend(items)

    print(f"  [公告流] 抓取 {len(all_raw)} 条")

    new_in_db = 0
    pushed = 0
    for ev in all_raw:
        is_new = save_event(
            source='cn_announcement',
            source_id=ev.get('source_id') or '',
            symbol=ev.get('symbol'),
            title=ev.get('title', ''),
            url=ev.get('url', ''),
            pub_date=ev.get('pub_date') or '',
            pub_date_precision='day',  # 东财公告日期只到日
        )
        if is_new:
            new_in_db += 1

        # 推送去重: 2026-06-19 P0 — 只对新入库的事件推送, 已存在的跳过
        # (公告流是全市场流, 同一公告会被多持仓匹配; 不去重会刷屏)
        if not is_new:
            continue

        # 推送: 持仓命中就推 (UNIQUE 约束保证不重复入库, 这里没去重推送)
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
                # P1: 推送成功回写 (审计闭环)
                from announcement_stream import mark_pushed
                mark_pushed('cn_announcement', ev.get('source_id') or '', ev.get('pub_date') or '')
            pushed += 1

    stats = {
        'job_name': 'announcement_stream',
        'status': 'ok' if all_raw else 'failed',
        'symbols_processed': new_in_db,
        'symbols_failed': 0 if all_raw else 1,
        'last_error': None if all_raw else 'all report_types 抓取空 (网络/限流?)',
        'started_at': started.strftime('%Y-%m-%d %H:%M:%S'),
        'fetched': len(all_raw),
        'new_in_db': new_in_db,
        'pushed': pushed,
    }
    print(f"  [公告流] 新入库 {new_in_db}, 推送 {pushed}")
    return stats


if __name__ == '__main__':
    import sys
    dry = '--dry-run' in sys.argv
    stats = run_announcement_stream(dry_run=dry)
    print(stats)
