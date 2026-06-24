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
    # 2026-06-20 优化: KeyError '代码' 是 akshare 上游 bug (stock_notice.py:118
    # 当数据没准备好时 '代码' 列缺失), 重试无用, 立即放弃
    last_err = None
    for attempt in range(3):
        try:
            df = ak.stock_notice_report(symbol=report_type, date=date)
            break
        except KeyError as e:
            # akshare 上游 bug, 重试不会成功
            last_err = e
            print(f"  [公告流] {report_type} @ {date} akshare 上游 KeyError {e} (数据未就绪?), 立即放弃")
            record_source_failure(f'stock_notice_report_{report_type}', e)
            return []
        except Exception as e:
            last_err = e
            wait = 2 ** attempt
            print(f"  [公告流] {report_type} @ {date} 第 {attempt+1}/3 次失败: {type(e).__name__}: {str(e)[:100]}; 等 {wait}s 重试")
            time.sleep(wait)
    else:
        print(f"  [公告流] {report_type} @ {date} 3 次全失败: {last_err}")
        record_source_failure(f'stock_notice_report_{report_type}', last_err)
        return []

    # 列名校验 (2026-06-20 加固): 缺列直接跳过整批, 避免 row.get KeyError
    # 注: 东财 公告日期 字段只到日 (YYYY-MM-DD), 没有时分. 尊重数据源精度.
    if df is None or df.empty:
        return []
    expected_cols = {'代码', '名称', '公告标题', '网址', '公告日期', '公告类型'}
    if not expected_cols.issubset(set(df.columns)):
        missing = expected_cols - set(df.columns)
        print(f"  [公告流] {report_type} 列名不符, 缺 {missing}, 实际列: {list(df.columns)}")
        return []
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
               relevance: Optional[str] = None, pushed: int = 0,
               cross_lid_dedup: bool = False) -> bool:
    """
    入 events 表, 唯一约束 source+source_id+pub_date.
    pub_date_precision: 'day' (东财公告) | 'datetime' (yf/财联社 实时流)
    relevance: 'primary' | 'thematic' | 'weak' | None (2026-06-19 P0)
    pushed: 0/1 推送标记, 默认 0 (2026-06-19 P1 修复: 审计断裂导致无法去重)
    cross_lid_dedup: True 时额外查 (source, title[:30], pub_date) 是否存在 (2026-06-22 P1)
                    解决 cls_telegraph 跨 lid 同一新闻 source_id 不同导致的重复入库
    返回 True=新插入, False=已存在(去重) 或失败.
    """
    try:
        # 2026-06-22 P1: 跨 lid 标题去重 (应用层)
        # 同 source 不同 source_id 但 title+pub_date 相同 → 视为同一新闻
        if cross_lid_dedup and title and pub_date:
            conn = get_db()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT 1 FROM events
                WHERE source=? AND substr(title,1,30)=? AND pub_date=?
                LIMIT 1
            ''', (source, (title or '')[:30], pub_date or ''))
            if cursor.fetchone():
                conn.close()
                return False  # 跨 lid 重复, 跳过
            conn.close()

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


def is_pushed_duplicate(symbol: str, title: str, pub_date: str,
                        within_minutes: int = 30) -> bool:
    """
    跨源去重 (2026-06-20 新增): 检查 (symbol, title, pub_date) 组合
    在最近 within_minutes 分钟内是否已被任一 source 推送.

    目的: 东财 (cn_announcement) 和巨潮 (cn_announcement_cninfo) 并行时,
    同一条公告会两边都入库, 推送会重复.
    按 (symbol + 标题前 50 字 + 发布日) 模糊匹配: 同标的同日同主题 = 同一公告.

    返回 True=最近已推过, False=没推过 (可推).
    """
    try:
        # 标题截前 50 字: 巨潮/东财对同一公告标题可能有微小差异 (如标点/空格)
        title_key = (title or '')[:50].strip()
        if not title_key or not symbol:
            return False
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT 1 FROM events
            WHERE pushed=1
              AND symbol=?
              AND substr(title, 1, 50)=?
              AND pub_date=?
              AND fetched_at > datetime('now', ?)
            LIMIT 1
        ''', (symbol, title_key, pub_date or '', f'-{within_minutes} minutes'))
        row = cursor.fetchone()
        conn.close()
        return bool(row)
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

        # 关键词命中 (2026-06-21 P0 修复: 必须持仓名出现在 title 才推, 否则行业词如「分红」
        # 会把别家公告推到这家. 例: 银河微电「分红」公告被推到 600036 招商银行)
        kws = cfg.get('news_keywords', [])
        if kws and any(kw in title for kw in kws):
            # 强约束: 持仓名必须在 title 里 (而非只匹配行业词)
            holding_name = cfg.get('name', '').strip()
            if holding_name and holding_name in title:
                hits.append((sym, cfg))
            # 否则: 主题相关, 但不直接推 — 由用户决定是否后续接 EVENT_DRIVEN 关键词报警

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
    skipped = 0
    for ev in all_raw:
        # 2026-06-23 P0 修复: 跟 cninfo 一致, 入库前过 match_holdings
        # 全市场抓的公告, 不命中持仓的不入库 (飞书 + 网页都看不到)
        hits = match_holdings(ev)
        if not hits:
            skipped += 1
            continue
        hit_syms = ','.join(s for s, _ in hits)

        is_new = save_event(
            source='cn_announcement',
            source_id=ev.get('source_id') or '',
            symbol=hit_syms,  # 2026-06-23 P0: 写命中持仓的 symbol, 不是公告本身
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
                # 跨源去重 (2026-06-20 P0): 巨潮可能在最近 30 分钟内推过同一条
                if is_pushed_duplicate(sym, ev.get('title', ''), ev.get('pub_date', '')):
                    print(f"    [跨源去重] 跳过 (30min 内 {sym} 已被任一 source 推过): {ev.get('title','')[:50]}")
                    continue
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
        'skipped_non_portfolio': skipped,  # 2026-06-23 P0: 非持仓公告跳过数
    }
    print(f"  [公告流] 新入库 {new_in_db}, 推送 {pushed}, 跳过(非持仓) {skipped}")
    return stats


if __name__ == '__main__':
    import sys
    dry = '--dry-run' in sys.argv
    stats = run_announcement_stream(dry_run=dry)
    print(stats)
