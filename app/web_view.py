"""
web_view.py — 投资监控新闻网页回看
- 只读 investment.db,不破坏监控
- 卡片化布局 (类似飞书卡片视觉)
- 筛选: 时间 / source / symbol / pushed-only / 关键词
- 后台 LLM 增强 worker: 补全 6874 条空摘要 (懒启动, queue 节流)
"""
import os
import sys
import json
import sqlite3
import threading
import time
from datetime import datetime, timedelta

from flask import Flask, request, render_template, jsonify, abort

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import DB_PATH, PORTFOLIO  # noqa: E402

app = Flask(__name__)

# ──────────────────── 工具函数 ────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# 来源颜色码 (类似飞书卡片)
SOURCE_META = {
    'cls_telegraph':         {'label': '财联社电报', 'color': '#ff6b35', 'icon': '⚡'},
    'sina_global':           {'label': '新浪全球',   'color': '#1e88e5', 'icon': '🌐'},
    'cn_announcement_cninfo':{'label': '巨潮公告',   'color': '#43a047', 'icon': '📋'},
    'cn_announcement':       {'label': '东财公告',   'color': '#26a69a', 'icon': '📋'},
    'yf_news':               {'label': 'Yahoo财经',  'color': '#8e24aa', 'icon': '🇺🇸'},
}


def symbol_display(sym: str) -> str:
    """持仓 symbol → 显示名 (e.g. 'NVDA' → 'NVDA · 英伟达')"""
    if not sym:
        return ''
    # 美股裸代码 (NVDA / V / BRK-B)
    if sym in PORTFOLIO:
        return f"{sym} · {PORTFOLIO[sym]['name']}"
    # 港股 0700.HK / 09992.HK / 9988.HK 等
    base = sym.replace('.HK', '')
    if base in PORTFOLIO:
        return f"{base} · {PORTFOLIO[base]['name']}"
    if sym.replace('.HK', '') in PORTFOLIO:
        return f"{sym.replace('.HK','')} · {PORTFOLIO[sym.replace('.HK','')]['name']}"
    return sym


# ──────────────────── 路由 ────────────────────

@app.route('/')
def index():
    return render_template('news.html',
                           sources=list(SOURCE_META.keys()),
                           source_meta=SOURCE_META)


@app.route('/api/events')
def api_events():
    """
    列表接口
    参数: page / page_size / source / symbol / pushed_only / q / days
    """
    page = max(int(request.args.get('page', 1)), 1)
    page_size = min(int(request.args.get('page_size', 30)), 200)
    offset = (page - 1) * page_size

    where = []
    params = []

    # source 多选 (逗号分隔)
    src = request.args.get('source', '').strip()
    if src:
        sources = [s.strip() for s in src.split(',') if s.strip()]
        placeholders = ','.join('?' * len(sources))
        where.append(f'source IN ({placeholders})')
        params.extend(sources)

    # symbol 过滤
    sym = request.args.get('symbol', '').strip()
    if sym:
        where.append('symbol = ?')
        params.append(sym)

    # pushed-only
    if request.args.get('pushed_only') in ('1', 'true', 'yes'):
        where.append('pushed = 1')

    # 时间窗口
    days = int(request.args.get('days', 7))
    where.append('fetched_at > datetime(?, ?)')
    params.extend(['now', f'-{days} day'])

    # 关键词
    q = request.args.get('q', '').strip()
    if q:
        where.append('(title LIKE ? OR content LIKE ?)')
        params.extend([f'%{q}%', f'%{q}%'])

    where_sql = (' WHERE ' + ' AND '.join(where)) if where else ''

    conn = get_db()
    cur = conn.cursor()

    total = cur.execute(f'SELECT COUNT(*) FROM events{where_sql}', params).fetchone()[0]

    rows = cur.execute(
        f'''SELECT id, source, source_id, symbol, title, content, url,
                   pub_date, fetched_at, llm_summary, llm_sentiment,
                   llm_themes, llm_severity, pushed, relevance
            FROM events{where_sql}
            ORDER BY fetched_at DESC
            LIMIT ? OFFSET ?''',
        params + [page_size, offset]
    ).fetchall()
    conn.close()

    items = []
    for r in rows:
        d = dict(r)
        # 解析 themes JSON
        if d.get('llm_themes'):
            try:
                d['llm_themes'] = json.loads(d['llm_themes'])
            except Exception:
                d['llm_themes'] = []
        else:
            d['llm_themes'] = []
        # 时间格式化
        if d['fetched_at']:
            try:
                d['fetched_at_str'] = datetime.fromisoformat(d['fetched_at']).strftime('%m-%d %H:%M')
            except Exception:
                d['fetched_at_str'] = d['fetched_at'][:16]
        else:
            d['fetched_at_str'] = ''
        if d['pub_date']:
            try:
                d['pub_date_str'] = datetime.fromisoformat(d['pub_date']).strftime('%m-%d %H:%M')
            except Exception:
                d['pub_date_str'] = str(d['pub_date'])[:16]
        else:
            d['pub_date_str'] = ''
        d['symbol_display'] = symbol_display(d.get('symbol') or '')
        d['source_meta'] = SOURCE_META.get(d['source'], {'label': d['source'], 'color': '#888', 'icon': '•'})
        items.append(d)

    return jsonify({
        'items': items,
        'total': total,
        'page': page,
        'page_size': page_size,
        'pages': (total + page_size - 1) // page_size,
    })


@app.route('/api/filters')
def api_filters():
    """当前可选 source / symbol"""
    conn = get_db()
    cur = conn.cursor()
    sources = [r[0] for r in cur.execute(
        'SELECT source FROM events GROUP BY source ORDER BY COUNT(*) DESC'
    )]
    symbols = [r[0] for r in cur.execute(
        '''SELECT symbol FROM events
           WHERE symbol IS NOT NULL AND symbol != ''
           GROUP BY symbol ORDER BY COUNT(*) DESC'''
    )]
    conn.close()
    return jsonify({'sources': sources, 'symbols': symbols})


@app.route('/api/stats')
def api_stats():
    """首页顶部统计卡片"""
    conn = get_db()
    cur = conn.cursor()
    today_start = datetime.now().strftime('%Y-%m-%d 00:00:00')

    pushed_today = cur.execute(
        'SELECT COUNT(*) FROM events WHERE pushed=1 AND fetched_at >= ?',
        [today_start]
    ).fetchone()[0]

    pushed_total = cur.execute('SELECT COUNT(*) FROM events WHERE pushed=1').fetchone()[0]

    total = cur.execute('SELECT COUNT(*) FROM events').fetchone()[0]

    by_source = {r[0]: r[1] for r in cur.execute(
        'SELECT source, COUNT(*) FROM events GROUP BY source'
    )}

    enhanced = cur.execute('SELECT COUNT(*) FROM events WHERE llm_summary IS NOT NULL').fetchone()[0]

    conn.close()
    return jsonify({
        'pushed_today': pushed_today,
        'pushed_total': pushed_total,
        'total': total,
        'by_source': by_source,
        'enhanced': enhanced,
    })


# ──────────────────── 后台 LLM 增强 ────────────────────

_enhancer_thread_started = False
_enhancer_lock = threading.Lock()


@app.route('/api/enhance', methods=['POST'])
def api_enhance():
    """手动触发 LLM 补全,limit 默认 50,最大 200"""
    limit = min(int(request.json.get('limit', 50) if request.json else 50), 200)
    started = start_enhancer(limit=limit)
    return jsonify({'ok': True, 'started': started, 'limit': limit})


def start_enhancer(limit: int = 50):
    """启动一个后台线程跑 N 条 LLM 增强,避免阻塞主请求"""
    global _enhancer_thread_started
    with _enhancer_lock:
        if _enhancer_thread_started:
            return False
        t = threading.Thread(target=_enhance_worker, args=(limit,), daemon=True)
        t.start()
        _enhancer_thread_started = True
    return True


def _enhance_worker(limit: int):
    """调 llm_enhancer.enhance_pending_events(limit=limit)"""
    global _enhancer_thread_started
    try:
        try:
            from llm_enhancer import enhance_pending_events, is_available
            if not is_available():
                print('[web_view] LLM not configured, skip enhance', flush=True)
                return
            print(f'[web_view] enhance start, batch_size={limit}', flush=True)
            n = enhance_pending_events(batch_size=limit)
            print(f'[web_view] enhance done, n={n}', flush=True)
        except Exception as e:
            print(f'[web_view] enhance error: {e}', flush=True)
    finally:
        with _enhancer_lock:
            _enhancer_thread_started = False


@app.route('/healthz')
def healthz():
    return jsonify({'ok': True, 'ts': int(time.time())})


# ──────────────────── main ────────────────────

if __name__ == '__main__':
    port = int(os.getenv('WEB_PORT', 8090))
    print(f'[web_view] listening on 0.0.0.0:{port}', flush=True)
    # debug=False, 容器里跑不开 debugger
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
