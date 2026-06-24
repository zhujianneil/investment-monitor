import sqlite3
import os
from datetime import datetime
from config import DB_PATH

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS stocks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT UNIQUE NOT NULL,
        name TEXT NOT NULL,
        market TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS prices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL,
        price REAL NOT NULL,
        change_pct REAL,
        volume INTEGER,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS announcements (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL,
        title TEXT NOT NULL,
        content TEXT,
        url TEXT,
        pub_date DATE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(symbol, title, pub_date)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS reports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL,
        year INTEGER NOT NULL,
        file_path TEXT,
        download_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(symbol, year)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL,
        alert_type TEXT NOT NULL,
        message TEXT,
        triggered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS system_status (
        key TEXT PRIMARY KEY,
        value TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # 数据源失败记录表（2026-06-09 新增，配合 data_sources.py 多源 fallback）
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS data_source_failures (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_name TEXT NOT NULL,
        error_type TEXT,
        error_message TEXT,
        occurred_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # 监控运行健康表（2026-06-11 L3 防御新增）
    # 记录每轮监控的成败、处理的 symbol 数、关键异常摘要
    # 供 watchdog 任务判断"是否连续多轮无成功"
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS monitor_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_name TEXT NOT NULL,
        status TEXT NOT NULL,           -- 'ok' | 'partial' | 'failed'
        symbols_processed INTEGER DEFAULT 0,
        symbols_failed INTEGER DEFAULT 0,
        last_error TEXT,
        started_at TIMESTAMP,
        finished_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # ── 事件流表（2026-06-19 新增）────────────────────────────
    # 实时流，区别于 announcements 的"历史镜像"。
    # events 存 cn_announcement / cls_telegraph / yf_news / google_news 四类源。
    # symbol 可空（宏观/板块新闻）。
    # UNIQUE 约束基于 source+source_id（外部 ID）或 source+title+pub_date（无外部 ID 时）
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT NOT NULL,            -- 'cn_announcement' | 'cls_telegraph' | 'yf_news' | 'google_news'
        source_id TEXT,                   -- 外部源 ID（去重锚点）
        symbol TEXT,                      -- 关联标的；空 = 宏观/板块
        title TEXT NOT NULL,
        content TEXT,                     -- 公告正文 / 新闻摘要
        url TEXT,
        pub_date TIMESTAMP,               -- 发布时间 (精度见 pub_date_precision)
        pub_date_precision TEXT DEFAULT 'day',  -- 'day' | 'datetime' (2026-06-19 P0)
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        relevance TEXT DEFAULT NULL,      -- 'primary' | 'thematic' | 'weak' | NULL (2026-06-19 P0)
        llm_summary TEXT,                 -- LLM 一句话解读
        llm_sentiment REAL,               -- -1 ~ 1
        llm_themes TEXT,                  -- JSON array string
        llm_severity TEXT,                -- 'high' | 'medium' | 'low'
        llm_cached_at TIMESTAMP,          -- 增强完成时间
        pushed INTEGER DEFAULT 0,         -- 是否已推飞书
        UNIQUE(source, source_id, pub_date)
    )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_events_symbol ON events(symbol)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_events_pub ON events(pub_date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_events_pushed ON events(pushed, llm_severity)')

    # ── LLM 增强缓存表（2026-06-19 新增）─────────────────────
    # 按 title hash 缓存增强结果，同一标题只调一次 LLM。
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS llm_cache (
        title_hash TEXT PRIMARY KEY,      -- sha1(title)[:16]
        title TEXT NOT NULL,
        summary TEXT,
        sentiment REAL,
        themes TEXT,                      -- JSON
        severity TEXT,
        cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # ── 持仓 thesis 归档表（2026-06-24 新增）──────────────────
    # 把 events 归档到某持仓的某条假设(支柱)下,并判 支持/削弱/中性。
    # 由 thesis_tracker.py(scheduler 进程)写入,web_view 只读。
    # 一条 event 只保留一条 link(取最相关假设)→ UNIQUE(event_id)。
    # assumption_id='__none__' = 已被 LLM 处理但与任何假设无关(防重复处理)。
    # thesis_version = 该 symbol 假设集的哈希;假设改动后版本变 → 自动重判。
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS thesis_links (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        assumption_id TEXT NOT NULL,      -- theses.py 里的 assumption id 或 '__none__'
        stance TEXT,                      -- 'support' | 'weaken' | 'neutral'
        confidence REAL,                  -- 0 ~ 1
        rationale TEXT,                   -- 一句话理由
        method TEXT DEFAULT 'llm',        -- 'llm' | 'manual'
        thesis_version TEXT,              -- 该 symbol 假设集哈希(版本)
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(event_id)
    )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_thesis_symbol ON thesis_links(symbol)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_thesis_assumption ON thesis_links(symbol, assumption_id)')

    conn.commit()
    conn.close()
    print("数据库初始化完成")


# ── 监控运行健康（2026-06-11 新增）───────────────────────────

def record_monitor_run(job_name, status, symbols_processed=0, symbols_failed=0, last_error=None, started_at=None):
    """记录一轮监控的运行结果。status ∈ {'ok','partial','failed'}"""
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO monitor_runs
              (job_name, status, symbols_processed, symbols_failed, last_error, started_at)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (job_name, status, symbols_processed, symbols_failed,
              (last_error or '')[:500] if last_error else None,
              started_at))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"  [record_monitor_run] 失败: {e}")


def get_recent_monitor_runs(job_name, limit=5):
    """取最近 N 条某 job 的运行记录（最新在前）"""
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM monitor_runs
            WHERE job_name = ?
            ORDER BY id DESC LIMIT ?
        ''', (job_name, limit))
        rows = [dict(r) for r in cursor.fetchall()]
        conn.close()
        return rows
    except Exception as e:
        print(f"  [get_recent_monitor_runs] 失败: {e}")
        return []

def save_price(symbol, price, change_pct, volume):
    """
    保存价格快照。2026-06-11 加固：防御 price=None/NaN（避免 IntegrityError 整轮崩），
    volume None/非数值 → 0。
    返回 True=成功，False=跳过（数据无效）。
    """
    import math
    # price 是 NOT NULL，遇到 None/NaN/非有限值直接跳过
    if price is None or (isinstance(price, float) and (math.isnan(price) or math.isinf(price))):
        print(f"  [save_price] 跳过 {symbol}：price 无效 ({price!r})")
        return False
    # change_pct 容错
    if change_pct is not None and isinstance(change_pct, float) and (math.isnan(change_pct) or math.isinf(change_pct)):
        change_pct = 0.0
    # volume 容错
    if volume is None:
        volume = 0
    elif not isinstance(volume, (int, float)):
        try:
            volume = int(volume)
        except (ValueError, TypeError):
            volume = 0
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO prices (symbol, price, change_pct, volume)
            VALUES (?, ?, ?, ?)
        ''', (symbol, float(price), change_pct, volume))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"  [save_price] 写入 {symbol} 失败（不致命）: {type(e).__name__}: {e}")
        return False

def get_last_alert_time(symbol, alert_type, hours=24):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT MAX(triggered_at) as last_time
        FROM alerts
        WHERE symbol = ? AND alert_type = ?
        AND triggered_at > datetime('now', ?)
    ''', (symbol, alert_type, f'-{hours} hours'))
    result = cursor.fetchone()
    conn.close()
    return result['last_time']

def save_alert(symbol, alert_type, message):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO alerts (symbol, alert_type, message)
        VALUES (?, ?, ?)
    ''', (symbol, alert_type, message))
    conn.commit()
    conn.close()

def save_announcement(symbol, title, url, pub_date, content=''):
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR IGNORE INTO announcements (symbol, title, url, pub_date, content)
            VALUES (?, ?, ?, ?, ?)
        ''', (symbol, title, url, pub_date, content))
        conn.commit()
        conn.close()
        return True
    except:
        return False

def update_heartbeat():
    """更新心跳时间，供面板检查监控是否在线"""
    try:
        conn = get_db()
        cursor = conn.cursor()
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute('''
            INSERT OR REPLACE INTO system_status (key, value, updated_at)
            VALUES (?, ?, ?)
        ''', ('last_heartbeat', 'running', now))
        conn.commit()
        conn.close()
    except Exception as e:
        # 2026-06-09 修复：原来静默吞错，导致 system_status 表缺失都不知道
        print(f"[HEARTBEAT ERROR] update_heartbeat 失败: {e}")


def record_source_failure(source_name, error):
    """记录数据源失败（2026-06-09 新增）"""
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO data_source_failures (source_name, error_type, error_message)
            VALUES (?, ?, ?)
        ''', (source_name, type(error).__name__, str(error)[:500]))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[ERROR] record_source_failure 也失败: {e}")


def get_recent_source_failures(hours=24):
    """查询最近 N 小时的数据源失败记录（健康检查任务用）"""
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT source_name, error_type, COUNT(*) as cnt, MAX(occurred_at) as last_at
            FROM data_source_failures
            WHERE occurred_at > datetime('now', ?)
            GROUP BY source_name, error_type
            ORDER BY cnt DESC
        ''', (f'-{hours} hours',))
        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return results
    except Exception as e:
        print(f"[ERROR] get_recent_source_failures 失败: {e}")
        return []
