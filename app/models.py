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

    conn.commit()
    conn.close()
    print("数据库初始化完成")

def save_price(symbol, price, change_pct, volume):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO prices (symbol, price, change_pct, volume)
        VALUES (?, ?, ?, ?)
    ''', (symbol, price, change_pct, volume))
    conn.commit()
    conn.close()

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
