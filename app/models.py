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
