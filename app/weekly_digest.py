"""
weekly_digest.py — 每周摘要推送

哲学：一周只主动看一次信息。这个模块在周末推送过去 7 天的汇总，
而不是让你每天接收零散警报。

内容：
1. 本周触发的 FCF 警报（如有）
2. 本周异常波动（如有）
3. 本周新新闻/公告摘要（按持仓分组）
4. 下周财报预告

如果一周内没有任何异动，推送"系统静默确认"——这本身就是有价值的信息。
"""
import sqlite3
from datetime import datetime, timedelta
from config import PORTFOLIO, DB_PATH
from feishu_push import send_weekly_digest


def get_week_alerts():
    """获取过去 7 天的警报记录"""
    since = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM alerts WHERE triggered_at > ? ORDER BY triggered_at DESC",
            (since,)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


def get_week_announcements():
    """获取过去 7 天的新公告/新闻"""
    since = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM announcements WHERE created_at > ? ORDER BY created_at DESC LIMIT 20",
            (since,)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


def get_upcoming_earnings():
    """获取未来 14 天内的财报提醒"""
    today = datetime.now()
    upcoming = []
    for symbol, cfg in PORTFOLIO.items():
        if cfg['monitor_type'] == 'EXIT_PENDING':
            continue
        for em in cfg.get('earnings_months', []):
            try:
                estimated = datetime(today.year, em, 15)
                if estimated < today:
                    estimated = datetime(today.year + 1, em, 15)
                days = (estimated - today).days
                if 0 <= days <= 14:
                    upcoming.append({
                        'name': cfg['name'],
                        'symbol': symbol,
                        'days': days,
                        'date': estimated.strftime('%Y-%m-%d'),
                    })
            except ValueError:
                continue
    return sorted(upcoming, key=lambda x: x['days'])


def send_weekly_report():
    print(f"\n{'='*55}")
    print(f"  周报生成 — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*55}")

    alerts      = get_week_alerts()
    announcements = get_week_announcements()
    upcoming    = get_upcoming_earnings()

    # 按类型分类警报
    fcf_alerts      = [a for a in alerts if 'FCF' in a.get('alert_type', '')]
    anomaly_alerts  = [a for a in alerts if a.get('alert_type') == 'ANOMALY']

    send_weekly_digest(
        fcf_alerts=fcf_alerts,
        anomaly_alerts=anomaly_alerts,
        announcements=announcements,
        upcoming_earnings=upcoming,
        portfolio=PORTFOLIO,
    )

    print(f"  周报推送完成")
