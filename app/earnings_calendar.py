"""
earnings_calendar.py — 财报日历提醒

哲学：财报日是你唯一应该主动准备的时间点。
- 财报发布前 7 天推送提醒，留出阅读和分析时间。
- 提醒内容包含该持仓的核心监控变量清单，让你带着问题去看财报。
"""
from datetime import datetime, timedelta
from config import PORTFOLIO
from models import get_last_alert_time, save_alert
from feishu_push import send_earnings_reminder


def check_earnings_calendar():
    print(f"\n{'='*55}")
    print(f"  财报日历检查 — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*55}")

    today = datetime.now()
    current_month = today.month
    reminders_sent = 0

    for symbol, cfg in PORTFOLIO.items():
        name         = cfg['name']
        monitor_type = cfg['monitor_type']
        earnings_months = cfg.get('earnings_months', [])

        # EXIT_PENDING：跳过
        if monitor_type == 'EXIT_PENDING':
            continue

        if not earnings_months:
            continue

        for em in earnings_months:
            # 估算财报发布日（通常为当月中下旬，默认取 15 号）
            try:
                year = today.year
                # 如果该月已经过去超过 20 天，看下一年
                estimated_date = datetime(year, em, 15)
                if estimated_date < today - timedelta(days=20):
                    estimated_date = datetime(year + 1, em, 15)
            except ValueError:
                continue

            days_until = (estimated_date - today).days

            # 提前 7 天内提醒（每个财报季只推送一次）
            if 0 <= days_until <= 7:
                alert_key = f"earnings_{em}_{today.year}"
                if not get_last_alert_time(symbol, alert_key, hours=24 * 6):
                    print(f"  📅 {name}({symbol}) 财报预计 {days_until} 天后，推送提醒")
                    send_earnings_reminder(name, symbol, estimated_date, cfg)
                    save_alert(symbol, alert_key,
                               f"财报预提醒 — 预计 {estimated_date.strftime('%Y-%m-%d')} 前后")
                    reminders_sent += 1

    print(f"\n  财报提醒完成 — 发出 {reminders_sent} 条")
    return reminders_sent
