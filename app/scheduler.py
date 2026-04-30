"""
scheduler.py — 任务调度

新哲学：频率越高的动作认知含量越低。
调度频率与"你能做的判断的频率"对齐。

  每个交易日（盘中）：只检测异常波动 + FCF 阈值
  每天（早晨）      ：新闻 & 公告扫描 + 财报日历检查
  每周日（下午）    ：周报摘要推送
"""
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

from stock_monitor import monitor_stocks
from news_monitor import monitor_news
from earnings_calendar import check_earnings_calendar
from weekly_digest import send_weekly_report
from models import init_db, update_heartbeat

tz = pytz.timezone('Asia/Shanghai')


def job_market_monitor():
    """交易时段：异常波动 + FCF 阈值检测"""
    print("\n>>> [交易时段] 价格 & FCF 监控")
    monitor_stocks()
    update_heartbeat()


def job_daily_news():
    """每日早晨：新闻公告扫描 + 财报日历"""
    print("\n>>> [每日] 新闻公告扫描")
    monitor_news()
    check_earnings_calendar()
    update_heartbeat()


def job_weekly_digest():
    """每周日：周报推送"""
    print("\n>>> [每周] 周报生成")
    send_weekly_report()
    update_heartbeat()


def start():
    init_db()
    update_heartbeat()

    scheduler = BlockingScheduler(timezone=tz)

    # ── 交易时段价格监控（A/H/US 覆盖）──
    # A股：工作日 9:30–15:00，每 2 小时一次（不是每分钟）
    scheduler.add_job(
        job_market_monitor,
        CronTrigger(day_of_week='mon-fri', hour='9,11,14', minute='35', timezone=tz),
        id='market_monitor',
        name='交易时段监控',
    )
    # 美股收盘（北京时间 05:00）：只捕获美股异常
    scheduler.add_job(
        job_market_monitor,
        CronTrigger(day_of_week='tue-sat', hour='5', minute='0', timezone=tz),
        id='us_close_monitor',
        name='美股收盘检测',
    )

    # ── 每日早晨新闻扫描（9:00）──
    scheduler.add_job(
        job_daily_news,
        CronTrigger(hour='9', minute='0', timezone=tz),
        id='daily_news',
        name='每日新闻公告',
    )

    # ── 每周日 20:00 周报 ──
    scheduler.add_job(
        job_weekly_digest,
        CronTrigger(day_of_week='sun', hour='20', minute='0', timezone=tz),
        id='weekly_digest',
        name='每周摘要',
    )

    print("=" * 55)
    print("  投资监控系统启动（纪律优先版）")
    print("=" * 55)
    print("\n  调度配置：")
    print("  · 交易时段监控：工作日 9:35 / 11:35 / 14:35")
    print("  · 美股收盘检测：Tue-Sat 05:00")
    print("  · 每日新闻公告：每天 09:00")
    print("  · 每周摘要报告：每周日 20:00")
    print("\n  EXIT_PENDING 持仓（海尔智家、福耀玻璃）已从监控中剔除")
    print("  监控原则：信息主动找你，你不主动找信息\n")

    # 启动时执行一次初始化
    print("  >>> 启动时执行一次初始化监控...")
    monitor_stocks()
    check_earnings_calendar()

    print("\n  开始定时调度...")
    scheduler.start()
