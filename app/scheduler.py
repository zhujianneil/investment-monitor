from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

from stock_monitor import monitor_stocks
from news_monitor import monitor_announcements
from fcf_calculator import check_fcf_thresholds
from models import init_db

tz = pytz.timezone('Asia/Shanghai')

def job_stock_monitor():
    print("\n>>> 开始执行股价监控任务")
    monitor_stocks()

def job_news_monitor():
    print("\n>>> 开始执行公告监控任务")
    monitor_announcements()

def job_fcf_check():
    print("\n>>> 开始执行 FCF 检查任务")
    check_fcf_thresholds()

def start():
    init_db()
    
    scheduler = BlockingScheduler(timezone=tz)
    
    scheduler.add_job(job_stock_monitor, CronTrigger(day_of_week='mon-fri', hour='9-14', minute='30', timezone=tz))
    scheduler.add_job(job_stock_monitor, CronTrigger(day_of_week='mon-fri', hour='10-14', minute='0', timezone=tz))
    scheduler.add_job(job_news_monitor, CronTrigger(hour='9,12,18', minute='0', timezone=tz))
    scheduler.add_job(job_fcf_check, CronTrigger(hour='9', minute='30', timezone=tz))
    
    print("="*50)
    print("投资监控系统启动")
    print("="*50)
    print("\n定时任务配置:")
    print("- 股价监控: 交易时段每小时")
    print("- 公告监控: 每天 9:00, 12:00, 18:00")
    print("- FCF 检查: 每天 9:30")
    print("\n启动时执行一次初始化监控...")
    
    job_stock_monitor()
    job_fcf_check()
    
    print("\n开始定时调度...")
    scheduler.start()
