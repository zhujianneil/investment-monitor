import akshare as ak
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from config import STOCKS
from models import save_announcement
from feishu_push import send_announcement_alert

def get_cn_announcements(symbol, name):
    announcements = []
    try:
        df = ak.stock_notice_report(symbol=symbol)
        if not df.empty:
            for _, row in df.head(5).iterrows():
                announcements.append({
                    'title': row['公告标题'],
                    'url': row.get('公告链接', ''),
                    'pub_date': str(row.get('公告日期', datetime.now().date()))
                })
    except Exception as e:
        print(f"获取 {name} 公告失败: {e}")
    return announcements

def monitor_announcements():
    print(f"\n{'='*50}")
    print(f"开始监控公告 - {datetime.now()}")
    print(f"{'='*50}")
    
    total_new = 0
    
    for stock in STOCKS['CN']:
        symbol, name = stock['symbol'], stock['name']
        announcements = get_cn_announcements(symbol, name)
        for ann in announcements:
            if save_announcement(symbol, ann['title'], ann['url'], ann['pub_date']):
                print(f"[新公告] {name}: {ann['title']}")
                send_announcement_alert(name, symbol, ann['title'], ann['url'])
                total_new += 1
    
    print(f"\n公告监控完成，发现新公告 {total_new} 条")
    return total_new
