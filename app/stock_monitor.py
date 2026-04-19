import akshare as ak
import yfinance as yf
from datetime import datetime
from config import STOCKS, PRICE_ALERT_THRESHOLD
from models import save_price, get_last_alert_time, save_alert
from feishu_push import send_price_alert

def get_cn_stock_price(symbol):
    try:
        df = ak.stock_zh_a_spot_em()
        stock = df[df['代码'] == symbol]
        if not stock.empty:
            return {
                'price': float(stock['最新价'].values[0]),
                'change_pct': float(stock['涨跌幅'].values[0]) / 100,
                'volume': int(stock['成交量'].values[0])
            }
    except Exception as e:
        print(f"获取A股 {symbol} 股价失败: {e}")
    return None

def get_hk_us_stock_price(symbol):
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period='2d')
        if len(hist) >= 2:
            current = hist['Close'].iloc[-1]
            previous = hist['Close'].iloc[-2]
            change_pct = (current - previous) / previous
            return {'price': float(current), 'change_pct': float(change_pct), 'volume': 0}
    except Exception as e:
        print(f"获取 {symbol} 股价失败: {e}")
    return None

def monitor_stocks():
    print(f"\n{'='*50}")
    print(f"开始监控股价 - {datetime.now()}")
    print(f"{'='*50}")
    
    alerts = []
    
    for stock in STOCKS['CN']:
        symbol, name = stock['symbol'], stock['name']
        data = get_cn_stock_price(symbol)
        if data:
            save_price(symbol, data['price'], data['change_pct'], data['volume'])
            print(f"{name}({symbol}): {data['price']:.2f} ({data['change_pct']*100:+.2f}%)")
            if abs(data['change_pct']) >= PRICE_ALERT_THRESHOLD:
                if not get_last_alert_time(symbol, 'price', hours=4):
                    send_price_alert(name, symbol, data['price'], data['change_pct'])
                    save_alert(symbol, 'price', f"涨跌幅 {data['change_pct']*100:.2f}%")
                    alerts.append((name, data['change_pct']))
    
    for stock in STOCKS['HK'] + STOCKS['US']:
        symbol, name = stock['symbol'], stock['name']
        data = get_hk_us_stock_price(symbol)
        if data:
            save_price(symbol, data['price'], data['change_pct'], data['volume'])
            print(f"{name}({symbol}): {data['price']:.2f} ({data['change_pct']*100:+.2f}%)")
            if abs(data['change_pct']) >= PRICE_ALERT_THRESHOLD:
                if not get_last_alert_time(symbol, 'price', hours=4):
                    send_price_alert(name, symbol, data['price'], data['change_pct'])
                    save_alert(symbol, 'price', f"涨跌幅 {data['change_pct']*100:.2f}%")
                    alerts.append((name, data['change_pct']))
    
    print(f"\n本轮监控完成，触发告警 {len(alerts)} 条")
    return alerts
