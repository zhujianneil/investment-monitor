import yfinance as yf
from datetime import datetime
from config import FCF_THRESHOLDS
from models import get_last_alert_time, save_alert
from feishu_push import send_fcf_alert

def calculate_fcf_multiple(symbol):
    try:
        ticker = yf.Ticker(symbol)
        market_cap = ticker.info.get('marketCap')
        cashflow = ticker.cashflow
        if cashflow.empty:
            return None
        
        operating_cf = cashflow.loc['Operating Cash Flow'].iloc[0] if 'Operating Cash Flow' in cashflow.index else None
        capex = cashflow.loc['Capital Expenditure'].iloc[0] if 'Capital Expenditure' in cashflow.index else None
        
        if operating_cf and capex:
            fcf = operating_cf + capex
            if fcf > 0:
                fcf_multiple = market_cap / fcf
                return {
                    'fcf_multiple': fcf_multiple,
                    'price': ticker.info.get('currentPrice', ticker.info.get('regularMarketPrice', 0))
                }
    except Exception as e:
        print(f"计算 FCF 倍数失败: {e}")
    return None

def check_fcf_thresholds():
    print(f"\n{'='*50}")
    print(f"检查 FCF 触发线 - {datetime.now()}")
    print(f"{'='*50}")
    
    alerts = []
    
    for symbol, config in FCF_THRESHOLDS.items():
        name = config['name']
        buy_threshold, sell_threshold = config['buy_threshold'], config['sell_threshold']
        
        data = calculate_fcf_multiple(symbol)
        if data:
            fcf_multiple, price = data['fcf_multiple'], data['price']
            print(f"{name}: FCF 倍数 = {fcf_multiple:.1f}x, 买入线={buy_threshold}x, 卖出线={sell_threshold}x")
            
            if fcf_multiple <= buy_threshold:
                if not get_last_alert_time(symbol, 'fcf_buy', hours=24):
                    send_fcf_alert(name, 'buy', fcf_multiple, price)
                    save_alert(symbol, 'fcf_buy', f"FCF {fcf_multiple:.1f}x <= {buy_threshold}x")
                    alerts.append((name, 'buy', fcf_multiple))
            elif fcf_multiple >= sell_threshold:
                if not get_last_alert_time(symbol, 'fcf_sell', hours=24):
                    send_fcf_alert(name, 'sell', fcf_multiple, price)
                    save_alert(symbol, 'fcf_sell', f"FCF {fcf_multiple:.1f}x >= {sell_threshold}x")
                    alerts.append((name, 'sell', fcf_multiple))
    
    print(f"\nFCF 检查完成，触发告警 {len(alerts)} 条")
    return alerts
