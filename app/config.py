import os
from dotenv import load_dotenv

load_dotenv()

FEISHU_WEBHOOK = os.getenv('FEISHU_WEBHOOK')
DB_PATH = os.getenv('DB_PATH', '/app/data/investment.db')
REPORTS_PATH = os.getenv('REPORTS_PATH', '/app/reports')

STOCKS = {
    'US': [
        {'symbol': 'BRK-B', 'name': '伯克希尔B', 'market': 'US'},
        {'symbol': 'PDD', 'name': '拼多多', 'market': 'US'},
        {'symbol': 'V', 'name': 'Visa', 'market': 'US'},
    ],
    'HK': [
        {'symbol': '0700.HK', 'code': '00700', 'name': '腾讯控股', 'market': 'HK'},
        {'symbol': '1810.HK', 'code': '01810', 'name': '小米集团', 'market': 'HK'},
    ],
    'CN': [
        {'symbol': '600941', 'name': '中国移动', 'market': 'CN'},
        {'symbol': '600660', 'name': '福耀玻璃', 'market': 'CN'},
        {'symbol': '600036', 'name': '招商银行', 'market': 'CN'},
        {'symbol': '600690', 'name': '海尔智家', 'market': 'CN'},
        {'symbol': '600019', 'name': '宝钢股份', 'market': 'CN'},
        {'symbol': '600018', 'name': '上港集团', 'market': 'CN'},
        {'symbol': '688009', 'name': '中国通号', 'market': 'CN'},
    ]
}

FCF_THRESHOLDS = {
    '0700.HK': {
        'name': '腾讯控股',
        'buy_threshold': 25,
        'sell_threshold': 30,
    }
}

PRICE_ALERT_THRESHOLD = 0.03
