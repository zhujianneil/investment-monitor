import os
from dotenv import load_dotenv

load_dotenv()
FEISHU_WEBHOOK = os.getenv('FEISHU_WEBHOOK')

# 获取项目根目录 (investment-monitor/)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, 'data', 'investment.db')
REPORTS_PATH = os.path.join(BASE_DIR, 'reports')

os.makedirs(os.path.join(BASE_DIR, 'data'), exist_ok=True)
os.makedirs(REPORTS_PATH, exist_ok=True)

# ============================================================
# 持仓 & 观察名单 深度同步
# ============================================================

PORTFOLIO = {

    # ── 1. VALUE_WATCHER (估值驱动型) ──────────────────────────
    
    '0700.HK': {
        'name': '腾讯控股',
        'market': 'HK',
        'yf_symbol': '0700.HK',
        'monitor_type': 'VALUE_WATCHER',
        'fcf': {'buy': 15, 'covered_call': 25, 'sell': 30},
        'anomaly_threshold': 0.05,
        'news_keywords': ['腾讯', '游戏版号', '监管', 'AI投入', '微信', '视频号'],
        'earnings_months': [3, 5, 8, 11],
    },
    
    'PDD': {
        'name': '拼多多',
        'market': 'US',
        'yf_symbol': 'PDD',
        'monitor_type': 'VALUE_WATCHER',
        'fcf': {'buy': 12, 'sell': 25}, # 拼多多增长快，FCF 阈值设得更积极
        'anomaly_threshold': 0.08,
        'news_keywords': ['拼多多', 'Temu', '出海', '电商', '黄峥'],
        'earnings_months': [3, 5, 8, 11],
    },
    
    '09992.HK': {
        'name': '泡泡玛特',
        'market': 'HK',
        'yf_symbol': '9992.HK',
        'monitor_type': 'VALUE_WATCHER',
        'fcf': {'buy': 20, 'sell': 35},
        'anomaly_threshold': 0.07,
        'news_keywords': ['泡泡玛特', '盲盒', '出海', 'IP', '王宁'],
        'earnings_months': [3, 8],
    },

    '600036': {
        'name': '招商银行',
        'market': 'CN',
        'akshare_symbol': '600036',
        'monitor_type': 'VALUE_WATCHER',
        'fcf': {'buy': None, 'sell': None}, # 银行看 PB/ROE，这里设为观察
        'anomaly_threshold': 0.05,
        'news_keywords': ['招商银行', '不良率', '净息差', '分红', '零售银行'],
        'earnings_months': [3, 8],
    },

    # ── 2. EVENT_DRIVEN (事件/政策驱动型) ────────────────────────
    
    'V': {
        'name': 'Visa',
        'market': 'US',
        'yf_symbol': 'V',
        'monitor_type': 'EVENT_DRIVEN',
        'anomaly_threshold': 0.05,
        'news_keywords': ['Visa', 'DOJ', 'antitrust', 'stablecoin', 'cross-border payment'],
        'earnings_months': [1, 4, 7, 10],
    },

    '600941': {
        'name': '中国移动',
        'market': 'CN',
        'akshare_symbol': '600941',
        'monitor_type': 'EVENT_DRIVEN',
        'anomaly_threshold': 0.06,
        'news_keywords': ['中国移动', '利润上缴', '提速降费', '5G', '算力网络'],
        'earnings_months': [3, 8],
    },

    'BRK-B': {
        'name': '伯克希尔B',
        'market': 'US',
        'yf_symbol': 'BRK-B',
        'monitor_type': 'EVENT_DRIVEN',
        'anomaly_threshold': 0.08,
        'news_keywords': ['Berkshire', 'Buffett', '巴菲特', 'annual letter'],
        'earnings_months': [2, 8],
    },
    
    '688009': {
        'name': '中国通号',
        'market': 'CN',
        'akshare_symbol': '688009',
        'monitor_type': 'EVENT_DRIVEN',
        'anomaly_threshold': 0.07,
        'news_keywords': ['中国通号', '高铁', '城轨', '订单', '中标'],
        'earnings_months': [3, 8],
    },

    # ── 3. EARNINGS_WAIT (等待财报/观察项) ────────────────────────
    
    '1810.HK': {
        'name': '小米集团',
        'market': 'HK',
        'yf_symbol': '1810.HK',
        'monitor_type': 'EARNINGS_WAIT',
        'fcf': {'sell': 22, 'hard_sell': 25},
        'anomaly_threshold': 0.06,
        'news_keywords': ['小米', '汽车交付', 'SU7', '印度'],
        'earnings_months': [3, 5, 8, 11],
    },

    'NVDA': {
        'name': '英伟达',
        'market': 'US',
        'yf_symbol': 'NVDA',
        'monitor_type': 'EARNINGS_WAIT',
        'anomaly_threshold': 0.08,
        'news_keywords': ['NVIDIA', 'AI', 'H100', 'B200', 'GPU', '黄仁勋'],
        'earnings_months': [2, 5, 8, 11],
    },

    '9988.HK': {
        'name': '阿里巴巴',
        'market': 'HK',
        'yf_symbol': '9988.HK',
        'monitor_type': 'EARNINGS_WAIT',
        'news_keywords': ['阿里巴巴', '阿里云', '淘天', '马云', '吴泳铭'],
        'earnings_months': [2, 5, 8, 11],
    },

    '002156': {
        'name': '通富微电',
        'market': 'CN',
        'akshare_symbol': '002156',
        'monitor_type': 'EARNINGS_WAIT',
        'news_keywords': ['通富微电', '封装', '半导体', '芯片', 'AMD'],
        'earnings_months': [3, 8],
    },

    '02050.HK': {
        'name': '健世科技',
        'market': 'HK',
        'yf_symbol': '2050.HK',
        'monitor_type': 'EARNINGS_WAIT',
        'news_keywords': ['健世科技', '瓣膜', '医疗器械', '临床', '三尖瓣'],
        'earnings_months': [3, 8],
    },

    '600019': {
        'name': '宝钢股份',
        'market': 'CN',
        'akshare_symbol': '600019',
        'monitor_type': 'EARNINGS_WAIT',
        'news_keywords': ['宝钢', '铁矿石', '钢价', '粗钢'],
        'earnings_months': [3, 8],
    },

    # ── 4. EXIT_PENDING (退出项) ────────────────────────────────
    
    '600690': {
        'name': '海尔智家',
        'market': 'CN',
        'monitor_type': 'EXIT_PENDING',
    },
    '600660': {
        'name': '福耀玻璃',
        'market': 'CN',
        'monitor_type': 'EXIT_PENDING',
    },
}

# ============================================================
# 衍生配置（自动生成兼容格式）
# ============================================================
def _build_stocks_compat():
    stocks = {'CN': [], 'HK': [], 'US': []}
    for symbol, cfg in PORTFOLIO.items():
        if cfg['monitor_type'] == 'EXIT_PENDING': continue
        market = cfg['market']
        entry = {'symbol': symbol, 'name': cfg['name'], 'market': market}
        stocks[market].append(entry)
    return stocks

STOCKS = _build_stocks_compat()

ALL_NEWS_KEYWORDS = {
    s: {'name': c['name'], 'keywords': c.get('news_keywords', [])}
    for s, c in PORTFOLIO.items() if c['monitor_type'] != 'EXIT_PENDING'
}
