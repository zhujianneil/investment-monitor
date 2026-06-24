import os
from dotenv import load_dotenv

load_dotenv()
FEISHU_WEBHOOK = os.getenv('FEISHU_WEBHOOK')
FEISHU_WEBHOOK_BACKUP = os.getenv('FEISHU_WEBHOOK_BACKUP')  # 可选：failover 第二通道

# 推送可靠性配置
FEISHU_MAX_RETRIES = int(os.getenv('FEISHU_MAX_RETRIES', '3'))
FEISHU_RETRY_BACKOFF = float(os.getenv('FEISHU_RETRY_BACKOFF', '0.5'))  # 秒，指数退避基数

# 数据/报告目录（2026-06-11 修复路径 bug）
# 旧代码：os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
#   → 容器内 = "/"（因为 __file__=/app/app/config.py，dirname dirname = /）
#   → DB_PATH=/data/investment.db（容器内 ephemeral，重启即丢！）
# 新逻辑：优先使用 /app/data（与 docker-compose 的 volumes 挂载一致），
#   仅在 /app/data 不可写时降级到 /tmp 兜底（开发模式）
_BASE_CANDIDATE = '/app/data'
if not os.path.isdir(_BASE_CANDIDATE) or not os.access(_BASE_CANDIDATE, os.W_OK):
    _BASE_CANDIDATE = '/tmp/investment-monitor'
BASE_DIR = _BASE_CANDIDATE
DB_PATH = os.path.join(BASE_DIR, 'investment.db')
REPORTS_PATH = os.path.join('/app/reports' if os.path.isdir('/app/reports') else os.path.join(BASE_DIR, '..', 'reports'), '')
FEISHU_DLQ_PATH = os.path.join(BASE_DIR, 'feishu_dlq.jsonl')

os.makedirs(BASE_DIR, exist_ok=True)
os.makedirs(os.path.dirname(REPORTS_PATH) or '.', exist_ok=True)

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
        # 2026-06-20 P0: 加 Tencent 英文名, Yahoo 英文新闻流相关校验靠它
        'news_keywords': ['腾讯', 'Tencent', '游戏版号', '监管', 'AI投入', '微信', '视频号'],
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
        # 2026-06-20 P0: 加 Pop Mart 英文名
        'news_keywords': ['泡泡玛特', 'Pop Mart', '盲盒', '出海', 'IP', '王宁'],
        'earnings_months': [3, 8],
    },

    '600036': {
        'name': '招商银行',
        'market': 'CN',
        'akshare_symbol': '600036',
        'monitor_type': 'VALUE_WATCHER',
        'fcf': {'buy': None, 'sell': None}, # 银行看 PB/ROE，这里设为观察
        'anomaly_threshold': 0.05,
        # 2026-06-21 注释: '分红' 是行业泛词 (2026-06-20 P0 bug: 银河微电分红公告错配到招行)
        # match_holdings 修复后已要求 cfg.name 出现在 title 才推, 此词暂保留作主题信号但不再触发误推
        'news_keywords': ['招商银行', '不良率', '净息差', '零售银行'],
        'earnings_months': [3, 8],
    },

    '600018': {
        'name': '上港集团',
        'market': 'CN',
        'akshare_symbol': '600018',
        'monitor_type': 'VALUE_WATCHER',
        'fcf': {'buy': None, 'sell': None},  # 港口类, 关注 PB/吞吐量
        'anomaly_threshold': 0.05,
        'news_keywords': ['上港集团', '港口', '集装箱吞吐量', '上海港', '关税'],
        'earnings_months': [3, 8],
    },

    # ── 2. EVENT_DRIVEN (事件/政策驱动型) ────────────────────────
    
    'V': {
        'name': 'Visa',
        'market': 'US',
        'yf_symbol': 'V',
        'monitor_type': 'EVENT_DRIVEN',
        'anomaly_threshold': 0.05,
        'news_keywords': ['Visa', 'VISA', 'DOJ', 'antitrust', 'stablecoin', 'cross-border payment'],
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
        'news_keywords': ['Berkshire', '伯克希尔', 'Buffett', '巴菲特', 'annual letter', 'BRK.B', 'BRK-B'],
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
        # 2026-06-20 P0: 加 Xiaomi 英文名
        'news_keywords': ['小米', 'Xiaomi', '汽车交付', 'SU7', '印度'],
        'earnings_months': [3, 5, 8, 11],
    },

    'NVDA': {
        'name': '英伟达',
        'market': 'US',
        'yf_symbol': 'NVDA',
        'monitor_type': 'EARNINGS_WAIT',
        'anomaly_threshold': 0.08,
        # 2026-06-20 P0 修复: 去掉 'AI' 关键词 — 几乎所有美股新闻都含 "AI",
        # 导致 yfinance 全市场热门流里 SpaceX/Gold/XRP 等都被错推到 NVDA.
        # 2026-06-23 P1: 加 Jensen/Huang (黄仁勋英文名), 加 HIVE/H200/GTC 行业词
        'news_keywords': ['NVIDIA', 'Nvidia', '英伟达', '黄仁勋', 'Jensen', 'Huang',
                          'H100', 'B200', 'B100', 'H200', 'Blackwell', 'Hopper', 'GPU', 'GTC'],
        'earnings_months': [2, 5, 8, 11],
    },

    '9988.HK': {
        'name': '阿里巴巴',
        'market': 'HK',
        'yf_symbol': '9988.HK',
        'monitor_type': 'EARNINGS_WAIT',
        # 2026-06-20 P0: 加 Alibaba / BABA 英文名
        'news_keywords': ['阿里巴巴', 'Alibaba', 'BABA', '阿里云', '淘天', '马云', '吴泳铭'],
        'earnings_months': [2, 5, 8, 11],
    },

    '002156': {
        'name': '通富微电',
        'market': 'CN',
        'akshare_symbol': '002156',
        'monitor_type': 'EARNINGS_WAIT',
        # 2026-06-21 注释: '半导体'/'芯片' 是行业泛词, 任何同业公告都会被错配 (同招行 bug)
        # match_holdings 修复后已要求 cfg.name 出现在 title 才推, 此词暂保留作主题信号但不再触发误推
        'news_keywords': ['通富微电', '封装', 'AMD'],
        'earnings_months': [3, 8],
    },

    '002050': {
        'name': '三花智控',
        'market': 'CN',
        'akshare_symbol': '002050',
        'monitor_type': 'EARNINGS_WAIT',
        'news_keywords': ['三花智控', '机器人', '热管理', '新能源车', '特斯拉'],
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
        'akshare_symbol': '600690',
        'monitor_type': 'EXIT_PENDING',
    },
    '600660': {
        'name': '福耀玻璃',
        'market': 'CN',
        'akshare_symbol': '600660',
        'monitor_type': 'EXIT_PENDING',
    },
    '3606.HK': {
        'name': '福耀玻璃 H',
        'market': 'HK',
        'yf_symbol': '3606.HK',
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


# ============================================================
# AH 溢价信号系统 (2026-06-22 新增)
# ============================================================

# AH 双重上市大盘股样本 (A 代码, 名称, H 代码 yfinance 4 位)
# 用于自算 AH 溢价指数（备源 / 主源）
# 选取标准：AH 双重上市 + 流通市值大 + yfinance 双边都有行情
# 注: H 代码必须是 yfinance 能查到的 4 位数（部分老代码 yfinance 没收，换成近似大股）
AH_DUAL_LISTED = [
    ("600036", "招商银行",     "3968"),     # ✓ 招行
    ("601398", "工商银行",     "1398"),     # ✓ 工行
    ("601318", "中国平安",     "2318"),     # ✓ 平安
    ("601988", "中国银行",     "3988"),     # ✓ 中行
    ("601628", "中国人寿",     "2628"),     # ✓ 人寿
    ("601088", "中国神华",     "1088"),     # ✓ 神华
    ("601328", "交通银行",     "3328"),     # ✓ 交行
    ("600027", "华电国际",     "1071"),     # ✓ 华电
    ("601898", "中煤能源",     "1898"),     # ✓ 中煤
    ("601939", "建设银行",     "939"),      # ✓ 建行
]  # fetcher 会按列名对齐 — yfinance 不支持的代码由 fetcher 静默 skip

# AH 溢价信号阈值（动态分位计算所需基础）
AH_PREMIUM_QUANTILE_HIGH = 0.80     # > 80分位为高位
AH_PREMIUM_QUANTILE_STRONG = 0.90   # > 90分位为强高位
AH_PREMIUM_QUANTILE_LOW = 0.20      # < 20分位为低位

# 南向资金阈值（动态倍数基准窗口）
SOUTHBOUND_WINDOW_DAYS = 30          # 30日均值基准
SOUTHBOUND_FACTOR_HIGH = 1.5         # > 1.5× 30日均值 = 高流入
SOUTHBOUND_FACTOR_OUTFLOW = -0.5     # < -0.5× 30日均值 = 净流出加速

# USDHKD 弱方保证（v1.2：制度常量 + 分位双轨）
USDHKD_WEAK_FLOOR = 7.83             # 制度阈值（弱方保证 7.85 前 0.02 触发）
USDHKD_RECOVER = 7.80               # 从弱方回稳到 < 7.80
USDHKD_QUANTILE_WEAK = 0.95         # 1 年分位 ≥ 95% 也算弱方压力

# 收敛/分化 5日变化率阈值
AH_DELTA_CONVERGE = -2.0             # 5日Δ < -2% → 收敛加速
AH_DELTA_DIVERGE = 3.0               # 5日Δ > +3% → 分化加速
