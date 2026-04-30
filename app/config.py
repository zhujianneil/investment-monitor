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
# 持仓监控配置
#
# 监控类型 (monitor_type):
#   VALUE_WATCHER   - FCF 估值驱动。有明确的买入/卖出 FCF 倍数阈值。
#                     这是最重要的一层，触发即行动（走四条件清单）。
#   EVENT_DRIVEN    - 事件驱动。价格警报宽松，新闻关键词警报要密。
#                     典型：Visa (DOJ案)、中国移动 (政策)
#   EARNINGS_WAIT   - 等待财报数据窗口。主要任务是等季报，不需要看价格。
#                     触发：财报发布日提前提醒 + 发布当日推送关键数据
#   EXIT_PENDING    - 已决定退出，监控停止。只需要决定退出节奏。
#                     不产生任何新警报，避免诱发"再等等"的犹豫。
# ============================================================

PORTFOLIO = {

    # ── 腾讯控股 ────────────────────────────────────────────
    '0700.HK': {
        'name': '腾讯控股',
        'market': 'HK',
        'yf_symbol': '0700.HK',
        'monitor_type': 'VALUE_WATCHER',

        # FCF 倍数阈值（三档纪律线）
        'fcf': {
            'buy':          15,   # ≤15x → 触发四条件买入清单
            'covered_call': 25,   # ≥25x → 开始卖 covered call
            'sell':         30,   # ≥30x → 准备被 call 走 / 主动减仓
        },

        # 异常波动警报（单日 ±5%，提示去看新闻——不是去交易）
        'anomaly_threshold': 0.05,

        # 核心监控关键词（新闻过滤用）
        'news_keywords': ['腾讯', '游戏版号', '监管', 'AI投入', '微信', '视频号', '反垄断'],

        # 财报发布月份（提前7天提醒）
        'earnings_months': [3, 5, 8, 11],
    },

    # ── 小米集团 ────────────────────────────────────────────
    '1810.HK': {
        'name': '小米集团',
        'market': 'HK',
        'yf_symbol': '1810.HK',
        'monitor_type': 'EARNINGS_WAIT',   # 等 Q 数据，不是看价格

        'fcf': {
            'buy':  None,    # 暂不设买入线，等财报确认
            'sell': 22,      # ≥22x → 开始减仓
            'hard_sell': 25, # ≥25x → 坚决清仓
        },

        'anomaly_threshold': 0.05,

        'news_keywords': ['小米', '汽车交付', '手机出货', '印度', 'SU7', '澎湃', 'AIoT'],

        'earnings_months': [3, 5, 8, 11],
    },

    # ── 中国移动 ────────────────────────────────────────────
    '600941': {
        'name': '中国移动',
        'market': 'CN',
        'akshare_symbol': '600941',
        'monitor_type': 'EVENT_DRIVEN',   # 政策驱动，新闻 > 价格

        'fcf': {
            'buy':  None,   # 央企，FCF 倍数参考意义有限
            'sell': None,
        },

        # EVENT_DRIVEN 的价格警报可以宽松
        'anomaly_threshold': 0.07,

        # 核心监控变量（来自你的分析框架）
        'news_keywords': ['中国移动', '利润上缴', '提速降费', '运营商重组', '5G', '算力网络', 'SOE'],

        'earnings_months': [3, 8],   # 中报 + 年报
    },

    # ── Visa ────────────────────────────────────────────────
    'V': {
        'name': 'Visa',
        'market': 'US',
        'yf_symbol': 'V',
        'monitor_type': 'EVENT_DRIVEN',   # DOJ 案是核心，事件驱动

        'fcf': {
            'buy':  None,   # 当前主要等 DOJ 判决，不主动加仓
            'sell': None,
        },

        'anomaly_threshold': 0.05,

        # 新闻密度要高
        'news_keywords': ['Visa', 'DOJ', 'antitrust', 'stablecoin', 'CBDC', 'cross-border payment', 'Mastercard'],

        'earnings_months': [1, 4, 7, 10],
    },

    # ── 伯克希尔 B ───────────────────────────────────────────
    'BRK-B': {
        'name': '伯克希尔B',
        'market': 'US',
        'yf_symbol': 'BRK-B',
        'monitor_type': 'EVENT_DRIVEN',   # 股东信 + 股东大会是主信息源

        'fcf': {
            'buy':  None,
            'sell': None,
        },

        # 基本不需要日常监控，只关注年度大事件
        'anomaly_threshold': 0.08,

        'news_keywords': ['Berkshire', 'Buffett', '巴菲特', 'annual letter', '股东大会', 'Apple stake'],

        'earnings_months': [2, 8],   # 年报 + 中报
    },

    # ── 中国通号 ────────────────────────────────────────────
    '688009': {
        'name': '中国通号',
        'market': 'CN',
        'akshare_symbol': '688009',
        'monitor_type': 'EVENT_DRIVEN',   # 政策 + 订单驱动

        'fcf': {
            'buy':  None,
            'sell': None,
        },

        'anomaly_threshold': 0.07,

        'news_keywords': ['中国通号', '高铁', '城轨', '铁路投资', '轨道交通', '订单', '中标'],

        'earnings_months': [3, 8],
    },

    # ── 海尔智家 ── EXIT_PENDING ─────────────────────────────
    '600690': {
        'name': '海尔智家',
        'market': 'CN',
        'akshare_symbol': '600690',
        'monitor_type': 'EXIT_PENDING',   # 已决定退出，停止监控

        # 退出节奏备注（仅供参考，不产生警报）
        'exit_note': '分批退出，评估是否等价格回升再卖',

        'fcf': {},
        'news_keywords': [],
        'anomaly_threshold': 1.0,  # 实际上不监控
    },

    # ── 福耀玻璃 ── EXIT_PENDING ─────────────────────────────
    '600660': {
        'name': '福耀玻璃',
        'market': 'CN',
        'akshare_symbol': '600660',
        'monitor_type': 'EXIT_PENDING',   # 已决定退出，停止监控

        'exit_note': '评估退出节奏',

        'fcf': {},
        'news_keywords': [],
        'anomaly_threshold': 1.0,
    },
}

# ============================================================
# 衍生出旧格式（保持向后兼容）
# ============================================================
def _build_stocks_compat():
    """生成兼容旧版 STOCKS 格式的数据"""
    stocks = {'CN': [], 'HK': [], 'US': []}
    for symbol, cfg in PORTFOLIO.items():
        if cfg['monitor_type'] == 'EXIT_PENDING':
            continue
        market = cfg['market']
        entry = {'symbol': symbol, 'name': cfg['name'], 'market': market}
        stocks[market].append(entry)
    return stocks

STOCKS = _build_stocks_compat()

# ============================================================
# 新闻监控关键词（全局汇总，供 RSS / Google News 抓取）
# ============================================================
ALL_NEWS_KEYWORDS = {}
for symbol, cfg in PORTFOLIO.items():
    if cfg.get('news_keywords') and cfg['monitor_type'] != 'EXIT_PENDING':
        ALL_NEWS_KEYWORDS[symbol] = {
            'name': cfg['name'],
            'keywords': cfg['news_keywords'],
        }
