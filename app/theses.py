# -*- coding: utf-8 -*-
"""
theses.py — 持仓投资逻辑（thesis）定义 (2026-06-24 新增)

定位:
  每只持仓一张"逻辑卡" = 几条关键假设(支柱)。
  thesis_tracker.py 会把带该 symbol 的 event 用 LLM 归档到对应假设下,
  并判定 支持 / 削弱 / 中性,前端 /thesis 按"持仓 → 假设支柱 → 证据流"展示。

结构(混合模式):
  - STANDARD_PILLARS = 五个默认支柱,每只持仓都有(护城河/成长/管理层与资本配置/估值/风险)。
  - 某只有特殊论点 → 直接在该 symbol 的 assumptions 里追加 id 以 'x_' 开头的自定义支柱。

每条 assumption 字段:
  id        — 稳定标识(英文,别改;改了会触发该假设下证据重新归档)
  label     — 中文显示名
  statement — 这条假设你赌的是什么(一句话)
  breaks_if — 被推翻条件:出现什么信号说明这条假设错了 / 要重估
              (这是监控的灵魂 —— LLM 归档时会重点对照这一条判 stance)

symbol key 用 PORTFOLIO 里的主键(美股裸代码 / 港股带 .HK / A股6位)。
thesis_tracker 做了容错(.HK、前导零、yf/akshare 别名),但优先写规范主键。

⚠️ 下面 3 只是 DRAFT 草稿(基于公开信息 + 你的 config 关键词起草)。
   请按你自己真实的投资逻辑替换 —— 这张卡是给"未来的你"对照用的,越诚实越有用。
   验证体验 OK 后,把其余持仓照同样格式补进来即可。
"""

# ── 五个默认支柱(混合模式的固定底座)──────────────────────────
# 仅作模板参考;每只持仓在 THESES 里写自己的具体内容。
STANDARD_PILLARS = [
    {"id": "moat",       "label": "护城河"},
    {"id": "growth",     "label": "成长驱动"},
    {"id": "management", "label": "管理层与资本配置"},
    {"id": "valuation",  "label": "估值"},
    {"id": "risk",       "label": "风险"},
]

# label 速查(前端/LLM 用):标准 id → 中文
PILLAR_LABELS = {p["id"]: p["label"] for p in STANDARD_PILLARS}


# ── 持仓逻辑卡 ────────────────────────────────────────────────
THESES = {

    # ========================================================
    # 腾讯控股 0700.HK  (VALUE_WATCHER)  —— DRAFT 草稿
    # ========================================================
    "0700.HK": {
        "name": "腾讯控股",
        "summary": "中国数字生活的底层基础设施:微信生态 + 高毛利游戏/广告/支付,"
                   "是被低估的现金牛,叠加视频号商业化与 AI 的免费期权。",
        "assumptions": [
            {
                "id": "moat",
                "label": "护城河",
                "statement": "微信 13 亿 MAU 的社交 + 支付双边网络,转换成本极高、生态自我强化。",
                "breaks_if": "用户时长被短视频持续侵蚀且夺不回;监管强制互联互通/拆生态削弱闭环。",
            },
            {
                "id": "growth",
                "label": "成长驱动",
                "statement": "视频号商业化(广告加载率↑)+ 游戏出海 + 微信小店交易生态 + 金融科技。",
                "breaks_if": "视频号 eCPM/加载见顶;游戏版号或玩法监管收紧;海外游戏管线断档。",
            },
            {
                "id": "management",
                "label": "管理层与资本配置",
                "statement": "管理层稳健,持续大额回购注销 + 分红 + 派发被投股权(美团/京东)。",
                "breaks_if": "回购显著放缓;资本配置转向低回报扩张或激进并购。",
            },
            {
                "id": "valuation",
                "label": "估值",
                "statement": "剔除投资组合后核心经营按 FCF/经调整净利估值不贵,投资组合提供隐藏价值。",
                "breaks_if": "核心经营利润停滞而估值已 price-in 高增长。",
            },
            {
                "id": "risk",
                "label": "风险",
                "statement": "可控风险敞口:监管(游戏/反垄断/数据)、中美/ADR 地缘、宏观消费。",
                "breaks_if": "出现结构性监管转向或宏观消费长期低迷。",
            },
            {
                "id": "x_ai",
                "label": "AI 投入与回报",
                "statement": "自研混元 + AI 重塑广告与云,是利润率的潜在放大器。",
                "breaks_if": "AI 资本开支大增但 ROI 不明,持续拖累利润率。",
            },
        ],
    },

    # ========================================================
    # 拼多多 PDD  (VALUE_WATCHER)  —— DRAFT 草稿
    # ========================================================
    "PDD": {
        "name": "拼多多",
        "summary": "极致供应链效率 + Temu 全球化的高增长低成本电商;主站现金牛供血 Temu,"
                   "核心赌注是 Temu 的单位经济与跨境监管。",
        "assumptions": [
            {
                "id": "moat",
                "label": "护城河",
                "statement": "主站'低价心智' + 白牌供应链 + 农产品上行;全托管对商家的组织能力。",
                "breaks_if": "淘宝/抖音电商在低价心智上持续夺回份额;商家因压价/罚款大规模流失。",
            },
            {
                "id": "growth",
                "label": "成长驱动",
                "statement": "Temu 海外 GMV 高速扩张 + 变现率提升;主站货币化仍有空间。",
                "breaks_if": "Temu 增长见顶或被迫退出市场;主站货币化触及商家承受上限。",
            },
            {
                "id": "management",
                "label": "管理层与资本配置",
                "statement": "黄峥隐退后职业经理人团队,执行力强但极度低调。",
                "breaks_if": "治理透明度进一步下降;巨额现金沉淀却无股东回报且再投资回报转差。",
            },
            {
                "id": "valuation",
                "label": "估值",
                "statement": "高增长但因 Temu 亏损与监管担忧被折价;Temu 转盈则重估。",
                "breaks_if": "主站利润见顶叠加 Temu 持续大额亏损。",
            },
            {
                "id": "risk",
                "label": "风险",
                "statement": "中美监管(PCAOB/退市)、Temu 合规(强迫劳动审查、数据)、补贴战。",
                "breaks_if": "退市风险实质化或核心市场合规受阻。",
            },
            {
                "id": "x_temu_ue",
                "label": "Temu 单位经济",
                "statement": "从全托管转半托管/本地仓,补贴退坡后能否走向盈利是关键。",
                "breaks_if": "de minimis 关税豁免取消 / 物流成本上升且无法转嫁,UE 恶化。",
            },
        ],
    },

    # ========================================================
    # 招商银行 600036  (VALUE_WATCHER)  —— DRAFT 草稿
    # ========================================================
    "600036": {
        "name": "招商银行",
        "summary": "中国最优质零售银行:低负债成本 + 高 ROE + 财富管理护城河;"
                   "命运系于净息差、零售信贷质量与地产/城投敞口。",
        "assumptions": [
            {
                "id": "moat",
                "label": "护城河",
                "statement": "低成本核心存款(活期占比高)+ 零售/私行客户黏性 + 金葵花/私行 AUM。",
                "breaks_if": "活期存款流失、负债成本抬升;财富管理 AUM 与中收持续下滑。",
            },
            {
                "id": "growth",
                "label": "成长驱动",
                "statement": "大财富管理中收 + 零售信贷;增长次于稳健,重质不重量。",
                "breaks_if": "中收长期负增长;零售转型停滞。",
            },
            {
                "id": "management",
                "label": "管理层与资本配置",
                "statement": "市场化程度高、稳健的管理层,稳定高分红。",
                "breaks_if": "管理层大幅更迭且战略漂移;分红率下调。",
            },
            {
                "id": "valuation",
                "label": "估值",
                "statement": "看 PB/ROE 与股息率;ROE 领先同业,PB 低于历史中枢则有安全边际。",
                "breaks_if": "ROE 持续下行至与同业趋同。",
            },
            {
                "id": "risk",
                "label": "风险",
                "statement": "净息差收窄(LPR 下行)、地产与城投不良、零售信贷资产质量。",
                "breaks_if": "净息差跌破关键水平且不良率与拨备同时恶化。",
            },
            {
                "id": "x_nim_npl",
                "label": "净息差与不良",
                "statement": "息差企稳 + 不良率/拨备覆盖稳定,是这只票的核心监控变量。",
                "breaks_if": "净息差与不良双双走坏,拨备覆盖率快速下滑。",
            },
        ],
    },
}


# ── 辅助:取某 symbol 的 assumptions / label 映射 ──────────────

def get_thesis(symbol: str):
    """返回某 symbol 的 thesis dict,无则 None。"""
    return THESES.get(symbol)


def assumption_label(symbol: str, assumption_id: str) -> str:
    """assumption_id → 中文 label;找不到回退到标准支柱表或原 id。"""
    th = THESES.get(symbol)
    if th:
        for a in th.get("assumptions", []):
            if a["id"] == assumption_id:
                return a["label"]
    return PILLAR_LABELS.get(assumption_id, assumption_id)


def thesis_symbols():
    """已定义 thesis 的所有 symbol。"""
    return list(THESES.keys())
