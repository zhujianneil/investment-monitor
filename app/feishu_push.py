"""
feishu_push.py — 飞书消息推送

按警报类型提供不同的消息模板，每种模板都内嵌了"下一步行动"提示，
避免收到消息之后不知道该做什么。

可靠性加固（v2）：
  - 单通道指数退避重试（最多 FEISHU_MAX_RETRIES 次）
  - 主通道失败时切到 FEISHU_WEBHOOK_BACKUP（failover）
  - 所有通道都失败时把卡片序列化追加到 data/feishu_dlq.jsonl（DLQ）
  - 5xx / 网络异常 / 超时 才重试；4xx（参数错）直接 DLQ 不重试
"""
import os
import json
import time
from datetime import datetime
import requests
from config import (
    FEISHU_WEBHOOK,
    FEISHU_WEBHOOK_BACKUP,
    FEISHU_MAX_RETRIES,
    FEISHU_RETRY_BACKOFF,
    FEISHU_DLQ_PATH,
)


# 飞书 webhook 通用错误码：0 = 成功；非 0 = 业务失败
# 业务失败通常是卡片格式错或签名错，重试无意义，直接进 DLQ
_RETRYABLE_HTTP = {500, 502, 503, 504, 429}


def _post_once(webhook, card, timeout=10):
    """单次 POST。返回 (ok: bool, status_code: int|None, payload: dict|str)。"""
    try:
        r = requests.post(
            webhook,
            headers={'Content-Type': 'application/json'},
            data=json.dumps(card),
            timeout=timeout,
        )
    except requests.RequestException as e:
        return False, None, f"network: {e!r}"

    # 飞书 webhook 业务响应是 JSON
    try:
        body = r.json()
    except ValueError:
        body = r.text

    if r.status_code in _RETRYABLE_HTTP:
        return False, r.status_code, body

    success = r.ok and (isinstance(body, dict) and (body.get('StatusCode') == 0 or body.get('code') == 0))
    return success, r.status_code, body


def _send_with_retry(webhook, card, label):
    """对一个通道做指数退避重试。返回 True = 成功。"""
    if not webhook:
        return False
    for attempt in range(1, FEISHU_MAX_RETRIES + 1):
        ok, status, body = _post_once(webhook, card)
        if ok:
            return True
        # 4xx / 业务错：不重试
        if status is not None and 400 <= status < 500 and status not in _RETRYABLE_HTTP:
            print(f"  [飞书] ✗ {label} HTTP {status}（不重试）: {body}")
            return False
        if status is None:
            print(f"  [飞书] ✗ {label} 第 {attempt}/{FEISHU_MAX_RETRIES} 次 {body}")
        else:
            print(f"  [飞书] ✗ {label} 第 {attempt}/{FEISHU_MAX_RETRIES} 次 HTTP {status}: {body}")
        if attempt < FEISHU_MAX_RETRIES:
            time.sleep(FEISHU_RETRY_BACKOFF * (2 ** (attempt - 1)))
    return False


def _write_dlq(title, content, color, last_error):
    """所有通道都失败 → 落本地 JSONL（事后人工/脚本重放）。"""
    try:
        os.makedirs(os.path.dirname(FEISHU_DLQ_PATH), exist_ok=True)
        record = {
            "ts": datetime.now().isoformat(timespec='seconds'),
            "title": title,
            "content": content,
            "color": color,
            "last_error": str(last_error)[:500],
        }
        with open(FEISHU_DLQ_PATH, 'a', encoding='utf-8') as f:
            f.write(json.dumps(record, ensure_ascii=False) + '\n')
        print(f"  [飞书] ⚠ 已写入 DLQ: {FEISHU_DLQ_PATH}")
    except Exception as e:
        # DLQ 自身失败只能喊出来
        print(f"  [飞书] ✗✗ DLQ 写入失败（消息丢失）: {e}")


def replay_dlq(max_items=20, max_age_hours=48):
    """
    DLQ 重放（2026-06-11 新增）。
    启动时 / 周期性调用，尝试把历史失败的消息再发一遍。
    超过 max_age_hours 的丢弃；max_items 防止一次性推送轰炸。
    返回: dict {replayed, succeeded, dropped_old, total_remaining}
    """
    from datetime import timedelta
    if not os.path.exists(FEISHU_DLQ_PATH):
        return {"replayed": 0, "succeeded": 0, "dropped_old": 0, "total_remaining": 0}

    cutoff = datetime.now() - timedelta(hours=max_age_hours)
    succeeded = 0
    dropped_old = 0
    replayed = 0
    remaining_lines = []

    try:
        with open(FEISHU_DLQ_PATH, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except Exception as e:
        print(f"  [DLQ 重放] 读文件失败: {e}")
        return {"replayed": 0, "succeeded": 0, "dropped_old": 0, "total_remaining": 0}

    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
            ts = datetime.fromisoformat(rec.get('ts', ''))
        except Exception:
            # 损坏的记录直接丢弃
            continue

        if ts < cutoff:
            dropped_old += 1
            continue

        if replayed >= max_items:
            # 超过本次预算，剩下的下次重放
            remaining_lines.append(line)
            continue

        # 重新构造 card 推送
        card = {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {"tag": "plain_text", "content": rec['title']},
                    "template": rec.get('color', 'blue'),
                },
                "elements": [
                    {"tag": "markdown", "content": rec['content']},
                    {"tag": "note", "elements": [
                        {"tag": "plain_text",
                         "content": f"🔁 DLQ 重放 · 原失败 {rec.get('ts', '?')}"}
                    ]},
                ],
            },
        }
        replayed += 1
        if (_send_with_retry(FEISHU_WEBHOOK, card, "DLQ重放主")
                or (FEISHU_WEBHOOK_BACKUP and _send_with_retry(FEISHU_WEBHOOK_BACKUP, card, "DLQ重放备"))):
            succeeded += 1
        else:
            # 还失败，保留回队列
            remaining_lines.append(line)

    # 写回剩余队列
    try:
        if remaining_lines:
            with open(FEISHU_DLQ_PATH, 'w', encoding='utf-8') as f:
                f.write('\n'.join(remaining_lines) + '\n')
        else:
            os.remove(FEISHU_DLQ_PATH)
    except Exception as e:
        print(f"  [DLQ 重放] 写回失败: {e}")

    if replayed:
        print(f"  [DLQ 重放] 尝试 {replayed} 条，成功 {succeeded} 条；剩余 {len(remaining_lines)} 条，过期丢弃 {dropped_old} 条")

    return {
        "replayed": replayed,
        "succeeded": succeeded,
        "dropped_old": dropped_old,
        "total_remaining": len(remaining_lines),
    }


def _send(title, content, color='blue'):
    if not FEISHU_WEBHOOK:
        print(f"  [飞书] 未配置 Webhook，跳过推送: {title}")
        return False

    card = {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {"tag": "plain_text", "content": title},
                "template": color,
            },
            "elements": [
                {"tag": "markdown", "content": content},
                {"tag": "note", "elements": [
                    {"tag": "plain_text",
                     "content": f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')} · investment-monitor"}
                ]},
            ],
        },
    }

    # 主通道
    if _send_with_retry(FEISHU_WEBHOOK, card, "主通道"):
        print(f"  [飞书] ✓ 推送成功: {title}")
        return True

    # Failover：备份通道
    if FEISHU_WEBHOOK_BACKUP and _send_with_retry(FEISHU_WEBHOOK_BACKUP, card, "备份通道"):
        print(f"  [飞书] ✓ 备份通道推送成功: {title}")
        return True

    # 全军覆没 → DLQ
    _write_dlq(title, content, color, "主+备通道均失败")
    return False


# ── FCF 阈值触发 ──────────────────────────────────────────

def send_fcf_threshold_alert(name, symbol, action, fcf_multiple, price, threshold):
    """FCF 估值纪律线触发 — 这是最重要的警报"""
    if action == 'buy':
        icon, action_text, color = '🟢', '买入信号', 'green'
        next_step = "**下一步**：执行四条件清单。\n1. 护城河是否完整？\n2. 管理层是否可信？\n3. 买入理由是否仍然成立？\n4. 当前仓位是否允许加仓？\n\n通过四条件 → 分批买入。未通过 → 等待更好机会。"
    elif action == 'covered_call':
        icon, action_text, color = '🟡', 'Covered Call 信号', 'yellow'
        next_step = "**下一步**：评估是否卖出 covered call。\n- 若认为短期内不太可能大涨 → 卖出 covered call 收取权利金\n- 若认为仍有上行空间 → 继续持有，等待 sell 线"
    else:  # sell
        icon, action_text, color = '🔴', '减仓/卖出信号', 'red'
        next_step = "**下一步**：执行减仓清单。\n1. 这家公司还是我当初买的那家公司吗？\n2. 卖出后资金的再投向是否更优？\n3. 分批卖出 vs 一次性清仓？\n\n通过检查 → 执行卖出计划。"

    content = (
        f"{icon} **{name}** ({symbol})\n"
        f"当前 FCF 倍数：**{fcf_multiple:.1f}x**  |  阈值：{threshold}x\n"
        f"当前价格：{price:.2f}\n\n"
        f"---\n{next_step}"
    )
    return _send(f"💰 {action_text} — {name}", content, color)


# ── 异常波动 ──────────────────────────────────────────────

def send_anomaly_alert(name, symbol, price, change_pct):
    """单日异常波动 — 提示去看新闻，不是交易信号"""
    direction = "上涨" if change_pct > 0 else "下跌"
    content = (
        f"**{name}** ({symbol}) 今日{direction} **{abs(change_pct)*100:.1f}%**\n"
        f"当前价格：{price:.2f}\n\n"
        f"---\n"
        f"**提示**：这是被动通知，不是行动信号。\n"
        f"建议：去看看是否有相关新闻（10分钟内），判断是否属于核心监控变量的变化。\n"
        f"若不是 → 关掉手机，继续你的日程。"
    )
    color = 'red' if change_pct < 0 else 'green'
    return _send(f"⚡ 异常波动 — {name} {change_pct*100:+.1f}%", content, color)


# ── 关键词新闻 ────────────────────────────────────────────

def _classify_relevance(name: str, title: str, keywords: list) -> str:
    """
    2026-06-19 P0 修复: 区分"主体相关"vs"只是关键词出现"
    返回 'primary' (新闻主体就是该公司) | 'thematic' (主题相关但不是标的专属) | 'weak' (弱相关)
    规则: title 含公司名/简称/股票代码 → primary; 只命中主题词 → thematic
    """
    # 1) 公司名直接出现 (强主体)
    name_short = name.replace('集团', '').replace('控股', '').replace('股份', '')
    if name and len(name) >= 2 and name in title:
        return 'primary'
    if name_short and len(name_short) >= 2 and name_short in title:
        return 'primary'

    # 2) 关键词里有股票代码 / 简称
    # (关键词列表通常第一项是公司名)
    if keywords:
        first_kw = str(keywords[0])
        if first_kw and len(first_kw) >= 2 and first_kw in title:
            return 'primary'

    # 3) 只有主题词命中 (如 5G/算力 是行业概念, 非标的专属)
    if keywords:
        theme_hits = [k for k in keywords[1:] if k in title]  # 跳过第一个(公司名)
        if theme_hits:
            return 'thematic'

    return 'weak'


def send_keyword_news_alert(name, symbol, title, url, keywords, relevance: str = None):
    """
    EVENT_DRIVEN 持仓的关键词新闻命中
    relevance: 'primary' (主体相关) | 'thematic' (主题相关) | 'weak' (弱相关)
              None 时自动用 _classify_relevance 判断
    """
    matched = [kw for kw in keywords if kw.lower() in title.lower()]
    if relevance is None:
        relevance = _classify_relevance(name, title, keywords)

    # 标签 + 颜色: primary 红色 (主信号) | thematic 橙色 (行业信号) | weak 灰色
    if relevance == 'primary':
        tag = '🔴 主体相关'
        color = 'red'
        header_emoji = '🎯'
    elif relevance == 'thematic':
        tag = '🟡 主题相关'
        color = 'orange'
        header_emoji = '📡'
    else:
        tag = '⚪ 弱相关'
        color = 'grey'
        header_emoji = 'ℹ️'

    # 2026-06-22 P1 修复: 英文/中文新闻用不同措辞
    # yf_news 拉美股/港股新闻 title 必是英文, cfg.name 是中文
    # 之前: "新闻主体就是该公司" 对中文用户是误导 (title 里没有"阿里巴巴"也能 primary)
    has_cjk = any('\u4e00' <= c <= '\u9fff' for c in title)
    is_english_news = not has_cjk

    if relevance == 'primary':
        if is_english_news:
            hint = "**信号强度**：🔴 高 — 该英文新闻主体即本公司（按英文名/代码匹配），建议立即阅读。"
        else:
            hint = "**信号强度**：🔴 高 — 新闻主体就是该公司，建议立即阅读。"
    elif relevance == 'thematic':
        if is_english_news:
            hint = ("**信号强度**：🟡 中 — 英文新闻命中持仓英文关键词/代码，**非新闻主体**。\n"
                    "请判断是否真影响该公司。")
        else:
            hint = ("**信号强度**：🟡 中 — 命中行业主题词，**非标的专属**。\n"
                    "请判断是否真影响该公司（例如「5G」可能是行业新闻也可能是中国移动具体动作）。")
    else:
        hint = "**信号强度**：⚪ 低 — 弱相关，可能误报。"

    content = (
        f"**{name}** ({symbol}) {tag}\n"
        f"{header_emoji} {title}\n"
        f"关键词命中：`{'` `'.join(matched) or '(无)'}`\n\n"
        f"[查看原文]({url})\n\n"
        f"---\n{hint}"
    )
    return _send(f"{header_emoji} 关键新闻 — {name}", content, color)


# ── 公告提醒 ──────────────────────────────────────────────

def send_announcement_alert(name, symbol, title, url):
    """A股公告推送"""
    content = (
        f"**{name}** ({symbol})\n"
        f"📝 {title}\n"
        f"[查看公告]({url})"
    )
    return _send(f"📢 新公告 — {name}", content, 'orange')


# ── 龙虎榜异动（2026-06-22 新增）──────────────────────────
# 广发接口返回的上榜个股清单，由 lhb_stream 调本函数
def send_lhb_alert(name, symbol, lhb_item: dict, reason_text: str):
    """
    龙虎榜异动推送 — 持仓股上榜时告警

    lhb_item: 广发接口返回的标准化字段
        {trdCode, secuSht, clsPrc, dayChgRat, tnvVol, tnvVal, items[...], date, market}
    reason_text: 拼接好的上榜原因 (e.g. "涨幅偏离值达 7% / 连续 3 日累计 30%")
    """
    chg = lhb_item.get('dayChgRat')
    chg_str = f"{chg:+.2f}%" if isinstance(chg, (int, float)) else "N/A"
    cls = lhb_item.get('clsPrc')
    cls_str = f"{cls:.2f}" if isinstance(cls, (int, float)) else "N/A"
    tnv_val = lhb_item.get('tnvVal')
    tnv_str = f"{tnv_val/1e8:.2f} 亿" if isinstance(tnv_val, (int, float)) and tnv_val else "N/A"
    market = lhb_item.get('market', '?')
    trd_code = lhb_item.get('trdCode', '?')
    date_str = str(lhb_item.get('date', ''))

    # 涨跌幅 → 颜色
    if isinstance(chg, (int, float)):
        if chg >= 9.5:
            color = 'red'       # 涨停级
            tag = '🔴 涨停级异动'
        elif chg <= -9.5:
            color = 'red'
            tag = '🔴 跌停级异动'
        elif chg > 0:
            color = 'orange'
            tag = '🟠 涨幅异动'
        elif chg < 0:
            color = 'orange'
            tag = '🟠 跌幅异动'
        else:
            color = 'blue'
            tag = '🔵 换手/振幅异动'
    else:
        color = 'red'
        tag = '🔴 龙虎榜异动'

    content = (
        f"**{name}** ({symbol})  {tag}\n"
        f"📅 交易日：{date_str}  市场：**{market}**\n\n"
        f"**上榜原因**：{reason_text}\n\n"
        f"**关键数据**：\n"
        f"- 收盘价：{cls_str}\n"
        f"- 日涨跌幅：{chg_str}\n"
        f"- 成交额：{tnv_str}\n"
        f"- 交易代码：`{trd_code}`\n\n"
        f"---\n"
        f"**信号强度**：🔴 **官方异动阈值** (上交所/深交所定义) — 不是模型拍的 ±5%。\n\n"
        f"**为什么这条有用**：\n"
        f"1. 龙虎榜 = 上交所/深交所对涨跌幅/振幅/换手率超阈值的强制披露\n"
        f"2. 你收到 = 这只股**真的异动**,不是市场情绪波动\n"
        f"3. 上榜原因比涨跌幅本身更准 (e.g. 「振幅 15%」可能当天没涨没跌,但波动巨大)\n\n"
        f"**下一步**：\n"
        f"- 持仓逻辑是否仍然成立？(参考 cfg.fcf / anomaly_threshold)\n"
        f"- 是否触发止盈/止损线？\n"
        f"- 龙虎榜详情 (营业部席位) 需广发接口 `lhb_stock_detail`, 本 skill 未集成"
    )
    return _send(f"🚨 龙虎榜异动 — 持仓 {name}", content, color)


# ── 财报提醒 ──────────────────────────────────────────────

def send_earnings_reminder(name, symbol, estimated_date, cfg):
    """财报前 7 天提醒，内嵌该持仓的核心监控问题"""
    monitor_type = cfg.get('monitor_type', '')
    keywords     = cfg.get('news_keywords', [])
    fcf_cfg      = cfg.get('fcf', {})

    # 构建核心问题清单
    questions = []
    if fcf_cfg.get('buy'):
        questions.append(f"- FCF 倍数距买入线 {fcf_cfg['buy']}x 还有多远？")
    if fcf_cfg.get('sell') or fcf_cfg.get('hard_sell'):
        sell_t = fcf_cfg.get('sell') or fcf_cfg.get('hard_sell')
        questions.append(f"- FCF 倍数距卖出线 {sell_t}x 还有多远？")
    if keywords:
        questions.append(f"- 核心监控变量：{' / '.join(keywords[:4])}")
    questions.append("- 这家公司还是我当初买的那家公司吗？")
    questions.append("- 护城河有没有出现新的侵蚀迹象？")

    q_text = '\n'.join(questions)

    content = (
        f"**{name}** ({symbol}) 财报预计 **{estimated_date.strftime('%Y-%m-%d')}** 前后发布\n\n"
        f"---\n**建议提前准备的问题清单**：\n{q_text}\n\n"
        f"---\n**提示**：留出 1-2 小时做季度财报深读，带着问题去看，而不是看完再想问什么。"
    )
    return _send(f"📅 财报提醒 — {name}（{estimated_date.strftime('%m月')}）", content, 'purple')


# ── 每周摘要 ──────────────────────────────────────────────

def send_weekly_digest(fcf_alerts, anomaly_alerts, announcements, upcoming_earnings, portfolio):
    """周报：汇总一周内的所有信号"""
    sections = []

    # FCF 触发
    if fcf_alerts:
        lines = [f"- [{a['symbol']}] {a['message']}" for a in fcf_alerts[:5]]
        sections.append("**💰 本周 FCF 触发**\n" + '\n'.join(lines))
    else:
        sections.append("**💰 FCF 状态**：本周无估值阈值触发 ✓")

    # 异常波动
    if anomaly_alerts:
        lines = [f"- [{a['symbol']}] {a['message']}" for a in anomaly_alerts[:5]]
        sections.append("**⚡ 本周异常波动**\n" + '\n'.join(lines))
    else:
        sections.append("**⚡ 波动状态**：本周无异常波动 ✓")

    # 新闻摘要
    if announcements:
        ann_by_symbol = {}
        for ann in announcements[:10]:
            sym = ann['symbol']
            ann_by_symbol.setdefault(sym, []).append(ann['title'])
        lines = []
        for sym, titles in ann_by_symbol.items():
            name = portfolio.get(sym, {}).get('name', sym)
            lines.append(f"- **{name}**：{titles[0][:40]}{'...' if len(titles[0]) > 40 else ''}")
        sections.append("**📰 本周新动态**\n" + '\n'.join(lines))
    else:
        sections.append("**📰 新闻状态**：本周无新动态 ✓")

    # 下周财报提醒
    if upcoming_earnings:
        lines = [f"- {e['name']}（{e['symbol']}）预计 {e['date']}，还有 {e['days']} 天" for e in upcoming_earnings]
        sections.append("**📅 近期财报**\n" + '\n'.join(lines))

    # 静默确认
    total_signals = len(fcf_alerts) + len(anomaly_alerts) + len(announcements)
    if total_signals == 0:
        footer = "\n\n🟢 **系统静默确认**：本周无任何信号触发。这是好事——系统在正常运转，你的持仓没有需要响应的变化。"
    else:
        footer = f"\n\n📊 本周共 {total_signals} 条信号，见上方详情。"

    content = '\n\n---\n\n'.join(sections) + footer
    return _send(
        f"📋 本周投资监控周报 — {datetime.now().strftime('%Y.%m.%d')}",
        content,
        'blue'
    )


# ── 兼容旧接口 ────────────────────────────────────────────
# 保留这些函数名以防止其他地方的 import 报错
def send_price_alert(stock_name, symbol, price, change_pct):
    return send_anomaly_alert(stock_name, symbol, price, change_pct)

def send_fcf_alert(stock_name, action, fcf_multiple, price):
    return send_fcf_threshold_alert(stock_name, symbol, action, fcf_multiple, price, 0)
