"""
feishu_push.py — 飞书消息推送

按警报类型提供不同的消息模板，每种模板都内嵌了"下一步行动"提示，
避免收到消息之后不知道该做什么。
"""
import requests
import json
from datetime import datetime
from config import FEISHU_WEBHOOK


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
    try:
        r = requests.post(
            FEISHU_WEBHOOK,
            headers={'Content-Type': 'application/json'},
            data=json.dumps(card),
            timeout=10,
        )
        result = r.json()
        if result.get('StatusCode') == 0 or result.get('code') == 0:
            print(f"  [飞书] ✓ 推送成功: {title}")
            return True
        else:
            print(f"  [飞书] ✗ 推送失败: {result}")
            return False
    except Exception as e:
        print(f"  [飞书] ✗ 推送异常: {e}")
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

def send_keyword_news_alert(name, symbol, title, url, keywords):
    """EVENT_DRIVEN 持仓的关键词新闻命中"""
    matched = [kw for kw in keywords if kw.lower() in title.lower()]
    content = (
        f"**{name}** ({symbol})\n"
        f"📰 {title}\n"
        f"关键词命中：`{'` `'.join(matched)}`\n\n"
        f"[查看原文]({url})\n\n"
        f"---\n**提示**：这是事件驱动型监控的核心信号，建议仔细阅读。"
    )
    return _send(f"📢 关键新闻 — {name}", content, 'orange')


# ── 公告提醒 ──────────────────────────────────────────────

def send_announcement_alert(name, symbol, title, url):
    """A股公告推送"""
    content = (
        f"**{name}** ({symbol})\n"
        f"📝 {title}\n"
        f"[查看公告]({url})"
    )
    return _send(f"📢 新公告 — {name}", content, 'orange')


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
