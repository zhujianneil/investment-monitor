import requests
import json
from datetime import datetime
from config import FEISHU_WEBHOOK

def send_message(title, content, color='blue'):
    if not FEISHU_WEBHOOK:
        print("未配置飞书 Webhook")
        return False
    
    card = {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {"tag": "plain_text", "content": title},
                "template": color
            },
            "elements": [
                {"tag": "markdown", "content": content},
                {"tag": "note", "elements": [
                    {"tag": "plain_text", "content": f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"}
                ]}
            ]
        }
    }
    
    try:
        response = requests.post(FEISHU_WEBHOOK, headers={'Content-Type': 'application/json'}, data=json.dumps(card))
        result = response.json()
        if result.get('StatusCode') == 0:
            print(f"飞书消息发送成功: {title}")
            return True
        else:
            print(f"飞书消息发送失败: {result}")
            return False
    except Exception as e:
        print(f"飞书消息发送异常: {e}")
        return False

def send_price_alert(stock_name, symbol, price, change_pct):
    direction = "📈 上涨" if change_pct > 0 else "📉 下跌"
    color = "red" if change_pct < -0.03 else "green" if change_pct > 0.03 else "blue"
    content = f"**{stock_name}** ({symbol})\n当前价格: **{price:.2f}**\n变动幅度: **{direction} {abs(change_pct)*100:.2f}%**"
    return send_message(f"⚠️ 股价异动提醒", content, color)

def send_announcement_alert(stock_name, symbol, title, url):
    content = f"**{stock_name}** ({symbol})\n📝 {title}\n[查看详情]({url})"
    return send_message("📢 重要公告提醒", content, "orange")

def send_fcf_alert(stock_name, action, fcf_multiple, price):
    action_text = "🟢 买入信号" if action == 'buy' else "🔴 卖出信号"
    content = f"**{stock_name}**\n{action_text}\n当前 FCF 倍数: **{fcf_multiple:.1f}x**\n当前股价: **{price:.2f}**"
    color = "green" if action == 'buy' else "red"
    return send_message(f"💰 FCF 估值触发", content, color)
