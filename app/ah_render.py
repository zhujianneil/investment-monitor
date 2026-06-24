"""
ah_render.py — AH 溢价 × 南向资金 × USDHKD 看板渲染

输出：
  1. Feishu 交互卡片（每日必推 + 强信号额外推）
  2. HTML dashboard（/opt/investment-monitor/reports/ah_premium_YYYYMMDD.html）

依赖：ah_analyzer.run() 返回的结构化数据 + jinja2 (已装)
"""

import os
import sqlite3
import logging
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
from jinja2 import Template

from config import DB_PATH, REPORTS_PATH
from ah_analyzer import run as run_analyzer
import feishu_push

logger = logging.getLogger("ah_render")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")


# ============================================================
# 颜色 + 图标（与 feishu_push 一致）
# ============================================================
_LEVEL_COLOR = {
    "STRONG": "red",
    "MEDIUM": "orange",
    "WARN": "yellow",
    "INFO": "blue",
}

_LEVEL_ICON = {
    "STRONG": "🔴",
    "MEDIUM": "🟡",
    "WARN": "⚠️",
    "INFO": "ℹ️",
}

_LEVEL_LABEL = {
    "STRONG": "强信号",
    "MEDIUM": "中等",
    "WARN": "警戒",
    "INFO": "提示",
}


def _fmt_ah(v: float) -> str:
    return f"{v:.2f}" if v is not None else "—"


def _fmt_hkd(v: float) -> str:
    return f"{v:.4f}" if v is not None else "—"


def _fmt_signed(v: float, suffix: str = "%") -> str:
    if v is None:
        return "—"
    sign = "+" if v > 0 else ""
    return f"{sign}{v:.2f}{suffix}"


def _fmt_yi(v: float) -> str:
    """亿元带符号"""
    if v is None:
        return "—"
    sign = "+" if v > 0 else ""
    return f"{sign}{v:.1f}亿"


def _quantile_band(q: float) -> str:
    """1 年分位用文字描述"""
    if q >= 0.90: return "🔴 极高位"
    if q >= 0.80: return "🟠 高位"
    if q >= 0.60: return "🟡 偏高"
    if q >= 0.40: return "⚪ 中位"
    if q >= 0.20: return "🟢 偏低"
    return "🔵 极低位"


# ============================================================
# Feishu 卡片
# ============================================================
def render_feishu_card(data: dict) -> tuple[str, str, str]:
    """
    返回 (title, content_md, color)
    - 标题：当日关键信号
    - 内容：分段 markdown
    - color: 强信号 → red；中等 → orange；常规 → blue
    """
    ah = data["ah_premium"]
    sb = data["southbound"]
    hkd = data["usdhkd"]
    signals = data["signals"]
    strong = data["strong_signals"]

    # 标题
    if strong:
        title = f"🔴 AH 收敛信号 · {data['as_of']}"
        color = "red"
    elif any(s["level"] == "MEDIUM" for s in signals):
        title = f"🟡 AH 溢价日报 · {data['as_of']}"
        color = "orange"
    else:
        title = f"🟢 AH 溢价日报 · {data['as_of']}"
        color = "blue"

    # 头部摘要
    header_lines = [
        "**📊 关键指标**",
        f"- **AH 溢价指数（2801.HK）**: {_fmt_ah(ah['value'])} | 5 日 {_fmt_signed(ah['delta_5d_pct'])} | 1 年分位 {_quantile_band(ah['quantile_1y'])} ({ah['quantile_1y']*100:.0f}%)",
        f"- **南向资金**: 今日 {_fmt_yi(sb['today_total'])} | 5 日累计 {_fmt_yi(sb['sum_5d'])} | vs 30 日均值 {sb['factor_5d_vs_mean']:.1f}x",
        f"- **USDHKD**: {_fmt_hkd(hkd['close'])} | 距弱方 {hkd['distance_to_weak_floor']:+.4f}",
    ]

    # 信号列表
    signal_lines = ["", "**🎯 信号清单**"]
    if signals:
        for s in signals:
            icon = _LEVEL_ICON.get(s["level"], "•")
            signal_lines.append(f"- {icon} **{_LEVEL_LABEL.get(s['level'], s['level'])}** · {s['msg']}")
    else:
        signal_lines.append("- 无（市场中性）")

    # 数据缺失提示
    footer_lines = []
    if not (ah.get("available") and sb.get("available") and hkd.get("available")):
        missing = []
        if not ah.get("available"): missing.append("AH 溢价")
        if not sb.get("available"): missing.append("南向资金")
        if not hkd.get("available"): missing.append("USDHKD")
        footer_lines.append("")
        footer_lines.append(f"⚠️ **数据缺失**: {', '.join(missing)}")

    # 数据源
    footer_lines.append("")
    footer_lines.append(f"📁 数据源: yfinance 2801.HK (主) · akshare 南向 · yfinance HKD=X | 详见 HTML 看板")

    content = "\n".join(header_lines + signal_lines + footer_lines)
    return title, content, color


def send_feishu_daily(data: Optional[dict] = None) -> bool:
    """每日推送接口。data=None 时自动跑 analyzer。"""
    if data is None:
        data = run_analyzer()
    title, content, color = render_feishu_card(data)
    return feishu_push._send(title, content, color=color)


def send_feishu_strong_signal(data: dict) -> bool:
    """强信号额外推送"""
    title, content, _ = render_feishu_card(data)
    return feishu_push._send(title, content, color="red")


# ============================================================
# HTML Dashboard
# ============================================================
_HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<title>AH 溢价 × 南向资金看板 · {{ date }}</title>
<style>
  body { font-family: -apple-system, "PingFang SC", "Microsoft YaHei", sans-serif; background: #0f1419; color: #e6e6e6; padding: 24px; max-width: 1200px; margin: 0 auto; }
  h1 { color: #fff; border-bottom: 2px solid #3b82f6; padding-bottom: 8px; }
  h2 { color: #94a3b8; margin-top: 32px; font-size: 16px; text-transform: uppercase; letter-spacing: 1px; }
  .card { background: #1e293b; border-radius: 8px; padding: 16px; margin: 12px 0; border-left: 4px solid #3b82f6; }
  .card.strong { border-left-color: #ef4444; }
  .card.medium { border-left-color: #f59e0b; }
  .card.warn { border-left-color: #eab308; }
  .card.info { border-left-color: #3b82f6; }
  .grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; margin: 16px 0; }
  .metric { background: #1e293b; border-radius: 8px; padding: 16px; }
  .metric .label { color: #94a3b8; font-size: 12px; }
  .metric .value { font-size: 28px; font-weight: bold; margin: 8px 0; color: #fff; }
  .metric .sub { color: #64748b; font-size: 12px; }
  .positive { color: #ef4444; }
  .negative { color: #10b981; }
  table { width: 100%; border-collapse: collapse; margin: 12px 0; font-size: 14px; }
  th, td { padding: 8px 12px; text-align: right; border-bottom: 1px solid #334155; }
  th:first-child, td:first-child { text-align: left; }
  th { background: #1e293b; color: #94a3b8; }
  .pill { display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 11px; }
  .pill.red { background: #ef4444; color: #fff; }
  .pill.orange { background: #f59e0b; color: #fff; }
  .pill.yellow { background: #eab308; color: #000; }
  .pill.blue { background: #3b82f6; color: #fff; }
  .footer { margin-top: 48px; padding-top: 16px; border-top: 1px solid #334155; color: #64748b; font-size: 12px; }
  pre { background: #0f172a; padding: 12px; border-radius: 4px; overflow-x: auto; font-size: 12px; }
</style>
</head>
<body>

<h1>AH 溢价 × 南向资金 × USDHKD 看板</h1>
<p>📅 {{ date }} · 数据来源: yfinance 2801.HK · akshare 沪深港通 · yfinance HKD=X</p>

<h2>📊 关键指标</h2>
<div class="grid">
  <div class="metric">
    <div class="label">AH 溢价指数 (2801.HK)</div>
    <div class="value">{{ ah.value }}</div>
    <div class="sub">5 日 {{ ah.delta_5d_pct | signed }} · 1 年分位 {{ ah.quantile_1y_pct }}%</div>
  </div>
  <div class="metric">
    <div class="label">南向资金 (今日)</div>
    <div class="value {% if sb.today_total >= 0 %}positive{% else %}negative{% endif %}">{{ sb.today_total | yi }}</div>
    <div class="sub">5 日累计 {{ sb.sum_5d | yi }} · vs 30 日均值 {{ sb.factor_5d_vs_mean }}x</div>
  </div>
  <div class="metric">
    <div class="label">USDHKD</div>
    <div class="value">{{ hkd.close }}</div>
    <div class="sub">距弱方保证 7.85: {{ hkd.distance_to_weak_floor }} · 5 日 {{ hkd.delta_5d_pct | signed }}</div>
  </div>
</div>

<h2>🎯 信号清单（{{ signal_count }} 个 · 强信号 {{ strong_count }} 个）</h2>
{% if signals %}
  {% for s in signals %}
  <div class="card {{ s.level_class }}">
    <div>
      <span class="pill {{ s.level_color }}">{{ s.level_label }}</span>
      <strong>{{ s.msg }}</strong>
    </div>
    <div class="sub" style="margin-top: 4px; color: #64748b;">
      {{ s.code }}{% if s.factors %} · {{ s.factors | join(' · ') }}{% endif %}
    </div>
  </div>
  {% endfor %}
{% else %}
  <div class="card info">市场中性，无显著信号。</div>
{% endif %}

<h2>📈 1 年 AH 溢价分位</h2>
<table>
  <tr><th>分位</th><th>数值</th></tr>
  <tr><td>当前</td><td><strong>{{ ah.value }}</strong></td></tr>
  <tr><td>90%</td><td>{{ ah.p80_1y }} ~ {{ ah.max_1y }}</td></tr>
  <tr><td>50% (中位)</td><td>{{ ah.median_1y }}</td></tr>
  <tr><td>20%</td><td>{{ ah.min_1y }} ~ {{ ah.p20_1y }}</td></tr>
</table>

<h2>💵 南向资金 5 日走势</h2>
<table>
  <tr><th>日期</th><th>沪净买</th><th>深净买</th><th>总净买</th></tr>
  {% for row in southbound_recent %}
  <tr>
    <td>{{ row.date }}</td>
    <td class="{% if row.sh_net and row.sh_net >= 0 %}positive{% else %}negative{% endif %}">{{ row.sh_net | yi }}</td>
    <td class="{% if row.sz_net and row.sz_net >= 0 %}positive{% else %}negative{% endif %}">{{ row.sz_net | yi }}</td>
    <td class="{% if row.total_net and row.total_net >= 0 %}positive{% else %}negative{% endif %}"><strong>{{ row.total_net | yi }}</strong></td>
  </tr>
  {% endfor %}
</table>

<h2>💱 USDHKD 近 10 日</h2>
<table>
  <tr><th>日期</th><th>USDHKD</th><th>距弱方</th></tr>
  {% for row in usdhkd_recent %}
  <tr>
    <td>{{ row.date }}</td>
    <td><strong>{{ row.close }}</strong></td>
    <td class="{% if row.close >= 7.84 %}positive{% else %}sub{% endif %}">{{ "%.4f" | format(7.85 - row.close) }}</td>
  </tr>
  {% endfor %}
</table>

<h2>🔍 数据完整性</h2>
<pre>{{ data_summary }}</pre>

<div class="footer">
  看板生成于 {{ timestamp }} · 由 investment-monitor / ah_render.py 渲染<br>
  信号规则: <a href="https://hermes-agent.nousresearch.com/docs" style="color:#3b82f6">v1.1 (动态分位 + HKD + 双向)</a>
</div>

</body>
</html>
"""


def _load_recent(table: str, value_cols: list, limit: int = 10) -> list:
    cols = ", ".join(value_cols)
    with sqlite3.connect(DB_PATH) as c:
        c.row_factory = sqlite3.Row
        rows = c.execute(
            f"SELECT date, {cols} FROM {table} ORDER BY date DESC LIMIT {limit}"
        ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        out.append(d)
    return out


def render_html(data: dict, output_path: Optional[str] = None) -> str:
    """渲染 HTML 看板到文件。返回文件路径。"""
    if output_path is None:
        date_str = data["as_of"].replace("-", "")
        output_path = os.path.join(REPORTS_PATH.rstrip("/"), f"ah_premium_{date_str}.html")

    # 准备模板数据
    ah = data["ah_premium"]
    sb = data["southbound"]
    hkd = data["usdhkd"]

    # Jinja filters
    def fmt_yi(v):
        if v is None or pd.isna(v): return "—"
        return f"{'+' if v and v > 0 else ''}{float(v):.1f}亿"
    def fmt_signed(v):
        if v is None: return "—"
        return f"{'+' if v > 0 else ''}{float(v):.2f}%"

    env = __import__('jinja2').Environment(autoescape=False)
    env.filters["yi"] = fmt_yi
    env.filters["signed"] = fmt_signed
    template = env.from_string(_HTML_TEMPLATE)

    # 信号卡片分类
    signals_for_html = []
    for s in data["signals"]:
        sig = {
            "code": s["code"],
            "msg": s["msg"],
            "factors": s.get("factors", []),
            "level_class": s["level"].lower(),
            "level_color": _LEVEL_COLOR.get(s["level"], "blue"),
            "level_label": _LEVEL_LABEL.get(s["level"], s["level"]),
        }
        signals_for_html.append(sig)

    # 数据完整性
    sb_rows = _load_recent("southbound", ["sh_net", "sz_net", "total_net"], 5)
    hkd_rows = _load_recent("usdhkd", ["close"], 10)
    data_summary_lines = [
        f"AH 溢价: {ah.get('available', False)} · 样本 {ah.get('samples', 0)}",
        f"南向: {sb.get('available', False)} · 5 日累计 {sb.get('sum_5d', '—')}",
        f"USDHKD: {hkd.get('available', False)} · 距弱方 {hkd.get('distance_to_weak_floor', '—')}",
    ]

    html = template.render(
        date=data["as_of"],
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ah={
            "value": _fmt_ah(ah.get("value")),
            "delta_5d_pct": ah.get("delta_5d_pct", 0),
            "quantile_1y_pct": round(ah.get("quantile_1y", 0) * 100, 1),
            "p20_1y": _fmt_ah(ah.get("p20_1y")),
            "p80_1y": _fmt_ah(ah.get("p80_1y")),
            "median_1y": _fmt_ah(ah.get("median_1y")),
            "min_1y": _fmt_ah(ah.get("min_1y")),
            "max_1y": _fmt_ah(ah.get("max_1y")),
        },
        sb={
            "today_total": sb.get("today_total", 0),
            "sum_5d": sb.get("sum_5d", 0),
            "factor_5d_vs_mean": sb.get("factor_5d_vs_mean", 0),
        },
        hkd={
            "close": _fmt_hkd(hkd.get("close")),
            "distance_to_weak_floor": hkd.get("distance_to_weak_floor", 0),
            "delta_5d_pct": hkd.get("delta_5d_pct", 0),
        },
        signals=signals_for_html,
        signal_count=len(data["signals"]),
        strong_count=len(data["strong_signals"]),
        southbound_recent=[{
            "date": r["date"],
            "sh_net": r["sh_net"],
            "sz_net": r["sz_net"],
            "total_net": r["total_net"],
        } for r in sb_rows],
        usdhkd_recent=[{"date": r["date"], "close": r["close"]} for r in hkd_rows],
        data_summary="\n".join(data_summary_lines),
    )

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    logger.info(f"HTML rendered: {output_path}")
    return output_path


# ============================================================
# 主入口
# ============================================================
def run(do_fetch: bool = True, push_feishu: bool = True) -> dict:
    """
    do_fetch=True 时先跑 fetcher 拉最新；之后跑 analyzer；渲染 HTML；可选推 Feishu。
    """
    if do_fetch:
        import ah_fetcher
        ah_fetcher.run_all()

    data = run_analyzer()

    html_path = render_html(data)

    feishu_ok = False
    if push_feishu:
        try:
            feishu_ok = send_feishu_daily(data)
        except Exception as e:
            logger.warning(f"feishu push fail: {e}")
            feishu_ok = False

    data["_render"] = {"html": html_path, "feishu_pushed": feishu_ok}
    return data


if __name__ == "__main__":
    import json as _j
    out = run(do_fetch=False, push_feishu=False)
    print(_j.dumps({k: v for k, v in out.items() if not k.startswith("_")}, indent=2, ensure_ascii=False, default=str))
    print(f"\nHTML: {out['_render']['html']}")
