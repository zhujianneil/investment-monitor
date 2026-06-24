"""
ah_analyzer.py — AH 溢价 × 南向资金 × USDHKD 信号引擎 (v1.1)

设计原则（2026-06-22 user 拍板的 v1.1 规则）：
  - 静态阈值 → 动态分位/倍数（适配中枢漂移）
  - 三因子交叉信号：AH高位 + 南向持续 + 港元回稳
  - 双方向信号：收敛（机会）+ 分化（风险）

输入: SQLite ah_premium / southbound / usdhkd (由 ah_fetcher 写入)
输出: {
    "as_of": "2026-06-22",
    "ah_premium": {"value": 23.66, "pct_change_5d": -1.8, "quantile_1y": 0.82, ...},
    "southbound": {"today_total": -58.2, "sum_5d": ..., "mean_30d": ..., "factor_vs_mean": ...},
    "usdhkd":    {"close": 7.8392, "pct_change_5d": ...},
    "signals":   [{"code": "STRONG_CONVERGE", "level": "STRONG", "msg": "..."}, ...],
  }
"""

import sqlite3
import logging
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from config import (
    DB_PATH,
    AH_PREMIUM_QUANTILE_HIGH,
    AH_PREMIUM_QUANTILE_STRONG,
    AH_PREMIUM_QUANTILE_LOW,
    SOUTHBOUND_WINDOW_DAYS,
    SOUTHBOUND_FACTOR_HIGH,
    SOUTHBOUND_FACTOR_OUTFLOW,
    USDHKD_WEAK_FLOOR,
    USDHKD_RECOVER,
    USDHKD_QUANTILE_WEAK,
    AH_DELTA_CONVERGE,
    AH_DELTA_DIVERGE,
)

logger = logging.getLogger("ah_analyzer")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")


# ============================================================
# 数据读取
# ============================================================
def _load_series(table: str, value_col: str, lookback_days: int = 400) -> pd.DataFrame:
    """从 SQLite 读取时序，按 date 升序。"""
    cutoff = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    with sqlite3.connect(DB_PATH) as c:
        df = pd.read_sql_query(
            f"SELECT date, {value_col} AS value FROM {table} WHERE date>=? ORDER BY date",
            c,
            params=[cutoff],
            parse_dates=["date"],
        )
    return df


def _latest_value(table: str, value_col: str) -> Optional[tuple]:
    with sqlite3.connect(DB_PATH) as c:
        row = c.execute(
            f"SELECT date, {value_col}, source FROM {table} ORDER BY date DESC LIMIT 1"
        ).fetchone()
    return row  # (date, value, source) or None


# ============================================================
# 信号计算
# ============================================================
def compute_ah_premium_metrics(lookback_days: int = 365) -> dict:
    """AH 溢价指标 + 动态分位 + 5日变化率"""
    df = _load_series("ah_premium", "value", lookback_days)
    if df.empty:
        return {"available": False}
    latest = df.iloc[-1]
    value = float(latest["value"])
    # 5日变化率
    if len(df) >= 6:
        prev5 = float(df.iloc[-6]["value"])
        delta5 = (value - prev5) / prev5 * 100 if prev5 else 0.0
    else:
        delta5 = 0.0
    # 1 年分位
    quantile = float((df["value"] <= value).mean()) if not df.empty else 0.5
    # 20/50/80 分位线（用于卡片展示"当前位置"）
    return {
        "available": True,
        "date": latest["date"].strftime("%Y-%m-%d"),
        "value": round(value, 3),
        "delta_5d_pct": round(delta5, 2),
        "quantile_1y": round(quantile, 3),
        "median_1y": round(float(df["value"].median()), 3),
        "p20_1y": round(float(df["value"].quantile(0.20)), 3),
        "p80_1y": round(float(df["value"].quantile(0.80)), 3),
        "min_1y": round(float(df["value"].min()), 3),
        "max_1y": round(float(df["value"].max()), 3),
        "samples": len(df),
    }


def compute_southbound_metrics(lookback_days: int = 365) -> dict:
    df = _load_series("southbound", "total_net", lookback_days)
    if df.empty:
        return {"available": False}
    latest = df.iloc[-1]
    today_total = float(latest["value"]) if pd.notna(latest["value"]) else 0.0
    # 5日累计 / 30日均值
    last5 = df["value"].tail(5).dropna()
    sum_5d = float(last5.sum()) if not last5.empty else 0.0
    last30 = df["value"].tail(SOUTHBOUND_WINDOW_DAYS).dropna()
    mean_30d = float(last30.mean()) if not last30.empty else 0.0
    std_30d = float(last30.std()) if len(last30) > 1 else 0.0
    factor = today_total / mean_30d if mean_30d != 0 else 0.0
    factor_5d = sum_5d / (mean_30d * 5) if mean_30d != 0 else 0.0
    return {
        "available": True,
        "date": latest["date"].strftime("%Y-%m-%d"),
        "today_total": round(today_total, 2),
        "sum_5d": round(sum_5d, 2),
        "mean_30d": round(mean_30d, 2),
        "std_30d": round(std_30d, 2),
        "factor_today_vs_mean": round(factor, 2),
        "factor_5d_vs_mean": round(factor_5d, 2),
        "is_outflow": today_total < 0,
    }


def compute_usdhkd_metrics(lookback_days: int = 365) -> dict:
    df = _load_series("usdhkd", "close", lookback_days)
    if df.empty:
        return {"available": False}
    latest = df.iloc[-1]
    value = float(latest["value"])
    if len(df) >= 6:
        prev5 = float(df.iloc[-6]["value"])
        delta5 = (value - prev5) / prev5 * 100 if prev5 else 0.0
    else:
        delta5 = 0.0
    distance_to_floor = round(7.85 - value, 4)
    # v1.2: 双重判断 — 制度常量 + 1 年分位（任一触发即告警）
    quantile_1y = float((df["value"] <= value).mean()) if not df.empty else 0.5
    return {
        "available": True,
        "date": latest["date"].strftime("%Y-%m-%d"),
        "close": round(value, 4),
        "delta_5d_pct": round(delta5, 3),
        "distance_to_weak_floor": distance_to_floor,
        # 制度常量：直接接近 7.85
        "is_near_weak_floor": value >= USDHKD_WEAK_FLOOR,
        # 分位判断：>= 95% 历史分位（说明比近 1 年 95% 时间都弱）
        "is_weak_quantile_high": quantile_1y >= USDHKD_QUANTILE_WEAK,
        "is_recovered_from_weak": value < USDHKD_RECOVER,
        "quantile_1y": round(quantile_1y, 3),
    }


# ============================================================
# 信号生成（v1.1）
# ============================================================
def generate_signals(ah: dict, sb: dict, hkd: dict) -> list:
    """
    返回 [{code, level, msg, factors: [...]}]
    level: STRONG / MEDIUM / INFO / WARN
    """
    signals = []

    if not (ah.get("available") and sb.get("available") and hkd.get("available")):
        signals.append({
            "code": "DATA_INCOMPLETE",
            "level": "WARN",
            "msg": "数据缺失，无法生成完整信号",
            "factors": [],
        })
        return signals

    # ---- 收敛方向 ----

    # AH_高位（动态分位）
    if ah["quantile_1y"] >= AH_PREMIUM_QUANTILE_STRONG:
        signals.append({
            "code": "AH_PREMIUM_STRONG_HIGH",
            "level": "MEDIUM",
            "msg": f"AH 溢价处于 1 年 {ah['quantile_1y']*100:.0f}% 分位（值 {ah['value']}），显著高位",
            "factors": [f"quantile={ah['quantile_1y']}", f"value={ah['value']}"],
        })
    elif ah["quantile_1y"] >= AH_PREMIUM_QUANTILE_HIGH:
        signals.append({
            "code": "AH_PREMIUM_HIGH",
            "level": "INFO",
            "msg": f"AH 溢价处于 1 年 {ah['quantile_1y']*100:.0f}% 分位（值 {ah['value']}），偏高",
            "factors": [f"quantile={ah['quantile_1y']}", f"value={ah['value']}"],
        })

    # 南向_持续（动态倍数）
    if sb["factor_5d_vs_mean"] >= SOUTHBOUND_FACTOR_HIGH:
        signals.append({
            "code": "SOUTHBOUND_SUSTAINED_INFLOW",
            "level": "MEDIUM",
            "msg": f"南向 5 日累计 {sb['sum_5d']}亿，是 30 日均值的 {sb['factor_5d_vs_mean']:.1f} 倍",
            "factors": [f"sum_5d={sb['sum_5d']}", f"factor={sb['factor_5d_vs_mean']:.2f}"],
        })

    # HKD_弱方 / 回稳（v1.2: 制度常量 OR 分位双轨）
    hkd_near_weak = hkd["is_near_weak_floor"] or hkd["is_weak_quantile_high"]
    if hkd_near_weak:
        signals.append({
            "code": "HKD_NEAR_WEAK_FLOOR",
            "level": "MEDIUM",
            "msg": f"USDHKD={hkd['close']} 接近弱方保证 7.85（差 {hkd['distance_to_weak_floor']:+.4f}，分位 {hkd['quantile_1y']*100:.0f}%）",
            "factors": [f"close={hkd['close']}", f"distance={hkd['distance_to_weak_floor']}", f"q={hkd['quantile_1y']}"],
        })
    if hkd["is_recovered_from_weak"] and hkd["delta_5d_pct"] < -0.2:
        signals.append({
            "code": "HKD_RECOVERING",
            "level": "INFO",
            "msg": f"USDHKD 5 日 {hkd['delta_5d_pct']:+.2f}%，从弱方回稳至 {hkd['close']}",
            "factors": [f"delta_5d={hkd['delta_5d_pct']}", f"close={hkd['close']}"],
        })

    # 🔵 强收敛 = AH高位 + 南向持续 + 港元回稳（三因子）
    factor_codes = {s["code"] for s in signals}
    if (
        "AH_PREMIUM_HIGH" in factor_codes or "AH_PREMIUM_STRONG_HIGH" in factor_codes
    ) and "SOUTHBOUND_SUSTAINED_INFLOW" in factor_codes and (
        "HKD_RECOVERING" in factor_codes or not hkd["is_near_weak_floor"]
    ):
        signals.append({
            "code": "STRONG_CONVERGE",
            "level": "STRONG",
            "msg": "🔵 强收敛信号：A 股相对港股溢价偏高 + 南向资金持续流入 + 港元回稳，三因子共振",
            "factors": [
                f"AH 分位={ah['quantile_1y']:.0%}",
                f"南向 5 日 = {sb['sum_5d']}亿 ({sb['factor_5d_vs_mean']:.1f}x)",
                f"USDHKD = {hkd['close']}",
            ],
        })

    # 收敛加速（5日Δ 强负）
    if ah["delta_5d_pct"] <= AH_DELTA_CONVERGE:
        signals.append({
            "code": "CONVERGE_ACCELERATING",
            "level": "MEDIUM",
            "msg": f"AH 溢价 5 日变化 {ah['delta_5d_pct']:+.2f}%，收敛加速",
            "factors": [f"delta_5d={ah['delta_5d_pct']}"],
        })

    # ---- 恶化/反向方向 ----

    # 🚨 分化加速
    if ah["delta_5d_pct"] >= AH_DELTA_DIVERGE and sb["is_outflow"]:
        signals.append({
            "code": "DIVERGE_ACCELERATING",
            "level": "STRONG",
            "msg": f"🚨 分化加速：AH 溢价 5 日 +{ah['delta_5d_pct']:.2f}% 同时南向净流出 {sb['today_total']:.0f}亿",
            "factors": [f"delta_5d={ah['delta_5d_pct']}", f"south_today={sb['today_total']}"],
        })

    # 价差极窄警告
    if ah["quantile_1y"] <= AH_PREMIUM_QUANTILE_LOW:
        signals.append({
            "code": "AH_PREMIUM_NARROW",
            "level": "INFO",
            "msg": f"AH 溢价处于 1 年 {ah['quantile_1y']*100:.0f}% 分位，H 股相对不便宜",
            "factors": [f"quantile={ah['quantile_1y']}", f"value={ah['value']}"],
        })

    # 过热警告（v1.1 — 南向强流入场景）
    if (
        ah["quantile_1y"] <= AH_PREMIUM_QUANTILE_LOW
        and sb["factor_5d_vs_mean"] >= SOUTHBOUND_FACTOR_HIGH
    ):
        signals.append({
            "code": "SOUTHBOUND_OVERHEAT",
            "level": "WARN",
            "msg": f"⚠️ 过热警告：AH 溢价低分位 + 南向仍 {sb['factor_5d_vs_mean']:.1f}x 均值，可能在追高 H 股",
            "factors": [f"quantile={ah['quantile_1y']}", f"factor_5d={sb['factor_5d_vs_mean']}"],
        })

    # 🔴 反向分化（v1.2 — 你指出的「资金跑 + 溢价窄」矛盾场景）
    # 触发：AH 低分位 + 南向 5 日累计净流出 + (港元触弱方 OR 港元高分位)
    # 含义：H 股已被抢筹到极高位，但资金已开始撤退，触弱方加剧 — 反转风险高
    if (
        ah["quantile_1y"] <= AH_PREMIUM_QUANTILE_LOW
        and sb["sum_5d"] < 0
        and (hkd["is_near_weak_floor"] or hkd["is_weak_quantile_high"])
    ):
        signals.append({
            "code": "REVERSE_DIVERGE_RISK",
            "level": "STRONG",
            "msg": f"🔴 反向分化：AH 已收敛到 {ah['quantile_1y']*100:.0f}% 分位 + 南向 5 日净流出 {sb['sum_5d']:.0f}亿 + USDHKD {hkd['close']} 触弱方 — H 股反转风险高",
            "factors": [
                f"AH 分位={ah['quantile_1y']*100:.0f}%",
                f"南向 5 日={sb['sum_5d']:.0f}亿",
                f"USDHKD={hkd['close']}",
            ],
        })

    # 港元触弱方警戒（与 AH 无关，但单独算汇率风险）
    if hkd["is_near_weak_floor"] and hkd["delta_5d_pct"] > 0.3:
        signals.append({
            "code": "HKD_PRESSURE_RISING",
            "level": "WARN",
            "msg": f"港元贬值压力上升（5 日 +{hkd['delta_5d_pct']:.2f}%，距弱方 {hkd['distance_to_weak_floor']:.4f}）",
            "factors": [f"close={hkd['close']}", f"delta_5d={hkd['delta_5d_pct']}"],
        })

    # 按 level 排序：STRONG > MEDIUM > WARN > INFO
    level_order = {"STRONG": 0, "MEDIUM": 1, "WARN": 2, "INFO": 3}
    signals.sort(key=lambda s: level_order.get(s["level"], 9))
    return signals


# ============================================================
# 主入口
# ============================================================
def run() -> dict:
    ah = compute_ah_premium_metrics()
    sb = compute_southbound_metrics()
    hkd = compute_usdhkd_metrics()
    signals = generate_signals(ah, sb, hkd)
    return {
        "as_of": datetime.now().strftime("%Y-%m-%d"),
        "ah_premium": ah,
        "southbound": sb,
        "usdhkd": hkd,
        "signals": signals,
        "signal_count": len(signals),
        "strong_signals": [s for s in signals if s["level"] == "STRONG"],
    }


if __name__ == "__main__":
    import json as _j
    print(_j.dumps(run(), indent=2, ensure_ascii=False, default=str))
