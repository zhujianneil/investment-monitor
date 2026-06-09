"""
data_sources.py — 多源行情获取框架（带 fallback 和冷却）

设计原则:
  1. 任何"必败"的接口（东方财富国内代理被封）放到 fallback 链尾部
  2. 新浪 hq.sinajs.cn 是 A 股最稳定的源（HTTP 明文，无需 API Key，2026-06-09 实测）
  3. 失败重试有冷却（同一源 10 分钟内不再尝试，避免拖慢主循环）
  4. 进程内记录每个源的成功/失败次数，供健康检查使用

数据源优先级（A 股实时行情）:
  Tier 1: 新浪 hq.sinajs.cn — 稳定，已验证
  Tier 2: akshare.stock_zh_a_spot（新浪版）— 偶尔失败
  Tier 3: akshare.stock_zh_a_spot_em（东方财富）— 国内代理常被封

数据源优先级（港股/美股）: yfinance（已有逻辑，不改）
"""
import requests
import time
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# ── 源健康度跟踪（进程内）───────────────────────────────────
# {source_name: {"ok": int, "fail": int, "last_fail": datetime, "last_ok": datetime, "cooldown_until": datetime}}
_source_stats = {}
_COOLDOWN_SECONDS = 600  # 失败后 10 分钟内不再尝试


def _record_success(source):
    s = _source_stats.setdefault(source, {"ok": 0, "fail": 0, "last_fail": None, "last_ok": None, "cooldown_until": None})
    s["ok"] += 1
    s["last_ok"] = datetime.now()
    s["cooldown_until"] = None


def _record_failure(source):
    s = _source_stats.setdefault(source, {"ok": 0, "fail": 0, "last_fail": None, "last_ok": None, "cooldown_until": None})
    s["fail"] += 1
    s["last_fail"] = datetime.now()
    s["cooldown_until"] = datetime.now() + timedelta(seconds=_COOLDOWN_SECONDS)


def _in_cooldown(source):
    s = _source_stats.get(source)
    if not s or not s.get("cooldown_until"):
        return False
    return datetime.now() < s["cooldown_until"]


def get_source_health():
    """返回所有源的当前健康状态（供健康检查任务使用）"""
    out = {}
    for name, stats in _source_stats.items():
        out[name] = {
            **stats,
            "cooldown_until": stats.get("cooldown_until").isoformat() if stats.get("cooldown_until") else None,
            "in_cooldown": _in_cooldown(name),
        }
    return out


# ── Tier 1: 新浪 hq.sinajs.cn（核心稳定源）────────────────────
def _sina_cn_quote(symbols_with_prefix):
    """
    新浪批量获取 A 股实时行情

    参数: symbols_with_prefix — ['sh600036', 'sz002156']
    返回: {'sh600036': {price, change_pct, volume, name}, ...}
    """
    if not symbols_with_prefix:
        return {}

    url = f"https://hq.sinajs.cn/list={','.join(symbols_with_prefix)}"
    headers = {
        "Referer": "https://finance.sina.com.cn",  # 必需，否则返回乱码
        "User-Agent": "Mozilla/5.0",
    }
    r = requests.get(url, headers=headers, timeout=10)
    r.encoding = "gbk"
    text = r.text

    results = {}
    for line in text.strip().split("\n"):
        if "=" not in line or '""' in line:
            continue
        try:
            var_part, val_part = line.split("=", 1)
            sym = var_part.strip().split("_")[-1]
            fields = val_part.strip().strip(';').strip('"').split(",")
            if len(fields) < 32:
                continue
            name = fields[0]
            prev_close = float(fields[2])
            price = float(fields[3])
            volume = int(fields[8]) if fields[8].isdigit() else 0
            change_pct = (price - prev_close) / prev_close if prev_close else 0
            results[sym] = {
                "price": price,
                "change_pct": change_pct,
                "volume": volume,
                "name": name,
            }
        except (ValueError, IndexError) as e:
            logger.warning(f"  [sina] parse error: {line[:80]} err={e}")
            continue

    return results


# ── Tier 2: akshare 新浪源 ────────────────────────────────────
def _akshare_sina_cn(symbols_6digit):
    """akshare.stock_zh_a_spot — 新浪版全市场行情"""
    import akshare as ak
    df = ak.stock_zh_a_spot()
    results = {}
    for sym in symbols_6digit:
        row = df[df["代码"] == sym]
        if not row.empty:
            results[sym] = {
                "price": float(row["最新价"].values[0]),
                "change_pct": float(row["涨跌幅"].values[0]) / 100,
                "volume": int(row["成交量"].values[0]) if str(row["成交量"].values[0]).isdigit() else 0,
                "name": str(row["名称"].values[0]),
            }
    return results


# ── Tier 3: akshare 东方财富源（最后兜底）────────────────────
def _akshare_em_cn(symbols_6digit):
    """akshare.stock_zh_a_spot_em — 东方财富，国内代理经常被封"""
    import akshare as ak
    df = ak.stock_zh_a_spot_em()
    results = {}
    for sym in symbols_6digit:
        row = df[df["代码"] == sym]
        if not row.empty:
            results[sym] = {
                "price": float(row["最新价"].values[0]),
                "change_pct": float(row["涨跌幅"].values[0]) / 100,
                "volume": int(row["成交量"].values[0]) if str(row["成交量"].values[0]).isdigit() else 0,
                "name": str(row["名称"].values[0]),
            }
    return results


# ── 主入口 ─────────────────────────────────────────────
def get_cn_quotes(symbols_6digit):
    """
    批量获取 A 股行情，按优先级尝试多个数据源

    参数: symbols_6digit — ['600036', '002156']
    返回: dict {symbol: {price, change_pct, volume, name}}
          所有源都失败时返回空 dict
    """
    if not symbols_6digit:
        return {}

    sources = [
        ("sina_hq", _to_sina_keys, _from_sina_keys),
        ("akshare_sina", lambda s: s, lambda r: r),
        ("akshare_em", lambda s: s, lambda r: r),
    ]

    last_error = None
    for source_name, to_keys, from_keys in sources:
        if _in_cooldown(source_name):
            continue

        try:
            keys = to_keys(symbols_6digit)
            if source_name == "sina_hq":
                raw = _sina_cn_quote(keys)
            elif source_name == "akshare_sina":
                raw = _akshare_sina_cn(keys)
            else:
                raw = _akshare_em_cn(keys)

            if raw:
                _record_success(source_name)
                results = from_keys(raw)
                logger.info(f"  [data_sources] {source_name} 成功 {len(results)}/{len(symbols_6digit)}")
                return results
            else:
                _record_failure(source_name)
                logger.warning(f"  [data_sources] {source_name} 返回空")
                continue
        except Exception as e:
            _record_failure(source_name)
            last_error = e
            logger.warning(f"  [data_sources] {source_name} 失败: {type(e).__name__}: {str(e)[:80]}")
            continue

    logger.error(f"  [data_sources] 所有 A 股源都失败。最后错误: {last_error}")
    return {}


def _to_sina_keys(symbols):
    """['600036', '002156'] -> ['sh600036', 'sz002156']"""
    out = []
    for s in symbols:
        if s.startswith(("5", "6", "9")):
            out.append(f"sh{s}")
        else:
            out.append(f"sz{s}")
    return out


def _from_sina_keys(results_with_prefix):
    """{'sh600036': {...}} -> {'600036': {...}}"""
    return {k.replace("sh", "").replace("sz", ""): v for k, v in results_with_prefix.items()}


# ── 健康检查（供调度器定时调用）──────────────────────────
def health_check(symbols_to_test=None):
    """
    检查所有数据源，返回状态。供 scheduler 调用并发送告警
    """
    if symbols_to_test is None:
        symbols_to_test = ["600036", "002156"]

    results = {}
    for source_name in ("sina_hq", "akshare_sina", "akshare_em"):
        if _in_cooldown(source_name):
            results[source_name] = {"ok": False, "in_cooldown": True}
            continue
        try:
            t0 = time.time()
            if source_name == "sina_hq":
                r = _sina_cn_quote(_to_sina_keys(symbols_to_test))
            elif source_name == "akshare_sina":
                r = _akshare_sina_cn(symbols_to_test)
            else:
                r = _akshare_em_cn(symbols_to_test)
            results[source_name] = {
                "ok": bool(r),
                "count": len(r),
                "time_ms": int((time.time() - t0) * 1000),
            }
        except Exception as e:
            results[source_name] = {"ok": False, "error": str(e)[:100]}
    return results