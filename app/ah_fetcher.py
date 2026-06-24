"""
ah_fetcher.py — AH 溢价指数 + 南向资金 + USDHKD 三源数据 fetcher

设计原则（2026-06-22）：
  - 双源冗余：每个指标主源 + 备源，主源失败自动切备源
  - 失败可观察：失败次数累加，超过阈值触发告警
  - 不依赖 akshare 历史接口：所有日级数据自维护 SQLite，逐日累积
  - 不破坏现有 investment-monitor 体系：复用 DB_PATH / config.py

被调用方：
  ah_analyzer.py (读取 → 计算信号)
  ah_render.py   (读取 → 渲染卡片/HTML)
"""

import os
import time
import sqlite3
import logging
from datetime import datetime, timedelta, date
from typing import Optional

import requests
import yfinance as yf
import pandas as pd

from config import DB_PATH

logger = logging.getLogger("ah_fetcher")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")


# ============================================================
# HTTP 通用：UA + Referer + 短超时 + 重试
# ============================================================
_EM_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": "https://quote.eastmoney.com/",
    "Accept": "application/json,text/plain,*/*",
}
_REQUEST_TIMEOUT = 12
_MAX_RETRIES = 3
_BACKOFF = 1.5


def _http_get(url: str, params: dict = None, headers: dict = None, timeout: int = _REQUEST_TIMEOUT) -> Optional[requests.Response]:
    """带指数退避的 GET，单源层不做 fallback（交给各 fetcher）。"""
    h = {**_EM_HEADERS, **(headers or {})}
    last_err = None
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, headers=h, timeout=timeout)
            if r.status_code == 200:
                return r
            last_err = f"HTTP {r.status_code}: {r.text[:120]}"
        except requests.RequestException as e:
            last_err = f"{type(e).__name__}: {str(e)[:120]}"
        if attempt < _MAX_RETRIES:
            time.sleep(_BACKOFF ** attempt)
    logger.warning(f"_http_get 失败 {url} err={last_err}")
    return None


# ============================================================
# 数据库表：ah_premium / southbound / usdhkd + 元数据
# ============================================================
def ensure_tables() -> None:
    """建表 + 迁移。幂等可重入。"""
    with sqlite3.connect(DB_PATH) as c:
        c.executescript(
            """
            CREATE TABLE IF NOT EXISTS ah_premium (
                date TEXT PRIMARY KEY,
                value REAL NOT NULL,           -- 恒生 AH 溢价指数 (HSCAHPI)
                source TEXT NOT NULL,          -- eastmoney / hsi / hkex
                fetched_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS southbound (
                date TEXT PRIMARY KEY,
                sh_net REAL,                   -- 港股通(沪) 当日成交净买额 (亿港元)
                sz_net REAL,                   -- 港股通(深) 当日成交净买额 (亿港元)
                total_net REAL,                -- 沪+深 合计
                sh_balance REAL,               -- 沪 资金余额
                sz_balance REAL,               -- 深 资金余额
                source TEXT,
                fetched_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS usdhkd (
                date TEXT PRIMARY KEY,
                close REAL NOT NULL,
                source TEXT NOT NULL,
                fetched_at TEXT NOT NULL
            );

            -- fetcher 失败计数（>3 触发告警，由 watchdog 读）
            CREATE TABLE IF NOT EXISTS ah_source_health (
                source_key TEXT PRIMARY KEY,   -- e.g. ah_premium:hkex
                fail_count INTEGER DEFAULT 0,
                last_success TEXT,
                last_fail TEXT,
                last_error TEXT
            );
            """
        )


def _record_success(key: str):
    with sqlite3.connect(DB_PATH) as c:
        c.execute(
            "INSERT INTO ah_source_health(source_key, fail_count, last_success) VALUES(?,0,?) "
            "ON CONFLICT(source_key) DO UPDATE SET fail_count=0, last_success=excluded.last_success, last_error=NULL",
            (key, datetime.now().isoformat()),
        )


def _record_fail(key: str, err: str):
    with sqlite3.connect(DB_PATH) as c:
        c.execute(
            "INSERT INTO ah_source_health(source_key, fail_count, last_fail, last_error) VALUES(?,1,?,?) "
            "ON CONFLICT(source_key) DO UPDATE SET fail_count=fail_count+1, last_fail=excluded.last_fail, last_error=excluded.last_error",
            (key, datetime.now().isoformat(), err[:300]),
        )


def source_health_summary() -> dict:
    """给 watchdog / 卡片使用。返回 {key: {fail_count, last_success, ...}}"""
    with sqlite3.connect(DB_PATH) as c:
        c.row_factory = sqlite3.Row
        rows = c.execute("SELECT * FROM ah_source_health").fetchall()
    return {r["source_key"]: dict(r) for r in rows}


# ============================================================
# 1) 恒生 AH 溢价指数 — 双源
#    主源: 东财 push2 (secid=124.HSCAHP, 港股指数段)
#    备源: yfinance HSI / HSCAHP 相关 ETF, fallback 用 HSI.HK 反推
#    注: 精确的 HSCAHPI 在 yfinance 上 ticker 是 'HSCAHP.HI'
# ============================================================
def fetch_ah_premium(target_date: Optional[str] = None) -> Optional[float]:
    """
    返回当日 HSAHP 等价值（恒生沪深港通 AH 股溢价指数）。

    v1.3 修复（2026-06-22 P0）：
      原主源 2801.HK (iShares MSCI China) 实际是港股大盘 ETF，与 AH 溢价无关，
      会把"港股跌"误读为"溢价收敛"。现已移除。

    新主源：用 AH 双重上市大盘股自算加权溢价率（恒生 HSAHP 指数本质就是这个）。
    新备源：akshare stock_zh_a_spot + stock_hk_spot 逐股配对。
    """
    ensure_tables()

    # 主源: yfinance A/H 双边行情自算
    val = _fetch_ah_premium_calc(target_date)
    if val is not None:
        _record_success("ah_premium:yfinance_calc")
        _upsert_ah(target_date or datetime.now().strftime("%Y-%m-%d"), val, "yfinance_calc")
        return val

    # 备源: akshare A/H 双边行情
    val = _fetch_ah_premium_akshare(target_date)
    if val is not None:
        _record_success("ah_premium:akshare_calc")
        _upsert_ah(target_date or datetime.now().strftime("%Y-%m-%d"), val, "akshare_calc")
        return val

    _record_fail("ah_premium:all", "yfinance 自算 + akshare 自算都失败")
    return None


def _fetch_ah_premium_yfinance(target_date: Optional[str]) -> Optional[float]:
    """
    旧版本保留但不再调用（2026-06-22 P0 修复后弃用）。
    原逻辑用 2801.HK ETF 数据，但 2801.HK 是 iShares MSCI China ETF
    而非 AH 溢价指数——会导致看板读错数据。
    """
    logger.warning("deprecated: _fetch_ah_premium_yfinance (2801.HK) — 请改用自算")
    return None


def _fetch_ah_premium_calc(target_date: Optional[str]) -> Optional[float]:
    """
    主源：用 AH 双重上市大盘股 (A 价 / H 价 × USDHKD 折算) 的等权平均溢价率，
    还原成 HSAHP 指数点位形式 = 100 + 平均溢价率×100。

    HSAHP 真实算法（市值加权），我们用等权简化——对方向感信号足够。
    """
    from config import AH_DUAL_LISTED
    pairs = [(c, n, h) for c, n, h in AH_DUAL_LISTED if not h.endswith("_alt")]
    if not pairs:
        return None
    a_syms = [f"{c}.SS" if c.startswith("6") else f"{c}.SZ" for c, _, _ in pairs]
    h_syms = [f"{h}.HK" for _, _, h in pairs]

    try:
        end = pd.Timestamp(target_date) + timedelta(days=1) if target_date else pd.Timestamp.now()
        start = end - timedelta(days=15)
        a_data = yf.download(a_syms, start=start.strftime("%Y-%m-%d"),
                             end=end.strftime("%Y-%m-%d"),
                             auto_adjust=False, progress=False)["Close"]
        h_data = yf.download(h_syms, start=start.strftime("%Y-%m-%d"),
                             end=end.strftime("%Y-%m-%d"),
                             auto_adjust=False, progress=False)["Close"]
        if a_data is None or h_data is None or a_data.empty or h_data.empty:
            return None
        # USDHKD 折算 HKD → CNY（关键：用正确折算率 USDCNY/USDHKD）
        cny_per_hkd = _cny_per_hkd(target_date)
        if cny_per_hkd is None or cny_per_hkd <= 0:
            return None
        # 按列名对齐（yf.download 多标的会按 ticker 字母排序，不能用 iloc）
        premiums = []
        weights = []
        for a_code, name, h_code in pairs:
            a_sym = f"{a_code}.SS" if a_code.startswith("6") else f"{a_code}.SZ"
            h_sym = f"{h_code}.HK"
            if a_sym not in a_data.columns or h_sym not in h_data.columns:
                continue
            a_price = a_data[a_sym].dropna()
            h_price = h_data[h_sym].dropna()
            if a_price.empty or h_price.empty:
                continue
            ap = float(a_price.iloc[-1])
            hp = float(h_price.iloc[-1])
            if ap and hp and hp > 0:
                h_cny = hp * cny_per_hkd
                prem_rate = (ap / h_cny) - 1.0
                premiums.append(prem_rate)
                weights.append(1.0)
        if not premiums:
            return None
        avg_premium = sum(premiums) / sum(weights)
        # v1.3: 标准化到 HSAHP 真实数量级 (100-160 区间)
        # 真实 HSAHP 120.37 = 100 + 20.37% 平均溢价
        return round(100 + avg_premium * 100, 2)
    except Exception as e:
        logger.warning(f"yfinance calc ah_premium fail: {e}")
        return None


def _fetch_ah_premium_akshare(target_date: Optional[str]) -> Optional[float]:
    """
    备源：akshare stock_zh_a_spot + stock_hk_spot_em 自算。
    港股 spot 接口偶尔不稳，所以是备源。
    """
    from config import AH_DUAL_LISTED
    try:
        import akshare as ak
        df_a = ak.stock_zh_a_spot()
        df_h = ak.stock_hk_spot_em()
        if df_a is None or df_h is None or df_a.empty or df_h.empty:
            return None
        cny_per_hkd = _cny_per_hkd(target_date)
        if cny_per_hkd is None or cny_per_hkd <= 0:
            cny_per_hkd = 0.92  # fallback
        a_map = {str(r["代码"]).strip(): float(r["最新价"]) for _, r in df_a.iterrows()}
        h_map = {str(r["代码"]).strip(): float(r["最新价"]) for _, r in df_h.iterrows()}
        premiums = []
        for a_code, _name, h_code in AH_DUAL_LISTED:
            if h_code.endswith("_alt"):
                continue
            # A: 600xxx, H: 0xxxx (5 位)
            h_key = h_code.zfill(5)  # '3968' → '03968'
            a_p = a_map.get(a_code)
            h_p = h_map.get(h_key)
            if a_p and h_p and h_p > 0:
                h_cny = h_p * cny_per_hkd
                prem_rate = (a_p / h_cny) - 1.0
                premiums.append(prem_rate)
        if not premiums:
            return None
        avg_premium = sum(premiums) / len(premiums)
        return round(100 + avg_premium * 100, 2)
    except Exception as e:
        logger.warning(f"akshare calc ah_premium fail: {e}")
        return None


def _upsert_ah(d: str, value: float, source: str):
    with sqlite3.connect(DB_PATH) as c:
        c.execute(
            "INSERT INTO ah_premium(date,value,source,fetched_at) VALUES(?,?,?,?) "
            "ON CONFLICT(date) DO UPDATE SET value=excluded.value, source=excluded.source, fetched_at=excluded.fetched_at",
            (d, value, source, datetime.now().isoformat()),
        )


def fetch_ah_premium_history(days: int = 365) -> pd.DataFrame:
    """补齐历史。v1.3: 改用自算逻辑（而非 2801.HK）。"""
    ensure_tables()
    try:
        from config import AH_DUAL_LISTED
        pairs = [(c, n, h) for c, n, h in AH_DUAL_LISTED if not h.endswith("_alt")]
        if not pairs:
            return pd.DataFrame(columns=["date", "value"])
        a_syms = [f"{c}.SS" if c.startswith("6") else f"{c}.SZ" for c, _, _ in pairs]
        h_syms = [f"{h}.HK" for _, _, h in pairs]
        end = pd.Timestamp.now()
        start = end - timedelta(days=days + 10)
        a_data = yf.download(a_syms, start=start.strftime("%Y-%m-%d"),
                             end=end.strftime("%Y-%m-%d"),
                             auto_adjust=False, progress=False)["Close"]
        h_data = yf.download(h_syms, start=start.strftime("%Y-%m-%d"),
                             end=end.strftime("%Y-%m-%d"),
                             auto_adjust=False, progress=False)["Close"]
        if a_data is None or h_data is None or a_data.empty or h_data.empty:
            return pd.DataFrame(columns=["date", "value"])
        # USDHKD 历史
        usdhkd_hist = yf.Ticker("HKD=X").history(period=f"{days+10}d")["Close"]
        usdcny_hist = yf.Ticker("CNY=X").history(period=f"{days+10}d")["Close"]
        if usdhkd_hist is None or usdcny_hist is None or usdhkd_hist.empty or usdcny_hist.empty:
            return pd.DataFrame(columns=["date", "value"])
        # 关键修复：去 tz，HKD=X yfinance 返回 Europe/London tz，A/H 是 tz-naive
        for s in [usdhkd_hist, usdcny_hist]:
            if s.index.tz is not None:
                s_new = s.tz_localize(None)
                s.__class__.__init__(s, data=s_new.values, index=s_new.index)
        # 按日迭代：每日算一次自算 HSAHP
        common_dates = a_data.index.intersection(h_data.index).intersection(usdhkd_hist.index).intersection(usdcny_hist.index)
        common_dates = common_dates[common_dates >= (pd.Timestamp.now() - timedelta(days=days))]
        for d in common_dates:
            row_fx_h = usdhkd_hist.loc[d]
            row_fx_c = usdcny_hist.loc[d]
            if pd.isna(row_fx_h) or pd.isna(row_fx_c) or row_fx_h <= 0 or row_fx_c <= 0:
                continue
            cny_per_hkd = float(row_fx_c) / float(row_fx_h)
            premiums = []
            for a_code, _name, h_code in pairs:
                a_sym = f"{a_code}.SS" if a_code.startswith("6") else f"{a_code}.SZ"
                h_sym = f"{h_code}.HK"
                if a_sym not in a_data.columns or h_sym not in h_data.columns:
                    continue
                ap_v = a_data[a_sym].loc[d] if d in a_data.index else None
                hp_v = h_data[h_sym].loc[d] if d in h_data.index else None
                if pd.isna(ap_v) or pd.isna(hp_v) or hp_v <= 0:
                    continue
                h_cny = float(hp_v) * cny_per_hkd
                premiums.append((float(ap_v) / h_cny) - 1.0)
            if premiums:
                avg_p = sum(premiums) / len(premiums)
                val = round(100 + avg_p * 100, 2)
                _upsert_ah(d.strftime("%Y-%m-%d"), val, "yfinance_calc_history")
    except Exception as e:
        logger.warning(f"ah_premium history fail: {e}")
    return pd.read_sql_query(
        "SELECT date, value FROM ah_premium ORDER BY date", sqlite3.connect(DB_PATH), parse_dates=["date"]
    )


# ============================================================
# 2) 南向资金（沪 + 深）
#    主源: 东财 push2 港股通(沪) + 港股通(深) (secid 用 m:90+t:2 类)
#    备源: akshare stock_hsgt_fund_flow_summary_em (timeout 8s)
#    实测: 东财 interface 不稳时 akshare 仍可用
# ============================================================
def fetch_southbound(target_date: Optional[str] = None) -> Optional[dict]:
    """返回 {'sh_net': float, 'sz_net': float, 'total_net': float}，单位亿元。"""
    ensure_tables()
    res = _fetch_southbound_eastmoney(target_date)
    if res:
        _record_success("southbound:eastmoney")
    else:
        res = _fetch_southbound_akshare(target_date)
        if res:
            _record_success("southbound:akshare")
        else:
            _record_fail("southbound:all", "eastmoney + akshare 都失败")
            return None
    if res:
        d = target_date or datetime.now().strftime("%Y-%m-%d")
        _upsert_southbound(
            d,
            res.get("sh_net"),
            res.get("sz_net"),
            res.get("total_net"),
            res.get("sh_balance"),
            res.get("sz_balance"),
            res.get("source", "unknown"),
        )
    return res


def _fetch_southbound_eastmoney(target_date: Optional[str]) -> Optional[dict]:
    """东财接口：港股通(沪) 1.HSGTL，港股通(深) 1.HSGTR/2.HSGTH。secid 段 124=港股，代码段 09301/09302 类。"""
    # 用东财行情接口 secid=124.09301 / 124.09302 （南向沪 / 南向深）
    # 字段 f43 最新价（这里其实是个统计值，单位亿元）
    try:
        r_sh = _http_get(
            "https://push2.eastmoney.com/api/qt/stock/get",
            params={"secid": "124.09301", "fields": "f43,f44,f60,f170", "invt": 2, "fltt": 2},
        )
        r_sz = _http_get(
            "https://push2.eastmoney.com/api/qt/stock/get",
            params={"secid": "124.09302", "fields": "f43,f44,f60,f170", "invt": 2, "fltt": 2},
        )
        if not r_sh or not r_sz:
            return None
        d_sh = r_sh.json().get("data") or {}
        d_sz = r_sz.json().get("data") or {}
        sh_net = d_sh.get("f43")
        sz_net = d_sz.get("f43")
        if sh_net is None or sz_net is None:
            return None
        return {
            "sh_net": float(sh_net),
            "sz_net": float(sz_net),
            "total_net": float(sh_net) + float(sz_net),
            "sh_balance": d_sh.get("f60"),
            "sz_balance": d_sz.get("f60"),
            "source": "eastmoney",
        }
    except Exception as e:
        logger.warning(f"eastmoney southbound fail: {e}")
        return None


def _fetch_southbound_akshare(target_date: Optional[str]) -> Optional[dict]:
    """akshare 兜底。注意 stock_hsgt_fund_flow_summary_em 返回最新一日的 4 行 (沪北/沪南/深北/深南)。"""
    try:
        import akshare as ak
        df = ak.stock_hsgt_fund_flow_summary_em()
        if df is None or df.empty:
            return None
        # 找南向两行
        sh_row = df[(df["板块"] == "港股通(沪)") & (df["资金方向"] == "南向")]
        sz_row = df[(df["板块"] == "港股通(深)") & (df["资金方向"] == "南向")]
        if sh_row.empty or sz_row.empty:
            return None
        sh_net = float(sh_row.iloc[0]["成交净买额"])
        sz_net = float(sz_row.iloc[0]["成交净买额"])
        sh_bal = float(sh_row.iloc[0]["当日资金余额"])
        sz_bal = float(sz_row.iloc[0]["当日资金余额"])
        return {
            "sh_net": sh_net,
            "sz_net": sz_net,
            "total_net": sh_net + sz_net,
            "sh_balance": sh_bal,
            "sz_balance": sz_bal,
            "source": "akshare",
        }
    except Exception as e:
        logger.warning(f"akshare southbound fail: {e}")
        return None


def _upsert_southbound(d, sh_net, sz_net, total_net, sh_balance, sz_balance, source):
    with sqlite3.connect(DB_PATH) as c:
        c.execute(
            "INSERT INTO southbound(date,sh_net,sz_net,total_net,sh_balance,sz_balance,source,fetched_at) "
            "VALUES(?,?,?,?,?,?,?,?) "
            "ON CONFLICT(date) DO UPDATE SET sh_net=excluded.sh_net, sz_net=excluded.sz_net, total_net=excluded.total_net, "
            "sh_balance=excluded.sh_balance, sz_balance=excluded.sz_balance, source=excluded.source, fetched_at=excluded.fetched_at",
            (d, sh_net, sz_net, total_net, sh_balance, sz_balance, source, datetime.now().isoformat()),
        )


def fetch_southbound_history(days: int = 365) -> pd.DataFrame:
    """akshare 历史接口不稳定 → 用我们的日级累积 + 一次性回填。"""
    ensure_tables()
    # 触发回填（一次性）
    try:
        import akshare as ak
        # 沪股通北向 / 深股通北向 / 港股通南向（沪） / 港股通南向（深）
        # stock_hsgt_hist_em(symbol='南向资金') 历史会拉沪深合计
        # 这里只取最近 days 天，分两次 (沪/深)
        for sym in ["港股通", "南向资金"]:
            try:
                df = ak.stock_hsgt_hist_em(symbol=sym)
                if df is None or df.empty:
                    continue
                # 不同 sym 返回的列不同，统一列名
                df = df.rename(columns={"日期": "date", "当日成交净买额": "net"})
                # 只取最后 days 天
                df["date"] = pd.to_datetime(df["date"])
                df = df[df["date"] >= (datetime.now() - timedelta(days=days))]
                # 沪 / 深 区分：靠 symbol 区分
                # 略：hist 接口粒度不够，先只存南向总计列
                for _, row in df.iterrows():
                    d = row["date"].strftime("%Y-%m-%d")
                    with sqlite3.connect(DB_PATH) as c:
                        c.execute(
                            "INSERT INTO southbound(date,total_net,source,fetched_at) VALUES(?,?,?,?) "
                            "ON CONFLICT(date) DO UPDATE SET total_net=COALESCE(excluded.total_net, southbound.total_net), "
                            "source=excluded.source, fetched_at=excluded.fetched_at",
                            (d, float(row["net"]) if pd.notna(row["net"]) else None, f"akshare:{sym}", datetime.now().isoformat()),
                        )
            except Exception as e:
                logger.warning(f"south history {sym} fail: {e}")
    except Exception as e:
        logger.warning(f"south history overall fail: {e}")
    return pd.read_sql_query(
        "SELECT * FROM southbound ORDER BY date", sqlite3.connect(DB_PATH), parse_dates=["date"]
    )


# ============================================================
# 3) USDHKD — 港元强弱（弱方保证 7.85）
#    主源: yfinance HKD=X (官方)
#    备源: akshare fx_spot_quote 反算 USD/CNY ÷ HKD/CNY
# ============================================================
def fetch_usdhkd(target_date: Optional[str] = None) -> Optional[float]:
    ensure_tables()
    val = _fetch_usdhkd_yfinance(target_date)
    if val is not None:
        _record_success("usdhkd:yfinance")
        _upsert_usdhkd(target_date or datetime.now().strftime("%Y-%m-%d"), val, "yfinance")
        return val
    val = _fetch_usdhkd_akshare()
    if val is not None:
        _record_success("usdhkd:akshare")
        _upsert_usdhkd(target_date or datetime.now().strftime("%Y-%m-%d"), val, "akshare")
        return val
    _record_fail("usdhkd:all", "yfinance + akshare 都失败")
    return None


def _fetch_usdhkd_yfinance(target_date: Optional[str] = None) -> Optional[float]:
    try:
        tk = yf.Ticker("HKD=X")
        if target_date:
            start = (pd.Timestamp(target_date) - timedelta(days=5)).strftime("%Y-%m-%d")
            end = (pd.Timestamp(target_date) + timedelta(days=1)).strftime("%Y-%m-%d")
            hist = tk.history(start=start, end=end, auto_adjust=False)
        else:
            hist = tk.history(period="5d", auto_adjust=False)
        if hist is None or hist.empty:
            return None
        close = hist["Close"]
        # 确保是 scalar（yfinance 有时返回 DataFrame）
        if hasattr(close, 'iloc'):
            v = close.iloc[-1]
            if hasattr(v, 'iloc'):
                v = v.iloc[-1]
            return float(v)
        return float(close)
    except Exception as e:
        logger.warning(f"yfinance HKD=X fail: {e}")
        return None


def _fetch_usdcny(target_date: Optional[str] = None) -> Optional[float]:
    """
    拿 USD/CNY 汇率。yfinance 'CNY=X'。
    注意：CNY/HKD = (USD/CNY) / (USD/HKD)，不是 1/USDHKD。
    """
    try:
        tk = yf.Ticker("CNY=X")
        if target_date:
            start = (pd.Timestamp(target_date) - timedelta(days=5)).strftime("%Y-%m-%d")
            end = (pd.Timestamp(target_date) + timedelta(days=1)).strftime("%Y-%m-%d")
            hist = tk.history(start=start, end=end, auto_adjust=False)
        else:
            hist = tk.history(period="5d", auto_adjust=False)
        if hist is None or hist.empty:
            return None
        close = hist["Close"]
        if hasattr(close, 'iloc'):
            v = close.iloc[-1]
            if hasattr(v, 'iloc'):
                v = v.iloc[-1]
            return float(v)
        return float(close)
    except Exception as e:
        logger.warning(f"yfinance CNY=X fail: {e}")
        return None


def _cny_per_hkd(target_date: Optional[str] = None) -> Optional[float]:
    """
    正确的 CNY/HKD 折算率 = USDCNY / USDHKD
    例：USDCNY=7.25, USDHKD=7.84 → CNY/HKD = 0.9247 (1 HKD = 0.9247 CNY)
    """
    usdcny = _fetch_usdcny(target_date)
    usdhkd = _fetch_usdhkd_yfinance(target_date)
    if usdcny is None or usdhkd is None or usdhkd <= 0:
        return None
    return usdcny / usdhkd


def _fetch_usdhkd_akshare() -> Optional[float]:
    try:
        import akshare as ak
        df = ak.fx_spot_quote()
        if df is None or df.empty:
            return None
        # cols: ['货币对', '买报价', '卖报价']
        usd_cny = df[df["货币对"] == "USD/CNY"]
        hkd_cny = df[df["货币对"] == "HKD/CNY"]
        if usd_cny.empty or hkd_cny.empty:
            return None
        usd = float((usd_cny["买报价"].iloc[0] + usd_cny["卖报价"].iloc[0]) / 2)
        hkd = float((hkd_cny["买报价"].iloc[0] + hkd_cny["卖报价"].iloc[0]) / 2)
        if hkd == 0:
            return None
        return round(usd / hkd, 5)
    except Exception as e:
        logger.warning(f"akshare USDHKD fail: {e}")
        return None


def _upsert_usdhkd(d: str, value: float, source: str):
    with sqlite3.connect(DB_PATH) as c:
        c.execute(
            "INSERT INTO usdhkd(date,close,source,fetched_at) VALUES(?,?,?,?) "
            "ON CONFLICT(date) DO UPDATE SET close=excluded.close, source=excluded.source, fetched_at=excluded.fetched_at",
            (d, value, source, datetime.now().isoformat()),
        )


def fetch_usdhkd_history(days: int = 365) -> pd.DataFrame:
    ensure_tables()
    try:
        tk = yf.Ticker("HKD=X")
        hist = tk.history(period=f"{days}d", auto_adjust=False)
        if hist is not None and not hist.empty:
            for d, row in hist.iterrows():
                _upsert_usdhkd(d.strftime("%Y-%m-%d"), float(row["Close"]), "yfinance_history")
    except Exception as e:
        logger.warning(f"usdhkd history fail: {e}")
    return pd.read_sql_query(
        "SELECT date, close FROM usdhkd ORDER BY date", sqlite3.connect(DB_PATH), parse_dates=["date"]
    )


# ============================================================
# 一站式 run_all
# ============================================================
def run_all(refresh_history: bool = False) -> dict:
    """每日 cron 调用。返回三表最新一行的 dict。"""
    ensure_tables()
    out = {}
    out["ah_premium"] = fetch_ah_premium()
    out["southbound"] = fetch_southbound()
    out["usdhkd"] = fetch_usdhkd()
    if refresh_history:
        fetch_ah_premium_history(365)
        fetch_southbound_history(365)
        fetch_usdhkd_history(365)
    return out


if __name__ == "__main__":
    # CLI: python3 ah_fetcher.py
    import sys
    refresh = "--history" in sys.argv
    print("=== AH Premium + Southbound + USDHKD 采集 ===")
    result = run_all(refresh_history=refresh)
    print(json_result := {k: v for k, v in result.items()})
    print("\n=== source health ===")
    import json as _j
    print(_j.dumps(source_health_summary(), indent=2, ensure_ascii=False))
