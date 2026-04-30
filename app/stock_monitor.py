"""
stock_monitor.py — 价格监控

新哲学：不是"看价格"，是检测两件事：
1. FCF 估值阈值是否被触发（买入/卖出纪律线）
2. 异常波动（单日 ±5%）是否需要去看新闻（提示，不是交易信号）

EXIT_PENDING 的持仓不产生任何警报。
"""
import akshare as ak
import yfinance as yf
from datetime import datetime
from config import PORTFOLIO
from models import get_last_alert_time, save_alert, save_price
from feishu_push import send_anomaly_alert, send_fcf_threshold_alert


# ── 价格获取 ──────────────────────────────────────────────

def get_cn_price(symbol):
    try:
        df = ak.stock_zh_a_spot_em()
        row = df[df['代码'] == symbol]
        if not row.empty:
            return {
                'price': float(row['最新价'].values[0]),
                'change_pct': float(row['涨跌幅'].values[0]) / 100,
                'volume': int(row['成交量'].values[0]),
            }
    except Exception as e:
        print(f"  [价格] 获取A股 {symbol} 失败: {e}")
    return None


def get_hk_us_price(yf_symbol):
    try:
        ticker = yf.Ticker(yf_symbol)
        hist = ticker.history(period='2d')
        if len(hist) >= 2:
            cur = float(hist['Close'].iloc[-1])
            prev = float(hist['Close'].iloc[-2])
            return {
                'price': cur,
                'change_pct': (cur - prev) / prev,
                'volume': 0,
            }
    except Exception as e:
        print(f"  [价格] 获取 {yf_symbol} 失败: {e}")
    return None


def get_price(symbol, cfg):
    market = cfg['market']
    if market == 'CN':
        return get_cn_price(cfg.get('akshare_symbol', symbol))
    else:
        return get_hk_us_price(cfg.get('yf_symbol', symbol))


# ── FCF 倍数计算 ──────────────────────────────────────────

def get_fcf_multiple(yf_symbol, current_price=None):
    """计算 FCF 倍数。仅对有 yf_symbol 的持仓有效。"""
    try:
        ticker = yf.Ticker(yf_symbol)
        info = ticker.info
        market_cap = info.get('marketCap')
        cashflow = ticker.cashflow

        if cashflow is None or cashflow.empty or not market_cap:
            return None

        op_cf = None
        capex = None
        if 'Operating Cash Flow' in cashflow.index:
            op_cf = cashflow.loc['Operating Cash Flow'].iloc[0]
        if 'Capital Expenditure' in cashflow.index:
            capex = cashflow.loc['Capital Expenditure'].iloc[0]

        if op_cf is not None and capex is not None:
            fcf = op_cf + capex   # capex 通常为负数
            if fcf > 0:
                return round(market_cap / fcf, 1)
    except Exception as e:
        print(f"  [FCF] 计算 {yf_symbol} 失败: {e}")
    return None


# ── 核心监控逻辑 ──────────────────────────────────────────

def monitor_stocks():
    print(f"\n{'='*55}")
    print(f"  价格 & FCF 监控 — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*55}")

    anomaly_count = 0
    fcf_count = 0

    for symbol, cfg in PORTFOLIO.items():
        name = cfg['name']

        # EXIT_PENDING：完全跳过
        if cfg['monitor_type'] == 'EXIT_PENDING':
            continue

        # ── 1. 获取当前价格 ──
        data = get_price(symbol, cfg)
        if not data:
            print(f"  {name}({symbol}): 无法获取价格")
            continue

        price = data['price']
        change_pct = data['change_pct']
        save_price(symbol, price, change_pct, data['volume'])

        direction = "↑" if change_pct > 0 else "↓"
        print(f"  {name}({symbol}): {price:.2f}  {direction}{abs(change_pct)*100:.2f}%", end="")

        # ── 2. 异常波动检测（被动提示，不是交易信号）──
        threshold = cfg.get('anomaly_threshold', 0.05)
        if abs(change_pct) >= threshold:
            if not get_last_alert_time(symbol, 'anomaly', hours=8):
                msg = f"单日波动 {change_pct*100:+.2f}%，建议查看是否有相关新闻，但无需立即行动"
                send_anomaly_alert(name, symbol, price, change_pct)
                save_alert(symbol, 'ANOMALY', msg)
                print(f"  ⚡ 异常波动", end="")
                anomaly_count += 1

        # ── 3. FCF 倍数阈值检测（VALUE_WATCHER 专用）──
        if cfg['monitor_type'] == 'VALUE_WATCHER':
            fcf_cfg = cfg.get('fcf', {})
            yf_sym = cfg.get('yf_symbol')
            if yf_sym and any(v is not None for v in fcf_cfg.values()):
                fcf_multiple = get_fcf_multiple(yf_sym, price)
                if fcf_multiple:
                    print(f"  FCF={fcf_multiple:.1f}x", end="")

                    buy_t = fcf_cfg.get('buy')
                    cc_t = fcf_cfg.get('covered_call')
                    sell_t = fcf_cfg.get('sell') or fcf_cfg.get('hard_sell')

                    if buy_t and fcf_multiple <= buy_t:
                        if not get_last_alert_time(symbol, 'FCF_BUY', hours=24):
                            msg = f"FCF {fcf_multiple:.1f}x ≤ 买入线 {buy_t}x → 执行四条件清单"
                            send_fcf_threshold_alert(name, symbol, 'buy', fcf_multiple, price, buy_t)
                            save_alert(symbol, 'FCF_BUY', msg)
                            fcf_count += 1

                    elif cc_t and fcf_multiple >= cc_t and (not sell_t or fcf_multiple < sell_t):
                        if not get_last_alert_time(symbol, 'FCF_COVERED_CALL', hours=24):
                            msg = f"FCF {fcf_multiple:.1f}x ≥ {cc_t}x → 考虑卖出 covered call"
                            send_fcf_threshold_alert(name, symbol, 'covered_call', fcf_multiple, price, cc_t)
                            save_alert(symbol, 'FCF_COVERED_CALL', msg)
                            fcf_count += 1

                    elif sell_t and fcf_multiple >= sell_t:
                        if not get_last_alert_time(symbol, 'FCF_SELL', hours=24):
                            msg = f"FCF {fcf_multiple:.1f}x ≥ 卖出线 {sell_t}x → 执行减仓/清仓清单"
                            send_fcf_threshold_alert(name, symbol, 'sell', fcf_multiple, price, sell_t)
                            save_alert(symbol, 'FCF_SELL', msg)
                            fcf_count += 1

        print()   # 换行

    print(f"\n  本轮完成 — 异常波动 {anomaly_count} 条，FCF 触发 {fcf_count} 条")
    return anomaly_count, fcf_count
