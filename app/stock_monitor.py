"""
stock_monitor.py — 价格监控

新哲学：不是"看价格"，是检测两件事：
1. FCF 估值阈值是否被触发（买入/卖出纪律线）
2. 异常波动（单日 ±5%）是否需要去看新闻（提示，不是交易信号）

EXIT_PENDING 的持仓不产生任何警报。
"""
import yfinance as yf
from datetime import datetime, timedelta
from config import PORTFOLIO
from models import get_last_alert_time, save_alert, save_price
from feishu_push import send_anomaly_alert, send_fcf_threshold_alert
from data_sources import (
    get_cn_quotes,
    get_cn_financial,
    get_cn_balance_sheet,
    get_net_cash_us_hk,
)

# 数据陈旧阈值（2026-06-09 新增）：FCF 数据超过此天数，卖出信号只记录不推送
STALE_DATA_DAYS = 180  # 6 个月
# 极端倍数阈值（2026-06-09 新增）：超过此值只记录不推送
EXTREME_MULTIPLE = 100


# ── 价格获取 ──────────────────────────────────────────────

def get_cn_price(symbol):
    """
    通过多源 fallback 框架获取单只 A 股行情
    （改造自原 akshare.stock_zh_a_spot_em 直接调用，2026-06-09
     因东方财富接口被封而全部失败，改用 data_sources 多源）
    """
    quotes = get_cn_quotes([symbol])
    q = quotes.get(symbol)
    if q:
        return {
            'price': q['price'],
            'change_pct': q['change_pct'],
            'volume': q['volume'],
        }
    print(f"  [价格] 获取A股 {symbol} 失败: 所有数据源都失败")
    return None


def get_cn_prices_batch(symbols):
    """
    批量获取多只 A 股行情（更高效：一次 HTTP 请求拿所有）
    替代对每只股票单独调用 get_cn_price 的 N+1 问题
    """
    if not symbols:
        return {}
    return get_cn_quotes(symbols)


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
    """
    计算 FCF 倍数。仅对有 yf_symbol 的持仓（港股/美股）有效。

    返回: dict {fcf_multiple: float, ev_fcf_multiple: float, report_period: str, lag_days: int}
          如果数据缺失或陈旧，返回 None

    2026-06-09 重构：
      - 数据陈旧超过 STALE_DATA_DAYS 天，标记 stale=True
      - 新增 EV/FCF（市值 - 净现金 = 企业价值）
      - 数据不足时返回 None
    """
    try:
        ticker = yf.Ticker(yf_symbol)
        info = ticker.info
        market_cap = info.get('marketCap')

        if not market_cap:
            return None

        # 优先用 quarterly_cashflow（季报更及时），如果没有则用 annual（年报）
        cashflow = None
        source = None
        if hasattr(ticker, 'quarterly_cashflow') and ticker.quarterly_cashflow is not None and not ticker.quarterly_cashflow.empty:
            cashflow = ticker.quarterly_cashflow
            source = 'quarterly'
        elif ticker.cashflow is not None and not ticker.cashflow.empty:
            cashflow = ticker.cashflow
            source = 'annual'

        if cashflow is None or cashflow.empty:
            return None

        # 报告期
        latest_col = cashflow.columns[0]
        try:
            report_period = datetime.strptime(str(latest_col)[:10], "%Y-%m-%d")
            lag_days = (datetime.now() - report_period).days
        except Exception:
            report_period = None
            lag_days = 999

        # 找 OCF 和 CapEx
        op_cf = None
        capex = None
        if 'Operating Cash Flow' in cashflow.index:
            op_cf = cashflow.loc['Operating Cash Flow'].iloc[0]
        if 'Capital Expenditure' in cashflow.index:
            capex = cashflow.loc['Capital Expenditure'].iloc[0]

        # 如果只有 OCF 没 CapEx，CapEx 当 0（保守：FCF = OCF）
        fcf = None
        if op_cf is not None and not (isinstance(op_cf, float) and op_cf != op_cf):  # 非 NaN
            if capex is not None and not (isinstance(capex, float) and capex != capex):
                fcf = op_cf + capex
            else:
                fcf = op_cf  # CapEx 缺失，用 OCF 代替（保守）

        if fcf is None or fcf <= 0:
            return None

        fcf_multiple = market_cap / fcf

        # EV/FCF = (市值 - 净现金) / FCF
        net_cash = get_net_cash_us_hk(yf_symbol)
        enterprise_value = market_cap - net_cash
        ev_fcf = enterprise_value / fcf if fcf > 0 else None

        return {
            'fcf_multiple': round(fcf_multiple, 1),
            'ev_fcf_multiple': round(ev_fcf, 1) if ev_fcf else None,
            'report_period': str(latest_col)[:10] if latest_col else None,
            'lag_days': lag_days,
            'source': source,
            'net_cash': net_cash,
            'stale': lag_days > STALE_DATA_DAYS,
        }
    except Exception as e:
        print(f"  [FCF] 计算 {yf_symbol} 失败: {e}")
    return None


def get_cn_fcf_multiple(symbol6, current_price=None):
    """
    A 股 FCF 倍数计算（2026-06-09 新增）
    使用 akshare 同花顺现金流接口 + 新浪资产负债表

    返回: dict {fcf_multiple, ev_fcf_multiple, report_period, lag_days, stale}
    """
    fin = get_cn_financial(symbol6)
    if not fin:
        return None

    # A 股最新一期 FCF（单季度，不是 TTM）
    # 注意：A 股 akshare 返回的是单期数据，需要 TTM 的话要聚合多期
    # 这里先用单期做简化（季度数据已足够新）
    fcf = fin['fcf']

    # 市值（用新浪接口拿实时市值）
    quotes = get_cn_quotes([symbol6])
    if not quotes or symbol6 not in quotes:
        return None

    q = quotes[symbol6]
    price = q['price']

    # 总股本（人民币计价）
    # 优先级：1) 腾讯 qt.gtimg.cn 接口（最准，含总股本和流通股本）
    #          2) akshare.stock_individual_info_em（东方财富，国内代理常被封）
    #          3) 流通股本 × 1.3 估算
    market_cap_yuan = None
    shares = None
    try:
        # 腾讯接口字段（2026-06-09 实测）：
        #   宿主机: parts[69] = 总股本，parts[70] = 流通股本
        #   容器内: parts[72] = 总股本，parts[73] = 流通股本
        # （接口响应字段位置因出口 IP 不同略有差异，需动态扫描）
        import requests as _req
        prefix = 'sh' if symbol6.startswith(('5', '6', '9')) else 'sz'
        url = f"https://qt.gtimg.cn/q={prefix}{symbol6}"
        r = _req.get(url, timeout=5)
        text = r.text
        if '~' in text:
            parts = text.split('~')
            # 动态扫描：找出第一个 > 1亿 的数字作为总股本候选
            for i in range(40, min(len(parts), 90)):
                try:
                    v = float(parts[i].strip())
                    if v > 1e9:
                        shares = v
                        if shares > 1e6:
                            market_cap_yuan = price * shares
                        break
                except (ValueError, IndexError):
                    continue
    except Exception:
        pass

    if market_cap_yuan is None:
        try:
            import akshare as ak
            info_df = ak.stock_individual_info_em(symbol=symbol6)
            if info_df is not None and not info_df.empty:
                for _, r in info_df.iterrows():
                    item = str(r.get('item', ''))
                    if '总股本' in item:
                        val = r.get('value')
                        if val is not None:
                            val_str = str(val)
                            if '亿' in val_str:
                                shares = float(val_str.replace('亿股', '').replace('亿', '').replace(',', '')) * 1e8
                            elif '万' in val_str:
                                shares = float(val_str.replace('万', '').replace(',', '')) * 1e4
                            else:
                                shares = float(val_str.replace(',', ''))
                            market_cap_yuan = price * shares
                            break
        except Exception:
            pass

    if market_cap_yuan is None:
        # 兜底：用流通股本（来自腾讯接口，parts[70]）
        try:
            import requests as _req
            prefix = 'sh' if symbol6.startswith(('5', '6', '9')) else 'sz'
            url = f"https://qt.gtimg.cn/q={prefix}{symbol6}"
            r = _req.get(url, timeout=5)
            text = r.text
            if '~' in text:
                parts = text.split('~')
                try:
                    if len(parts) > 70:
                        free_shares_str = parts[70].strip()
                        if free_shares_str and free_shares_str != '':
                            free_shares = float(free_shares_str)
                            if free_shares > 1e6:
                                # 估算总股本 = 流通股本 × 1.2（多数股票限售 < 流通）
                                market_cap_yuan = price * free_shares * 1.2
                except (ValueError, IndexError):
                    pass
        except Exception:
            pass

    if market_cap_yuan is None or market_cap_yuan <= 0:
        return None

    # fin['fcf'] 现在是 TTM（过去12个月累加），无需再年化
    annualized_fcf = fin['fcf']

    if annualized_fcf <= 0:
        # 防御：TTM FCF 仍为负（持续亏损股），跳过估值
        return None

    fcf_multiple = market_cap_yuan / annualized_fcf

    # EV/FCF
    bs = get_cn_balance_sheet(symbol6)
    net_cash = bs['net_cash'] if bs else 0
    enterprise_value = market_cap_yuan - net_cash
    ev_fcf = enterprise_value / annualized_fcf

    return {
        'fcf_multiple': round(fcf_multiple, 1),
        'ev_fcf_multiple': round(ev_fcf, 1) if ev_fcf else None,
        'report_period': fin['report_period'],
        'lag_days': fin['lag_days'],
        'source': 'cn_akshare_ttm',
        'net_cash': net_cash,
        'stale': fin['lag_days'] > STALE_DATA_DAYS,
        'market_cap': market_cap_yuan,
        'ttm_ocf': fin.get('ttm_ocf'),
        'ttm_capex': fin.get('ttm_capex'),
        'ttm_fcf': fin.get('ttm_fcf'),
    }


# ── 核心监控逻辑 ──────────────────────────────────────────

def monitor_stocks():
    print(f"\n{'='*55}")
    print(f"  价格 & FCF 监控 — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*55}")

    anomaly_count = 0
    fcf_count = 0

    # ── 优化：先批量收集所有 A 股代码，一次 HTTP 请求拿全部行情（2026-06-09 新增）──
    cn_codes_to_fetch = []
    cn_code_to_symbol = {}  # akshare_code → 投资组合 symbol（600036 是 symbol 也是 akshare_code，映射是恒等）
    for symbol, cfg in PORTFOLIO.items():
        if cfg['monitor_type'] == 'EXIT_PENDING':
            continue
        if cfg['market'] == 'CN':
            code = cfg.get('akshare_symbol', symbol)
            cn_codes_to_fetch.append(code)
            cn_code_to_symbol[code] = symbol

    cn_quotes_cache = get_cn_prices_batch(cn_codes_to_fetch) if cn_codes_to_fetch else {}
    if cn_codes_to_fetch:
        ok = len(cn_quotes_cache)
        total = len(cn_codes_to_fetch)
        print(f"  A股批量拉取：{ok}/{total} 只成功")

    for symbol, cfg in PORTFOLIO.items():
        name = cfg['name']

        # EXIT_PENDING：完全跳过
        if cfg['monitor_type'] == 'EXIT_PENDING':
            continue

        # ── 1. 获取当前价格 ──
        if cfg['market'] == 'CN':
            # A股走批量缓存路径，避免重复 HTTP 请求
            code = cfg.get('akshare_symbol', symbol)
            data = cn_quotes_cache.get(code)
            if data:
                data = {'price': data['price'], 'change_pct': data['change_pct'], 'volume': data['volume']}
        else:
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
            market = cfg['market']

            # ── 3a. 根据市场选择不同的 FCF 计算路径（2026-06-09 重构）──
            fcf_data = None
            if market == 'CN':
                # A股用 akshare 接口
                code = cfg.get('akshare_symbol', symbol)
                fcf_data = get_cn_fcf_multiple(code, price)
            else:
                # 港股/美股用 yfinance
                yf_sym = cfg.get('yf_symbol')
                if yf_sym:
                    fcf_data = get_fcf_multiple(yf_sym, price)

            # ── 3b. 输出 + 陈旧/极端数据保护（2026-06-09 新增）──
            if fcf_data:
                fcf_mult = fcf_data['fcf_multiple']
                ev_fcf = fcf_data.get('ev_fcf_multiple')
                lag = fcf_data.get('lag_days', 0)
                is_stale = fcf_data.get('stale', False)

                # 打印
                staleness_tag = f" [数据滞后{lag}天]" if is_stale else ""
                ev_tag = f", EV/FCF={ev_fcf:.1f}x" if ev_fcf else ""
                print(f"  FCF={fcf_mult:.1f}x{ev_tag}{staleness_tag}", end="")

                # 保护：如果数据陈旧或倍数极端，卖出信号只记录不推送
                skip_push = is_stale or (fcf_mult and fcf_mult > EXTREME_MULTIPLE)

                buy_t = fcf_cfg.get('buy')
                cc_t = fcf_cfg.get('covered_call')
                sell_t = fcf_cfg.get('sell') or fcf_cfg.get('hard_sell')

                if buy_t and fcf_mult <= buy_t:
                    if not get_last_alert_time(symbol, 'FCF_BUY', hours=24):
                        msg = f"FCF {fcf_mult:.1f}x ≤ 买入线 {buy_t}x → 执行四条件清单"
                        send_fcf_threshold_alert(name, symbol, 'buy', fcf_mult, price, buy_t)
                        save_alert(symbol, 'FCF_BUY', msg)
                        fcf_count += 1

                elif cc_t and fcf_mult >= cc_t and (not sell_t or fcf_mult < sell_t):
                    if not get_last_alert_time(symbol, 'FCF_COVERED_CALL', hours=24):
                        msg = f"FCF {fcf_mult:.1f}x ≥ {cc_t}x → 考虑卖出 covered call"
                        send_fcf_threshold_alert(name, symbol, 'covered_call', fcf_mult, price, cc_t)
                        save_alert(symbol, 'FCF_COVERED_CALL', msg)
                        fcf_count += 1

                elif sell_t and fcf_mult >= sell_t:
                    if not get_last_alert_time(symbol, 'FCF_SELL', hours=24):
                        if skip_push:
                            # 数据陈旧或极端倍数 → 只记录，不推送飞书
                            reason = "数据陈旧" if is_stale else f"倍数极端(>{EXTREME_MULTIPLE}x)"
                            msg = f"⚠️ FCF {fcf_mult:.1f}x 触发卖出线 {sell_t}x，但因{reason}（报告期 {fcf_data.get('report_period')}，滞后 {lag} 天），仅记录不推送"
                            save_alert(symbol, 'FCF_SELL_STALE', msg)
                            print(f"  🔇 卖出信号[抑制推送：{reason}]", end="")
                        else:
                            msg = f"FCF {fcf_mult:.1f}x ≥ 卖出线 {sell_t}x → 执行减仓/清仓清单"
                            send_fcf_threshold_alert(name, symbol, 'sell', fcf_mult, price, sell_t)
                            save_alert(symbol, 'FCF_SELL', msg)
                            fcf_count += 1
            elif market == 'CN':
                # A股但拿不到数据（akshare 接口失败）
                print(f"  [FCF 数据缺失]", end="")

        print()   # 换行

    print(f"\n  本轮完成 — 异常波动 {anomaly_count} 条，FCF 触发 {fcf_count} 条")
    return anomaly_count, fcf_count
