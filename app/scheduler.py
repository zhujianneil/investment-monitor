"""
scheduler.py — 任务调度

新哲学：频率越高的动作认知含量越低。
调度频率与"你能做的判断的频率"对齐。

  每个交易日（盘中）：只检测异常波动 + FCF 阈值
  每天（早晨）      ：新闻 & 公告扫描 + 财报日历检查
  每周日（下午）    ：周报摘要推送

2026-06-11 三层防御加固：
  L1: 各 job 内的循环给单只 symbol/单条新闻包 try/except（详见 stock_monitor.py / news_monitor.py）
  L2: 每个 job 外层包 try/except + 写 monitor_runs 表 + 飞书告警"本轮崩溃"
  L3: 新增 watch_dog 任务，每 2 小时检查最近 N 轮是否全是 failed/partial
      + 启动时调用一次 replay_dlq()，自动重放历史失败的消息
"""
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
import pytz

from stock_monitor import monitor_stocks
from news_monitor import monitor_news
from announcement_stream import run_announcement_stream
from yf_news_stream import run_yf_news_stream
from cls_stream import run_cls_stream
from llm_enhancer import enhance_pending_events, is_available as llm_available
from thesis_tracker import run_thesis_tracker
from earnings_calendar import check_earnings_calendar
from weekly_digest import send_weekly_report
from cninfo_stream import run_cninfo_stream
from sina_announcement_stream import run_sina_announcement_stream
from sina_global_stream import run_sina_global_stream
from models import (
    init_db, update_heartbeat, get_recent_source_failures,
    record_monitor_run, get_recent_monitor_runs,
    get_db,
)
from data_sources import health_check as ds_health_check
from feishu_push import _send, replay_dlq
from ah_render import run as run_ah_dashboard
from lhb_stream import run_lhb_stream

tz = pytz.timezone('Asia/Shanghai')


# ── 通用包装器：L2 防御（2026-06-11 新增）─────────────────────
def _wrap(job_name, fn):
    """
    把 job 函数包成"外层 try/except + 健康记录 + 飞书告警"。
    - 正常完成 → 写 monitor_runs(status=ok)
    - 异常但部分处理完成 → status=partial
    - 整体崩溃 → status=failed + 飞书红色告警
    """
    started_at = datetime.now().isoformat(timespec='seconds')
    symbols_processed = 0
    last_err = None
    try:
        result = fn()
        # 多数 job 返回 (anomaly, fcf) 元组；news 返回 int；其他返回 None
        if isinstance(result, tuple):
            symbols_processed = sum(x for x in result if isinstance(x, int))
        elif isinstance(result, int):
            symbols_processed = result
        record_monitor_run(job_name, 'ok', symbols_processed=symbols_processed,
                          symbols_failed=0, started_at=started_at)
        update_heartbeat()
        return result
    except Exception as e:
        last_err = f"{type(e).__name__}: {str(e)[:300]}"
        print(f"  ✗✗ {job_name} 整体崩溃: {last_err}")
        record_monitor_run(job_name, 'failed', symbols_processed=0,
                          symbols_failed=0, last_error=last_err, started_at=started_at)
        # 飞书红色告警（告警本身如果失败，进 DLQ 等待重放）
        _send(
            f"🚨 监控任务崩溃 — {job_name}",
            f"**任务**：{job_name}\n"
            f"**开始时间**：{started_at}\n"
            f"**异常**：\n```\n{last_err}\n```\n\n"
            f"**自动恢复**：本次崩溃已隔离。容器仍存活，下一时点 (≤2小时) 会重试。\n"
            f"**人工处理**：\n"
            f"1. `docker logs investment-monitor --since 1h | tail -100`\n"
            f"2. `sqlite3 /opt/investment-monitor/data/investment.db 'SELECT * FROM monitor_runs ORDER BY id DESC LIMIT 5;'`\n",
            'red'
        )
        update_heartbeat()
        return None


# ── 各 job 包装 ────────────────────────────────────────────

def job_market_monitor():
    """交易时段：异常波动 + FCF 阈值检测"""
    print("\n>>> [交易时段] 价格 & FCF 监控")
    return monitor_stocks()


def job_daily_news():
    """
    每日 9:00 早晨: news_monitor 关键词扫描 + 财报日历.
    注: 2026-06-19 后, A 股公告 + 7x24 电报 + 港美股 yf 新闻
    已被 announcement_stream/cls_stream/yf_news_stream 接管 (15min/60min).
    本 job 保留: 1) news_monitor 的 announcements 表历史镜像 2) 财报日历检查
    """
    print("\n>>> [每日 9:00] 关键词新闻 (legacy) + 财报日历")
    monitor_news()
    check_earnings_calendar()


def job_announcement_stream():
    """
    A 股公告流（2026-06-19 新增）— 第一层, 无 LLM 依赖
    每 15 分钟跑一次, 只抓"重大事项"类 (快, 30s 内).
    """
    print("\n>>> [公告流] A 股全市场重大事项")
    return run_announcement_stream(report_types=['重大事项'])


def job_announcement_stream_full():
    """
    A 股公告流 (全量) — 跑全报告类型, 1 小时一次.
    补齐 15min 跑漏的类型 (资产重组/风险提示/财务报告).
    """
    print("\n>>> [公告流] A 股全市场全量")
    return run_announcement_stream(report_types=['重大事项', '资产重组', '风险提示', '财务报告'])


def job_yf_news_stream():
    """
    港美股 yfinance 新闻流 (2026-06-19 新增) — 第一层
    每小时一次, 关键词命中推送.
    """
    print("\n>>> [港美股新闻] yfinance 抓取")
    return run_yf_news_stream()


def job_cls_stream():
    """
    7×24 电报流 (2026-06-19 P1 新增) — 第一层
    新浪财经 7×24 公开 API (lid=2515 科技/AI, 2509/2516 国际财经, 2514 国际时政)
    每 15 分钟一次, 关键词命中推送.
    """
    print("\n>>> [7×24 电报] 新浪财经")
    return run_cls_stream()


def job_cninfo_stream():
    """
    巨潮公告流 (2026-06-19 P1 新增; 2026-06-25 v2 修) — 公告流 1 号源
    每 15 分钟拉一次 (错开东财流的 :00/:15/:30/:45),
    抓今日全市场 (SSE + SZSE 并行 20 页) + 持仓 ticker 兜底
    """
    print("\n>>> [巨潮] A 股公告 1 号源 (v2)")
    return run_cninfo_stream()


def job_sina_announcement_stream():
    """
    新浪财经 vCB_AllBulletin 公告流 (2026-06-25 新增) — 公告流 备源
    每小时一次, 9 只 A 股持仓 ticker 专属 HTML 解析
    跟巨潮 + 东财 跨源去重, 互不重复推送
    """
    print("\n>>> [新浪公告] A 股备源 (vCB_AllBulletin)")
    return run_sina_announcement_stream()


def job_sina_global_stream():
    """
    新浪全球财经 (2026-06-19 P1 新增) — 港美股新闻备源
    每小时一次 (lid=1686 国际财经), 失败时 yfinance 兜底
    """
    print("\n>>> [新浪全球] 港美股新闻备源")
    return run_sina_global_stream()


def job_llm_enhance():
    """
    LLM 增强 (2026-06-19 新增) — 第二层
    每 15 分钟扫一次未增强的 events (LLM key 未配时降级跳过).
    """
    if not llm_available():
        print("\n>>> [LLM 增强] 未配置, 跳过")
        return {'disabled': True}
    print("\n>>> [LLM 增强] 处理未增强 events")
    return enhance_pending_events(batch_size=20)


def job_thesis_track():
    """
    持仓 thesis 归档 (2026-06-24 新增) — 第三层
    每 15 分钟扫一次"有 thesis 的 symbol"的未归档 events,
    LLM 判命中假设 + 支持/削弱/中性。LLM 未配时降级跳过。
    """
    if not llm_available():
        print("\n>>> [thesis 归档] 未配置 LLM, 跳过")
        return {'disabled': True}
    print("\n>>> [thesis 归档] 处理未归档 events")
    return run_thesis_tracker(batch_size=30)


def job_weekly_digest():
    """每周日：周报推送"""
    print("\n>>> [每周] 周报生成")
    send_weekly_report()


# ── AH 看板 (2026-06-22 新增) ────────────────────────────────
def job_ah_dashboard_daily():
    """
    每个交易日 18:00 跑一次：
      1. 采集三源数据（ah_premium / southbound / usdhkd）
      2. 计算信号（动态分位 + 三因子交叉）
      3. 渲染 HTML 看板到 reports/ah_premium_YYYYMMDD.html
      4. 推 Feishu 卡片到 DM

    强信号（STRONG_CONVERGE / DIVERGE_ACCELERATING）已通过红色标题在常规卡片里突出，不再额外推避免刷屏
    """
    try:
        data = run_ah_dashboard(do_fetch=True, push_feishu=True)
        n = len(data.get("signals", []))
        strong = len(data.get("strong_signals", []))
        print(f"  [AH 看板] ✓ 已生成 (信号 {n} 个, 强信号 {strong} 个)")
    except Exception as e:
        print(f"  [AH 看板] ✗ 失败: {e}")
        raise


# ── 龙虎榜异动 (2026-06-22 新增) ─────────────────────────────
def job_lhb_daily():
    """
    每个交易日 16:00 跑一次（15:30 上交所/深交所公布当日龙虎榜）:
      1. 调广发 gf-skills MCP 接口拉 sh+sz 当日全市场龙虎榜
      2. 跟 PORTFOLIO 持仓做精确 symbol 匹配
      3. 命中 → 推飞书红色告警 (官方异动阈值, 比 ±5% 准)
      4. 全市场清单入 events 表 (source='gf_lhb') 供回看

    频次: 1 天 1 次够 (龙虎榜盘后才公布, 盘中永远是空)
    兜底: 16:00 / 18:00 双跑, 防止广发接口首次不稳定
    """
    return run_lhb_stream()


def job_ah_history_backfill():
    """周日晚回填一周历史，确保数据连续。"""
    try:
        from ah_fetcher import fetch_ah_premium_history, fetch_usdhkd_history
        fetch_ah_premium_history(400)
        fetch_usdhkd_history(400)
        print("  [AH 历史] ✓ 回填完成")
    except Exception as e:
        print(f"  [AH 历史] ✗ 回填失败: {e}")
        raise


def job_data_source_health():
    """
    数据源健康检查（2026-06-09 新增）
    每 6 小时跑一次，发现所有源都不可用时推送告警
    """
    print("\n>>> [健康检查] 数据源可用性")
    hc = ds_health_check()
    print(f"  检查结果: {hc}")

    # 统计：所有源都不可用 = 紧急
    ok_count = sum(1 for v in hc.values() if v.get("ok"))
    if ok_count == 0:
        # 紧急：所有 A 股数据源都挂了
        lines = ["**🚨 A 股数据源全部失效**\n"]
        lines.append("所有 A 股数据源都不可用，A 股监控已失效。\n")
        lines.append("**当前状态**：")
        for src, status in hc.items():
            lines.append(f"- `{src}`: {status}")
        lines.append("\n**建议**：")
        lines.append("1. 登录服务器检查容器日志：`docker logs investment-monitor`")
        lines.append("2. 测试新浪源：`curl -H 'Referer: https://finance.sina.com.cn' 'https://hq.sinajs.cn/list=sh600036'`")
        lines.append("3. 如果是网络问题，检查 Oracle 安全组是否封了 443 端口")
        content = "\n".join(lines)
        _send("🚨 数据源紧急告警", content, "red")
    elif ok_count < len(hc):
        # 部分源失效，不致命但要记录
        failed = [k for k, v in hc.items() if not v.get("ok")]
        print(f"  部分源失效: {failed}（系统仍可用，仅记录）")


def job_watchdog():
    """
    L3 看门狗（2026-06-11 新增）。
    每 2 小时检查一次：最近 3 轮监控是否全部 failed/partial + 至少 1 轮 failed
    → 说明系统持续异常，需要"叫醒人"。
    """
    print("\n>>> [看门狗] 系统健康度自检")

    # 1) 连续失败检测
    recent = get_recent_monitor_runs('market_monitor', limit=3)
    if recent and len(recent) >= 2:
        # 最新一轮是 failed 且前几轮连续 ≤1 ok → 升级
        all_bad = all(r['status'] in ('failed', 'partial') for r in recent)
        if all_bad:
            lines = ["**🚨 监控连续 2+ 轮未成功**\n"]
            lines.append("**最近运行记录**：\n")
            for r in recent:
                lines.append(
                    f"- `{r['finished_at']}` status={r['status']} "
                    f"处理 {r.get('symbols_processed', 0)} 只，"
                    f"失败 {r.get('symbols_failed', 0)} 只"
                )
                if r.get('last_error'):
                    lines.append(f"  - 错误：`{r['last_error'][:200]}`")
            lines.append("\n**可能原因**：")
            lines.append("- 所有 A 股数据源被封")
            lines.append("- 数据库表损坏 / 锁死")
            lines.append("- 容器内 Python 环境异常")
            lines.append("\n**下一步**：")
            lines.append("1. `docker exec investment-monitor python3 -c 'from data_sources import health_check; print(health_check())'`")
            lines.append("2. `docker exec investment-monitor python3 -c 'from models import init_db; init_db()'`")
            lines.append("3. 必要时 `docker restart investment-monitor`")
            _send("🚨 监控持续异常 — 需要人工介入", "\n".join(lines), 'red')
            return

    # 1.5) 东财公告流连续失败检测 (2026-06-19 P0)
    # 逻辑: 最近 2 小时 stock_notice_report_* 失败 >= 5 次 → 东财接口可能挂了
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute('''SELECT COUNT(*) as cnt FROM data_source_failures
                     WHERE source_name LIKE 'stock_notice_report_%'
                     AND occurred_at > datetime('now', '-2 hours')''')
        em_fail_cnt = c.fetchone()['cnt']
        conn.close()
        if em_fail_cnt >= 5:
            lines = ["**🚨 东财公告接口疑似挂了**\n"]
            lines.append(f"最近 2 小时 `stock_notice_report_*` 失败 **{em_fail_cnt} 次** (阈值 5)\n")
            lines.append("**可能原因**：")
            lines.append("- Oracle 服务器到 np-anotice-stock.eastmoney.com 的 SSL 不稳")
            lines.append("- 东财接口本身限流/维护")
            lines.append("- 容器内 Python requests 库版本过老\n")
            lines.append("**下一步**：")
            lines.append("1. `docker exec investment-monitor python3 -c 'import requests; r=requests.get(\"https://np-anotice-stock.eastmoney.com/\", timeout=5); print(r.status_code)'`")
            lines.append("2. 如果一直 SSL EOF → 临时降级, 等东财恢复")
            lines.append("3. 考虑加备用源 (巨潮 cninfo)")
            _send("🚨 东财公告接口持续失败", "\n".join(lines), 'red')
    except Exception as e:
        print(f"  [watchdog] 东财检测异常 (不致命): {e}")

    # 2) DLQ 重放：每 2 小时把历史失败的消息再发一次
    result = replay_dlq(max_items=10, max_age_hours=48)
    if result.get('succeeded'):
        print(f"  DLQ 重放成功 {result['succeeded']}/{result['replayed']} 条")

    # 3) 心跳
    update_heartbeat()


def start():
    init_db()
    update_heartbeat()

    # 启动时立即重放一次 DLQ（捕获上次崩溃期间未送达的消息）
    print("\n  >>> 启动时重放 DLQ ...")
    try:
        replay_dlq(max_items=20, max_age_hours=48)
    except Exception as e:
        print(f"  DLQ 重放失败（不致命）: {e}")

    scheduler = BlockingScheduler(timezone=tz)

    # ── 交易时段价格监控（A/H/US 覆盖）──
    scheduler.add_job(
        lambda: _wrap('market_monitor', job_market_monitor),
        CronTrigger(day_of_week='mon-fri', hour='9,11,14', minute='35', timezone=tz),
        id='market_monitor',
        name='交易时段监控',
    )
    # 美股收盘（北京时间 05:00）：只捕获美股异常
    scheduler.add_job(
        lambda: _wrap('us_close_monitor', job_market_monitor),
        CronTrigger(day_of_week='tue-sat', hour='5', minute='0', timezone=tz),
        id='us_close_monitor',
        name='美股收盘检测',
    )

    # ── 每日早晨新闻扫描（9:00）──
    scheduler.add_job(
        lambda: _wrap('daily_news', job_daily_news),
        CronTrigger(hour='9', minute='0', timezone=tz),
        id='daily_news',
        name='每日新闻公告',
    )

    # ── A 股公告流 (2026-06-19 新增) ──
    # 15min 跑一次快版 (只抓'重大事项')
    scheduler.add_job(
        lambda: _wrap('announcement_stream', job_announcement_stream),
        CronTrigger(minute='*/15', timezone=tz),
        id='announcement_stream',
        name='A 股公告流 (快版)',
    )
    # 60min 跑一次全量 (补齐其他报告类型)
    scheduler.add_job(
        lambda: _wrap('announcement_stream_full', job_announcement_stream_full),
        CronTrigger(minute='30', timezone=tz),
        id='announcement_stream_full',
        name='A 股公告流 (全量)',
    )

    # ── 港美股 yfinance 新闻流 (2026-06-19 新增) ──
    scheduler.add_job(
        lambda: _wrap('yf_news_stream', job_yf_news_stream),
        CronTrigger(minute='45', timezone=tz),
        id='yf_news_stream',
        name='港美股 yf 新闻流',
    )

    # ── 7×24 电报流 (2026-06-19 P1 新增) ──
    # 时分错开 announcement_stream (公告流 */15 跑在 0/15/30/45 分的 :00)
    scheduler.add_job(
        lambda: _wrap('cls_stream', job_cls_stream),
        CronTrigger(minute='5,20,35,50', timezone=tz),
        id='cls_stream',
        name='7×24 电报流',
    )

    # ── 巨潮公告流 (2026-06-19 P1 新增, 公告流 1 号源; 2026-06-25 v2 修) ──
    # 与东财 announcement_stream 并行, 巨潮是 1 号源 (官方, 数据质量最高)
    # 时分错开: 跑在 :10/:25/:40/:55, 避开其他流
    scheduler.add_job(
        lambda: _wrap('cninfo_stream', job_cninfo_stream),
        CronTrigger(minute='10,25,40,55', timezone=tz),
        id='cninfo_stream',
        name='巨潮公告流 (1号源)',
    )

    # ── 新浪 vCB_AllBulletin (2026-06-25 新增, 公告流备源) ──
    # 1 小时一次, 9 只 A 股持仓 ticker 专属, 跟巨潮/东财 跨源去重
    # 错开 cninfo (15min) 和 announcement_stream (15min), 单独节奏
    scheduler.add_job(
        lambda: _wrap('sina_announcement_stream', job_sina_announcement_stream),
        CronTrigger(minute='20', timezone=tz),
        id='sina_announcement_stream',
        name='新浪公告流 (备源)',
    )

    # ── 新浪全球财经 (2026-06-19 P1 新增, 港美股新闻备) ──
    # 每小时一次 (lid=1686 国际财经), 失败降级 yfinance
    scheduler.add_job(
        lambda: _wrap('sina_global_stream', job_sina_global_stream),
        CronTrigger(minute='15', timezone=tz),
        id='sina_global_stream',
        name='新浪全球财经 (港美股备)',
    )

    # ── LLM 增强 (2026-06-19 新增) ──
    # 15min 一次, key 未配时降级跳过
    scheduler.add_job(
        lambda: _wrap('llm_enhance', job_llm_enhance),
        CronTrigger(minute='7,22,37,52', timezone=tz),
        id='llm_enhance',
        name='LLM 事件增强',
    )

    # ── 持仓 thesis 归档 (2026-06-24 新增) ──
    # 15min 一次, 跟在 llm_enhance 之后 (错峰 :9/:24/:39/:54), key 未配时降级跳过
    scheduler.add_job(
        lambda: _wrap('thesis_track', job_thesis_track),
        CronTrigger(minute='9,24,39,54', timezone=tz),
        id='thesis_track',
        name='持仓 thesis 归档',
    )

    # ── 每周日 20:00 周报 ──
    scheduler.add_job(
        lambda: _wrap('weekly_digest', job_weekly_digest),
        CronTrigger(day_of_week='sun', hour='20', minute='0', timezone=tz),
        id='weekly_digest',
        name='每周摘要',
    )

    # ── 数据源健康检查（每 6 小时，2026-06-09 新增）──
    scheduler.add_job(
        lambda: _wrap('data_source_health', job_data_source_health),
        CronTrigger(hour='*/6', minute='0', timezone=tz),
        id='data_source_health',
        name='数据源健康检查',
    )

    # ── AH 溢价看板日报 (2026-06-22 新增) ──
    # 每个交易日 18:00 跑：采集+分析+渲染 HTML+推 Feishu 卡片
    scheduler.add_job(
        lambda: _wrap('ah_dashboard_daily', job_ah_dashboard_daily),
        CronTrigger(day_of_week='mon-fri', hour='18', minute='0', timezone=tz),
        id='ah_dashboard_daily',
        name='AH 溢价看板日报',
    )
    # 每周日 20:30 跑一次历史回填（补周末缺数据）
    scheduler.add_job(
        lambda: _wrap('ah_history_backfill', job_ah_history_backfill),
        CronTrigger(day_of_week='sun', hour='20', minute='30', timezone=tz),
        id='ah_history_backfill',
        name='AH 历史回填',
    )

    # ── 龙虎榜异动 (2026-06-22 新增) ──
    # 15:30 上交所/深交所公布当日龙虎榜 → 16:00 拉取最稳
    # 18:00 再跑一次兜底（广发接口可能 16:00 还没刷新）
    scheduler.add_job(
        lambda: _wrap('lhb_daily', job_lhb_daily),
        CronTrigger(day_of_week='mon-fri', hour='16', minute='0', timezone=tz),
        id='lhb_daily',
        name='龙虎榜异动 (主)',
    )
    scheduler.add_job(
        lambda: _wrap('lhb_daily_backup', job_lhb_daily),
        CronTrigger(day_of_week='mon-fri', hour='18', minute='5', timezone=tz),
        id='lhb_daily_backup',
        name='龙虎榜异动 (兜底)',
    )

    # ── 看门狗（每 2 小时，2026-06-11 新增）──
    scheduler.add_job(
        lambda: _wrap('watchdog', job_watchdog),
        CronTrigger(hour='*/2', minute='15', timezone=tz),
        id='watchdog',
        name='看门狗',
    )

    print("=" * 55)
    print("  投资监控系统启动（纪律优先版 + 三层防御）")
    print("=" * 55)
    print("\n  调度配置：")
    print("  · 交易时段监控：工作日 9:35 / 11:35 / 14:35")
    print("  · 美股收盘检测：Tue-Sat 05:00")
    print("  · 每日新闻公告：每天 09:00")
    print("  · 每周摘要报告：每周日 20:00")
    print("  · 数据源健康检查：每 6 小时")
    print("  · 龙虎榜异动：工作日 16:00 / 18:05（兜底）")
    print("  · AH 溢价看板：工作日 18:00 / 周日 20:30 回填")
    print("  · 看门狗：每 2 小时")
    print("\n  EXIT_PENDING 持仓（海尔智家、福耀玻璃）已从监控中剔除")
    print("  监控原则：信息主动找你，你不主动找信息")
    print("  防御层级：L1 单 symbol 隔离 / L2 job 外层兜底 / L3 跨轮看门狗\n")

    # 启动时执行一次初始化监控
    print("  >>> 启动时执行一次初始化监控...")
    _wrap('init_monitor', job_market_monitor)
    _wrap('init_news', job_daily_news)
    _wrap('init_announcement_stream', job_announcement_stream)  # 2026-06-19 立即跑一次抓当日
    _wrap('init_yf_news', job_yf_news_stream)  # 2026-06-19 立即跑一次抓港美股
    _wrap('init_cls_stream', job_cls_stream)  # 2026-06-19 P1 立即跑一次 7×24
    _wrap('init_cninfo', job_cninfo_stream)  # 2026-06-25 立即跑一次巨潮 v2
    _wrap('init_sina_announcement', job_sina_announcement_stream)  # 2026-06-25 立即跑一次新浪备源
    _wrap('init_thesis_track', job_thesis_track)  # 2026-06-24 启动回填一次 thesis 归档

    print("\n  开始定时调度...")
    scheduler.start()
