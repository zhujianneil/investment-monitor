# P1: 信息源双轨制改造 (2026-06-19)

## 背景

东财 (`np-anotice-stock.eastmoney.com`) 多次 SSL EOF / 超时, 是当前监控的
**单点故障** (公告流完全依赖东财)。用户要求"有备用源"。

## 容器内实测 (2026-06-19 13:00+)

| 源 | 实测 | 用途 | 改造难度 |
|---|---|---|---|
| 新浪 7×24 feed.mix.sina.com.cn | 200 1.8s | 公告流 backup + cls 备用 | 🟢 低 (已有部分) |
| 巨潮 cninfo.com.cn | 500 5.5s (GET) | 公告**官方**权威源 | 🟡 中 (需 POST + 正确 API) |
| 财联社 cls.cn | 200 2.5s (HTML, SPA) | 财联社电报权威源 | 🔴 高 (需 Playwright) |
| 东方财富股吧 | 200 4.4s | 评论 (噪音大) | ⚪ 跳过 |
| 雪球 | 400 | 需登录 | ⚪ 跳过 |
| 华尔街见闻 | 200 (46B 限流) | 新闻 | ⚪ 跳过 |

## 目标架构

```
[公告流]                    [新闻流]                   [电报流]
  │                          │                          │
  ├─ 东财 (主) ✅             ├─ yfinance (主) ✅        ├─ 新浪 7×24 (主) ✅
  │                          │                          │
  └─ 巨潮 cninfo (备) 🆕      └─ 新浪全球财经 (备) 🆕    └─ 财联社 Playwright (备) ⏳
      │                          │                          │
      └────── 写 events 表 (主键: source + source_id) ─────┘
                  │
                  └─ 主源失败 → 自动降级到备源 → 仍失败 → watchdog 告警
```

## 三阶段

### Phase 1: 巨潮 cninfo 公告流 (推荐先做)

**为什么**: 巨潮是证监会指定的 A 股公告官方源, **比东财更权威**, 且
**结构化数据** (无需正则解析 HTML)。能解**"东财挂了"这个根问题**。

**实现**:
1. 新建 `app/cninfo_stream.py`
2. POST `https://www.cninfo.com.cn/new/hisAnnouncement/query` (正确 headers)
3. 入 `events` 表 (source='cn_announcement_cninfo', source_id=announcementId)
4. 失败重试 + 60s 硬超时 (参考 announcement_stream 写法)
5. scheduler 绑 `cninfo_stream` 15min

**坑**:
- 巨潮需要 POST + `column=szse/sse` (板块), 不能直接 GET
- 需要 Referer 头 (`http://www.cninfo.com.cn/`)
- 公告 detail 需要二次 GET (但基础 list 已含标题 + 时间)

**时间**: 1-2 小时 (探接口 + 写代码 + 测)

### Phase 2: 新浪全球财经 (港美股新闻备)

**为什么**: yfinance 国内访问不稳, 港美股新闻流是 0 条的根因。
新浪全球财经 (`https://finance.sina.com.cn/world/`) 有港美股板块。

**实现**:
1. 新建 `app/sina_global_stream.py`
2. 抓 `https://feed.mix.sina.com.cn/api/roll/get?pageid=153&lid=...`
   (lid 加 1686 国际财经 / 2513 美股)
3. symbol 启发式匹配持仓 (tsla, aapl, baba, 0700.hk 等)
4. 失败降级到 yfinance (主源还在就优先主)

**坑**:
- 新浪全球财经覆盖不如 yfinance 全 (有些小盘股没)
- 启发式匹配容易漏 (L1 关键词即可, L2 LLM 实体识别)

**时间**: 1 小时

### Phase 3: 财联社 SPA 化 (远期)

**为什么**: 财联社是 A 股最快电报源 (快讯), 但 Next.js SPA 纯 HTTP
抓不到结构化数据, 必须 Playwright。

**实现**:
1. 引入 playwright (`pip install playwright && playwright install chromium`)
2. 抓 `https://www.cls.cn/telegraph` 渲染后的 DOM
3. 入 `events` 表 (source='cls_telegraph_official')

**坑**:
- 镜像 +300MB (chromium)
- 启动慢 (5-10s)
- 需要 headless 模式 + 反爬绕过

**时间**: 4-6 小时 (含 playwright 调试)

**建议**: **等 Phase 1+2 跑顺再启动**, 避免一次推太多新源。

## 优先级决策

**用户当前最痛的**: 东财公告流挂了 = 系统关键功能失效

**建议路径**: **先做 Phase 1 (巨潮)**, 因为:
- 公告是监控的**第一信号源** (政策 / 重组 / 财报)
- 巨潮是**官方源**, 数据质量 > 东财
- 工作量最小 (1-2h), 立刻见效果

**Phase 2 港美股** 第二优先 (影响小, 用户港美股持仓少)
**Phase 3 财联社** 第三优先 (锦上添花, 不是必须)

## 当前选择

需要用户确认:
- [ ] 干 Phase 1 (巨潮) ✅ 推荐
- [ ] 干 Phase 1 + Phase 2 (巨潮 + 港美股备) ✅ 强烈推荐 (1.5x 工作量)
- [ ] 只干 Phase 1
- [ ] 先观察东财, 等它自己恢复 (不推荐 — 单点风险没解)
- [ ] 全干 1+2+3 (4-8h, 大改)
