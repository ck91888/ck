# 2026-10-13 合并候选：全量核对证据与浏览器验收

本任务仅增加 `tests/unified-check-fixture.mjs` 和 `tests/unified-check-browser.mjs`，没有修改业务模块、远端、main 或真实业务数据。所有请求限于同一 localhost，Worker 和 staging-entry 从 `CK_SMOKE_ROOT` 动态导入，数据库为合成 SQLite 内存 D1。

## 本阶段已通过

- **6001、20001 条日志两套 API 证据验收**：全量条码聚合、托盘数量、分类计数、最后扫描时间均与独立 fixture 计算一致。每批 248 个托盘，主条码覆盖 242 个托盘，包含原 5000 条截断会丢失的最早孤立托盘。
- **分页完整性**：读取 200／200 条两页，无重复；读取最后一页 1 条，准确返回最早孤立托盘日志。页面初始 100 条，实际 UI 下一页为 101–200，可返回上一页。
- **PC 1440px、手机 390px 两套实际 /002 流程**：分类筛选数量、九列表格、数量差异、有效数量、托盘展开、最近扫描、分页失败保留旧内容、重试、离开详情后的迟到分页保护全部通过。
- **最终 CSS 八组原生浏览器缩放**：PC 1440／1280 在 100／125／150%；手机 390／320。均为九列、八个当前条目；所有页面宽度不超视口，筛选按钮位于视口内。最终主表韩文表头、时间、状态按整词换行。
- **合并候选四项原有导航回归**：PC／手机分别延迟核对和问题点 SOP 入口读取，切到作业计划后放行请求，筛选均保留 1 → 1。
- 无 JS 错误；只读证据、筛选和分页没有改变 `sop_records` 的 state/revision、`sop_events` 或扫描日志。

合成分类包含完成、少扫、超扫、未扫、数量改版待复核、改期移入待复核、已撤销、清单外和重复尝试。正常部分扫描归少扫；改版／移期优先显示待复核。一次正常＋一次 duplicate 的条码保留当前核对一致，另显示重复尝试；全部扫描与差异计入历史尝试，有效核对独立显示。无扫描日志的托盘、最近扫描显示“未记录”。

20001 条主 fixture 的当前计划 20006、全部扫描 20001、有效核对 19994、扫描差异 -5。全部当前条目 8、完成 2、异常 6、少扫 1、超扫 1、未扫 1、清单外 1、待复核 2；差异筛选 7（包含 duplicate 尝试导致的数量差异），已撤销另查 1。

## 可重复运行

先在业务根构建 staging assets：

```bash
node worker-v2/build-sop-staging.mjs
```

从本测试目录执行完整专项：

```bash
CK_SMOKE_ROOT=/workspace/ck-ui-overlap-audit-1013 \
CK_CHECKS_OUT=/tmp/ck-unified-check-merged \
node tests/unified-check-browser.mjs
```

仅在 CSS 调整后重跑可视验收，不重复 API／功能断言：

```bash
CK_SMOKE_ROOT=/workspace/ck-ui-overlap-audit-1013 \
CK_CHECKS_VISUAL_ONLY=1 \
CK_CHECKS_OUT=/tmp/ck-unified-check-merged-visual-final \
node tests/unified-check-browser.mjs
```

合并后原有四项导航回归：

```bash
CK_SMOKE_ROOT=/workspace/ck-ui-overlap-audit-1013 \
CK_SMOKE_RACE_ONLY=1 \
CK_SMOKE_OUT=/tmp/ck-ui-entry-smoke-merged-races \
node tests/ui-entry-smoke-browser.mjs
```

## 证据与边界

持久证据：`/workspace/ck-unified-check-browser-evidence-20261013`。`functional/result.json` 记录两套 API、两套流程；`visual-final/result.json` 记录最终局部 CSS 的八组验证及源码哈希，含实际 `worker-v2/check-evidence.js`；`navigation/result.json` 记录四项已复现导航回归。最终截图为本轮新生成的 40 张，另保留功能流程截图和导航前后截图。

人工查看最终 14 张截图：八组条码表、390／320 的概览、托盘展开、日志第二页。主表韩文整词显示已修正；320／390 的**托盘子表**韩文表头 `바코드`、`현재 계획` 仍可跨字符换行，是本阶段留下的轻微排版项，已反馈主任务，未改业务模块扩大处理。

五应用的 94 个入口／模式快照是此前统一详情第一版结果，见 `docs/20261013-ui-entry-smoke.md`。本阶段按主任务要求重跑关键导航和新核对专项，不能将旧快照称为最终合并版全系统重新验收。

本脚本没有执行扫描写入、修改数量、撤销、改日期、异常复核、保存轮次或最终交接。相关写流程和角色权限由主任务专项测试负责。浏览器为 Chromium 原生缩放及手机视口，实体 iPhone／Safari 尚未实测。本任务没有部署，仍需主任务合并测试提交并核实测试 Worker 的部署终态。
