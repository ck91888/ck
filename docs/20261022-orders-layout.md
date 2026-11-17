# 单子数据紧凑排版验收

基线：远端测试分支 `upgrade/sop-safe-rollout-20260917` 的 `9d60a1ccf5810908b4f67e11b972ead30a0dcb31`。范围仅为 `/shuju/` 列表显示与 staging 发布标识 `20261022-orders-layout`。保留历史修复，不修改统计、查询、筛选、分页、导出实现、数据库、鉴权或正式环境。

- 业务阶段与任务类型合列、任务开始/结束时间合列，单号与派审员分层显示；过长单号可展开。
- 结果摘要最多显示3个有标签的指标，优先非零值；全零记录显示明确的0和零值数量。缺失记录仍明确“不等于零产出”。完整指标、零值、长备注及口径说明均可展开/收起，内容区可滚动并可键盘聚焦。原始 API 行保持完整，未作截断或改写。
- “任务累计结果 / 整任务累计 · 非本期产量”与“本期工时 / 任务累计工时”分开标示。待核入口及原有完整统计说明保留。
- 桌面内容利用宽屏，结果列至少300px；1024px仅表格容器横滚，详情操作列固定右侧。手机改为有标签卡片，详情按钮占整行。

## 验证

1. 专项：`CK_JSDOM_MODULE=/tmp/ck-orders-deps/node_modules/jsdom/lib/api.js node --test tests/orders-layout.test.mjs tests/actual-work-report.test.mjs tests/dashboard-load-performance.test.mjs`，19项通过。
2. 全量：`CK_JSDOM_MODULE=/tmp/ck-orders-deps/node_modules/jsdom/lib/api.js node --test --test-concurrency=4 --test-timeout=120000 tests/*.test.mjs`，1286项通过，0失败、0跳过。
3. `node tests/orders-layout-browser.mjs`：真实Chromium、真实Worker、合成SQLite、同源localhost请求。1600/1440/1024/390/320全部通过。覆盖长中韩文标签/备注、长业务单号/派审员、13项混合结果、13项全零、无结果、展开/收起/键盘操作、200条首屏、下一页5条及最后一条详情、任务类型筛选、详情点击命中测试及横滚。
4. 五种宽度均无页面横向溢出。桌面最复杂默认行高126.5px；1600/1440/1024结果列分别约359/321/300px。1024表格容器可滚328px，操作列始终在可见区域；手机卡片约560px（合成最复杂记录，展示全部任务事实），结果摘要仍有限高。
5. 实际点击下载本期任务明细、同类数量样本、工时人员段以及建单日期追溯明细。与基线客户端同一合成数据库的下载逐项比对：前三份字节相同，追溯明细仅规范化其服务端每次生成的“截止时间”响应时间列后完全相同；其余所有字段、长备注、零值、完整结果JSON全部保留。
6. JS语法、`node worker-v2/build-sop-staging.mjs`、指定 `wrangler.sop-staging.toml` 的 `wrangler deploy --dry-run`、`git diff --check`通过。浏览器无异常，合成结果表在读取/导出前后完全相同。

本地证据：`/tmp/ck-orders-layout/`（截图、合成CSV、`evidence.json`）；`/tmp/ck-orders-{targeted-final,full,browser-final,dry-run}.log`。未下载/提交用户截图、真实个人数据、真实业务CSV或凭据。

## 发布与验证边界

提交前再次fetch，普通快进推送到指定测试分支，由既有Cloudflare Workers Builds部署 `ck-v2-api-sop-staging`。按最终提交SHA核对 `Workers Builds: ck-v2-api-sop-staging` 成功check-run与Version ID；不能只凭推送称发布成功。

执行环境访问 `https://sop-test.ck91888.cn/shuju/` 被代理以 `CONNECT tunnel failed, response 403` 拒绝；未绕过。页面交互验收限于本地真实Worker与合成数据，线上真实会话及页面资源未验收。main、正式Worker、现有业务记录均不操作。
