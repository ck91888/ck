# 作业计划打印历史内存索引

独立分支：`feature/perf-work-print-history-20261103`。
基线：已审且已发布测试版 `deb5f249246156bae532fe50c8148c2cd125c3e4`。不包含独立待审的历史出库来源打印修复。

## 阶段一

原 `workNeedPrintFacts` 为每份作业单遍历全部打印事件，之后再次遍历自己的历史以计算当前版本打印数。现仅在查询后按 `record_id` 分桶，后续从桶内计算，消除不同作业单间的重复扫描。桶内沿用原 SQL 返回顺序，`last`、计数、JSON 解析及状态表达式未改。SQL 文本、参数、查询次数、哈希计算、返回对象、筛选分页和写入路径保持原样，没有新增数据库索引、迁移、数据修复或缓存。

阶段 Node **21 通过、0 失败、0 跳过**：7 项新差分，加原打印 6 项、作业计划 8 项。与冻结的发布算法逐字段比较完整输出、原 SQL/参数、摘要、事件数组不变；覆盖 100/1000 单、同时间事件顺序、多个历史版本、无历史、旧字段缺失、损坏 JSON、原始 JSON 值、重复 ID、混合 kind、特殊 Map 键及严格匹配。

字面 JSON `null` 在发布版原逻辑会抛 TypeError；本轮保留该既有行为和抛错前的逐单赋值顺序，不在性能批静默更改旧数据策略。语法损坏的 JSON 与空字符串仍按原 fallback 计算。

所有输入为隔离合成数据。100/1000/10000 的 helper 与真实登录完整 API 对照、全量和浏览器回归正在后续阶段执行；本阶段不作为发布放行。未合测试、未部署，不接触正式版或真实业务数据。

## 阶段二：规模与完整 API 对照

第一阶段源码已推送 `8403b5427fd5cc35394e8b5a2ca37727dbf94c26`。本环境串行 helper 对照，计时不含输入生成/复制、输出和 SQL 哈希计算：

| 作业单 | 打印事件 | 发布算法 | 分桶算法 | 完整输出及查询参数 |
| --- | ---: | ---: | ---: | --- |
| 100 | 900 | 16.291ms | 26.404ms | 相同 |
| 1000 | 9090 | 612.799ms | 450.702ms | 相同 |
| 10000 | 90900 | 36666.468ms | 1139.690ms | 相同 |

万单历史计算减少约 96.89%；100 单本轮稍慢，小规模不承诺加速。没有 CI 墙钟阈值断言；增加万单完整输出/查询冻结哈希作为回归。

完整 API 夹具通过实际办公室登录、操作人确认与 Worker `fetch` 执行 `sop_need_groups`，数据均在 `:memory:`，可选复制至本地 workerd D1。包含旧字段缺失、多版本、无历史、损坏 JSON、空/数组 JSON；请求前后检查原需求、单号、审计、工时、成果、出入库与附件快照。实际 API 小规模专项 **1/0/0**。

完整 API 计时包含授权、查询、全量历史归集、组装、筛选、分页与序列化；不含准备数据及原记录快照。完整对照结果见下表，不用 helper 降幅代替完整 API 收益。

可复现命令（均不访问站点）：

```sh
node tests/work-print-history-performance.mjs
CK_PRINT_HISTORY_API_MODULE=/已发布deb5f249工作区/worker-v2/index.js CK_PRINT_HISTORY_METRICS=/tmp/print-old.json node tests/work-print-history-api-performance.mjs
CK_PRINT_HISTORY_BASELINE_METRICS=/tmp/print-old.json CK_PRINT_HISTORY_METRICS=/tmp/print-new.json node tests/work-print-history-api-performance.mjs
```

默认 100/1000/10000 单及万单末尾筛选；`CK_PRINT_HISTORY_SIZES` 可选规模。新实现可直接加载旧版指标文件，自动断言完整响应哈希/字节、SQL/参数哈希、总数、顺序所体现的全文与分页一致，仅耗时除外。小规模原生 D1 由 `CK_PRINT_HISTORY_NATIVE_D1=true` 启用。

## 阶段二完整结果与发布基线整合

完整 API 对照已全部完成。先串行运行独立发布版 `deb5f249` 工作区的 Worker，再运行本分支；自动差分通过，读前后原记录快照均相同。

| 作业单 / 请求 | 发布版 API | 新版 API | 减少 | 载荷 |
| --- | ---: | ---: | ---: | ---: |
| 100 / 首屏 | 113.841ms | 47.288ms | 58.46% | 65526 字节 |
| 1000 / 首屏 | 1742.958ms | 194.151ms | 88.86% | 65901 字节 |
| 10000 / 首屏 | 170508.479ms | 1446.125ms | 99.15% | 66356 字节 |
| 10000 / 末尾筛选 | 134051.870ms | 1755.762ms | 98.69% | 1346 字节 |

万单为 90900 条历史事件；四种请求的完整响应哈希、字节、SQL 文本顺序/次数、历史查询参数、总数、返回数和偏移均相同。仍先归集所有作业单再筛选分页。此次是 CPU 重复扫描优化，SQL 仍读取同样的历史行，没有把降低读取行数或现场设备网络速度当作收益。

helper 使用混合来源和分组字段；完整 API 使用独立库存来源组及各状态。两种输入字段形状不同，不能把两者耗时相减当作 CPU 分项。完整 API 速度来自本地 Node SQLite 适配器，原生 workerd D1 另做功能回归；未在真实 D1 万行库或线上压测，不保证 Cloudflare、国内网络或实体手机有同等耗时。

阶段二测试夹具和万单冻结哈希已推送 `3dd393a297e97e21e12e382402951a6b688a44fe`。过程中按总管指令，精确打印候选 `840f7aaf07a0421d6ee94882bcd24f52499d295d` 先独立发布测试站，未带本性能修改。成功检查 `https://github.com/ck91888/ck/runs/125866420776`；Build `2a9930c9-18fe-4cde-8c74-ff345a592c3b`；Worker `da888e69-cf75-4527-bac9-90ef66e02806`。版本标识仍为 `20261103-audit-p1-transactions`，生成清单通过精确 commit 区分。只读站点 GET 受代理 CONNECT 403，未绕过；main 仍 `4671f49b9dd74c729e738abbe1440223bdf6af0f`。

随后将这份已审且已发布打印基线无冲突合入本独立性能分支，整合源码 `d7e40ddf153dad601ed27bf0b727e174c2ca4b1d`。历史索引函数与测速阶段字节一致，保留已发布的出库来源列修复；相对新测试基线仍仅有本批历史分桶业务差异。正在该整合版本上进行全量、原生 D1 和浏览器回归，尚未申请直接发布本性能候选。

## 最终开发验证

固定整合源码为 `d7e40ddf153dad601ed27bf0b727e174c2ca4b1d`，其后仅修改本说明。该源码包含当前测试站已发布 `840f7aa`；历史归集函数与测速阶段 `3dd393a` 字节一致，打印摘要/写入逻辑与已发布 `840f7aa` 字节一致。

- 构建通过、`git diff --check` 通过；全量 **1647 通过、0 失败、0 跳过、0 取消**，约 383 秒。包含前批权限/并发事务、部分到仓、返岗派审、产出草稿与照片压缩单元回归及本批万单冻结哈希。
- 实际本地 workerd D1 **9/0/0**：历史出库来源打印 8 项，加本批真实登录 API 的旧字段/损坏 JSON/历史顺序/计数/过滤后分页/数据不变专项 1 项。
- `work-plan-list-browser.mjs` exit 0：2 组功能流程、9 组宽度/缩放视觉，包含 320/390 手机宽度。
- `work-need-groups-print-browser.mjs` exit 0：分组/旧单/历史出库来源、真实 PDF、100 行跨页备注、中韩文资料名、QR、打印状态与业务记录不变。
- `work-need-print-browser.mjs` 原完整脚本 exit 1，第 49 行同一已知卡片高度断言：1754px/100% 实测 158.984375px，普通卡上限 140px。先前在独立发布版 `deb5f249` 原脚本/原构建也已复现同值，已随打印修复独立 QA 记录；本批未改脚本、样式、断言或等待。不把此轮浏览器报告成全绿。
- 既有 `CK_PLANS_SKIP_VISUAL=1` 开关的功能子集 exit 0，3 组流程：1440/390 打印、弹窗拦截/打印失败、丢响应重试、批量/单张、返回保留条件、要求修改后重印、韩文提示；此子集不覆盖上述视觉失败。

最终测试命令：

```sh
node worker-v2/build-sop-staging.mjs
CK_JSDOM_MODULE=/已安装依赖/node_modules/jsdom/lib/api.js node --test --test-concurrency=2 tests/*.test.mjs
CK_PRINT_HISTORY_NATIVE_D1=true CK_OUTBOUND_PRINT_NATIVE_D1=true CODEX_PRIMARY_RUNTIME_NODE_MODULES=/已安装依赖/node_modules node --test --test-concurrency=1 tests/work-print-history-api.test.mjs tests/outbound-work-print.test.mjs
```

日志及指标位于本环境 `/workspace/scratch/ck-p1-test-runtime/print-history-*`；`helper-metrics.json`、`api-published.json`、`api-indexed.json` 记录性能/全文与 SQL 哈希，`final-full.log`、`final-d1.log`、`final-*-browser.log` 和 `final-browser-results.json` 保留原结果及失败。仅提交合成夹具、源码与说明，不上传数据库、截图、PDF、凭据或真实业务数据。

相对当前测试版 `840f7aa`，业务改动只在 `worker-v2/work-need-print.js` 的分桶部分，外加 6 个合成测试/测速文件和此说明。SQL、查询次数、事件顺序、权限、分页、写入及部署配置均未改。工作已阶段推送至独立分支，性能候选未合测试、未部署，交固定独立 QA；字面 JSON `null` 的既有行为和旧卡片高度失败如前述记录，不借本性能批扩大修复。
