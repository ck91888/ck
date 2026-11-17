# 打印历史 JSON null 容错

独立分支：`feature/fix-print-history-null-20261103`。
初始基线：已审且已发布测试版 `840f7aaf07a0421d6ee94882bcd24f52499d295d`。首阶段未合入当时待审的打印历史 Map 性能分支；后续已审发布后的整合见末节。

## 最小改动与业务语义

`sop_events.after_json` 为文本 `null` 时，JSON 解析成功却得到 JS null。原历史读取随后访问 `document_key` 而报错；只修第一处后，最新事件的 `requirement_version` 仍会报错。本次仅在这两个历史读取位置使用可选链，不改共享解析器、SQL、查询参数/次数、事件排序、文档哈希、打印摘要、权限、幂等或写入代码。无 schema 迁移，无历史数据清理或重写。

- 空快照不能证明当前文档版本，故不增加 `current_count`；没有有效匹配时按现有其他损坏 JSON 的语义显示“需重新打印”。没有历史仍是“打印未记录”。
- 其他合法历史的版本匹配继续生效，null 位于最新或较早事件均不会覆盖有效证据。
- 原请求行、请求次数、实际记录人/时间保留；未知要求版本不伪造为 0 或当前版本。
- 正常打印仍由服务端生成对象快照，仅追加审计，不改变需求、库存、工时、成果或作业状态。

## 阶段回归

修复前新增 Node API 场景 **0 通过、6 失败、0 跳过**；原生本地 workerd D1 最小场景 **0/1/0**，均为 null 的 `document_key` 错误。未放宽原断言。

修复后相关 Node **28/0/0**（新增 6 项＋原打印/历史出库来源/作业计划）；新增 6 项在实际本地 workerd D1 上 **6/0/0**：

1. 字面/带空白 JSON null；详情、全列表、普通健康条件筛选、空筛选及 SQL 批次范围均可读。
2. 最新 null＋旧的当前版本匹配：保持已发起打印，保留最近事件人/时间，未知版本不伪造。
3. 较早 null＋最新有效记录：当前版本次数、最新要求版本和摘要保持。
4. 旧有效版本＋null：继续提示重印，真正请求当前版本后才变为已发起；保留原审计及原业务数据。
5. 真实仅直发权限日当：列表不泄漏大货单、详情和打印仍拒绝，办公室读取正常。
6. 同请求并发打印只追加一条对象快照、返回同回执；改内容复用请求号仍拒绝；原 null 行不动。

所有数据为隔离合成夹具，Node 使用内存库，D1 使用本地 workerd，未查询或修改线上数据。阶段测试日志位于本环境 `/workspace/scratch/ck-p1-test-runtime/print-null-fix-*`，保留修复前失败与修复后通过。后续进行构建、全量和实际打印 UI 回归，当前未申请发布。

首阶段已记录整合边界：性能候选 `0b69e34` 有一项差分测试刻意冻结了旧 null 抛错行为，以证明性能批没有改变语义。后续整合只调整这一已明确授权变更的断言，保留其余差分及冻结的旧算法。

## 实际页面阶段

业务修复及 API 专项已先推送 `4a9ea151cb7bb1f31c73faaa8a4cb0a3f132a3c7`。构建通过，随后使用真实构建 `/002`、Worker、Chromium 的新增 `work-print-null-browser.mjs`：

- 1440 PC 和 390 手机宽度均通过：四个合成批次列表正常，只有 null 历史的单显示需重印；较早有效当前记录＋最新 null 的单保持已发起打印，并显示原最近事件人。
- 从实际列表进入批次、单张详情和打印按钮，真实 PDF 包含完整要求，二维码存在；成功后页面变已发起打印，仅增加一条正常对象审计。
- 两轮后原需求/单号/工时/成果/出入库/附件快照相同，所有原 null/有效审计行完全保留，页面无 JS 错误。
- 原 `work-need-groups-print-browser.mjs` 同轮通过：55/7 分组、100 行跨页备注、中韩文文件名、QR、旧单及历史出库来源 PDF、文档指纹和业务记录不变。

未改旧浏览器脚本的断言或等待，也未修其它批次记录的卡片高度问题；本阶段执行上述功能/PDF脚本，不将未执行的完整视觉脚本报告为通过。测试截图及 PDF 仅存本地 `/tmp/ck-work-print-null-browser`、`/tmp/ck-recovery-print`，不上传公开仓库。

## 首次候选完整验证（840f7aa 基线）

固定业务源码：`4a9ea151cb7bb1f31c73faaa8a4cb0a3f132a3c7`；后续 `b84a69cd6a5392fc7189c0b289a566a657084512` 仅增加已验证的实际页面测试及说明，其后仅补本节。生产代码仍仅原历史读取表达式中的两个可选链；共享解析器、文档哈希、原 SQL/排序、打印摘要及全部写入路径与 `840f7aa` 字节一致。

- 构建成功、`git diff --check` 通过；全量 **1644 通过、0 失败、0 跳过、0 取消**，约 298 秒，包含现有部分到仓/权限/事务/草稿/照片/返岗派审单元回归。
- 本批原生本地 workerd D1 **6/0/0**；此前原生 D1 失败场景已转绿，所有旧 null 审计、正常追加对象、幂等回执、权限和原业务快照断言均保留。
- 两份实际浏览器脚本成功：新增空快照 PC 1440/手机宽度 390 的列表→详情→打印→真实 PDF；原分组/旧单/历史出库来源完整 PDF。无页面 JS 错误、无原数据覆盖。
- 本批未执行旧完整密度/缩放视觉脚本，不声称它已通过或其既有高度问题已修复。浏览器为本地 Chromium 视口模拟，不是实体手机或国内网络验收。
- 首阶段完成时未合入待审性能分支，未合测试、未部署；当时测试分支为 `840f7aa`、main 为 `4671f49`。首阶段候选提交独立 QA，之后因新测试基线发布继续做下述整合。

复现命令：

```sh
node --test tests/work-print-null.test.mjs tests/work-need-print.test.mjs tests/outbound-work-print.test.mjs tests/work-plan-list.test.mjs
CK_PRINT_NULL_NATIVE_D1=true CODEX_PRIMARY_RUNTIME_NODE_MODULES=/已安装依赖/node_modules node --test --test-concurrency=1 tests/work-print-null.test.mjs
node worker-v2/build-sop-staging.mjs
CK_JSDOM_MODULE=/已安装依赖/node_modules/jsdom/lib/api.js node --test --test-concurrency=2 tests/*.test.mjs
node tests/work-print-null-browser.mjs
node tests/work-need-groups-print-browser.mjs
```

修复前后的 Node/D1 日志、构建、最终全量及浏览器日志均保存在本环境 `/workspace/scratch/ck-p1-test-runtime/print-null-fix-*`。仅提交本批 1 个业务文件、2 个合成测试文件、此说明，不提交凭据、业务数据、数据库、截图或 PDF。

## 已审性能发布与整合

首阶段候选 `78bf0a4c94ad211e13ed84fb3958fb9968255f89` 已远端保存。按总管授权，先将独立 QA 放行的精确性能候选 `0b69e34e5f2c5ea7c154499ccf6313131f4df9a2` 从 `840f7aa` 安全快进至测试分支，自动部署成功；没有发布 null 修复。成功检查 `https://github.com/ck91888/ck/runs/125878944946`，Build `188225b1-838d-4376-818d-7306b4133fae`，Worker `e05884f3-574e-47c5-8945-2c9e4eee4165`。版本标识仍 `20261103-audit-p1-transactions`，生成清单以精确 commit 区分。只读站点 GET 仍遭代理 CONNECT 403，未绕过。main 保持 `4671f49b9dd74c729e738abbe1440223bdf6af0f`。

随后在本独立 null 分支整合这份已审且已发布基线。仅历史计算同一行发生合并冲突，显式保留完整 Map 分桶及两个可选链；摘要/写入、SQL、查询顺序、状态、分页和数据均不改。

原性能测试唯一刻意要求新算法也抛 null 异常的案例，改为检查最新/旧 null 被容错后：原有效匹配数、摘要和此前逐单赋值保留，请求总数加 1，最新空快照人/时间保留且不造要求版本。冻结的旧算法仍断言抛 TypeError，其余完整输出/SQL 冻结哈希和非 null 差分全部保留，并非删除失败用例。

整合阶段先通过相关 Node **37/0/0**，再对该整合候选执行下述万单 API 输出/查询对照、全量、原生 D1 与实际页面。null 整合候选尚未获独立 QA 放行、未合测试、未部署。

## 整合后验证（当前 0b69e34 基线）

整合源码 `09ea5b89dc8b4c8c2e8386d46a7fcac9868f2a83` 已推送；它同时包含原 null 候选 `78bf0a4` 和当前已发布性能基线 `0b69e34`。相对后者的业务差异仍严格只有两个 `?.`，保留完整 Map、原 SQL/排序、所有查询/写入与权限实现。

- 相关 Node **37/0/0**，包括 100/1000 的旧算法逐字段对照、万单完整输出与 SQL 冻结哈希、最新/旧 null 的精确状态/计数/元数据断言。
- 再跑真实登录完整 API 100/1000/10000 及万单末尾筛选。正常（非 null）合成历史的四份响应全文、载荷字节、SQL 文本顺序、查询参数、计数及分页与已发布性能版指标完全一致，原业务表快照不变。原单轮整合样本万单首屏 1904.491ms、末尾筛选 2933.471ms；仅作本地样本记录，不据此承诺现场延迟。新增 null 语义单独由专项/API/D1 覆盖。
- 原生本地 workerd D1 合集 **15/0/0**：本批 null 6 项、历史出库来源打印 8 项、原打印历史 API 1 项，保留所有审计/权限/幂等/数据不变断言。
- 构建成功；三份实际 Chromium 脚本全部 exit 0：`work-print-null-browser.mjs`（PC 1440/手机视口 390、真实 PDF）、`work-need-groups-print-browser.mjs`（分组/旧单/历史出库来源 PDF）、`work-plan-list-browser.mjs`（2 功能流程、9 组宽度/缩放，含 320/390）。未改旧等待或断言；未执行其它完整打印密度脚本，未声称修复其旧高度问题。

整合源码上的最终全量 **1653 通过、0 失败、0 跳过、0 取消**，约 371 秒。其后仅更新此说明，没有改业务或测试源码。`git diff --check` 通过，null 候选未发布，提交固定独立 QA 重新审核。所有测试为本地隔离合成夹具，未访问线上数据库或改写真实记录。

相对当前已发布 `0b69e34` 只有 5 文件差异：两处空值保护的业务文件、此前旧 null 异常断言的专项更新、两个新增合成测试、此说明。部署配置、schema、SQL、共享解析器、文档哈希、打印摘要和全部写入路径未变。

整合最终命令：

```sh
CK_PRINT_HISTORY_BASELINE_METRICS=/本地已审版本指标.json CK_PRINT_HISTORY_METRICS=/tmp/null-integrated-api.json node tests/work-print-history-api-performance.mjs
node worker-v2/build-sop-staging.mjs
CK_JSDOM_MODULE=/已安装依赖/node_modules/jsdom/lib/api.js node --test --test-concurrency=2 tests/*.test.mjs
CK_PRINT_NULL_NATIVE_D1=true CK_PRINT_HISTORY_NATIVE_D1=true CK_OUTBOUND_PRINT_NATIVE_D1=true CODEX_PRIMARY_RUNTIME_NODE_MODULES=/已安装依赖/node_modules node --test --test-concurrency=1 tests/work-print-null.test.mjs tests/work-print-history-api.test.mjs tests/outbound-work-print.test.mjs
node tests/work-print-null-browser.mjs
node tests/work-need-groups-print-browser.mjs
node tests/work-plan-list-browser.mjs
```

整合日志/指标保留在本环境 `/workspace/scratch/ck-p1-test-runtime/print-null-integrated-*`，包括 `full.log`、`d1.log`、`api-metrics.json`、`api-performance.log`、三份 `*-browser.log` 和 `browser-results.json`。首阶段的失败和通过日志仍在 `print-null-fix-*`；未删除原证据。
