# 打印历史 JSON null 容错

独立分支：`feature/fix-print-history-null-20261103`。
基线：当前已审且已发布测试版 `840f7aaf07a0421d6ee94882bcd24f52499d295d`。未合入待审的打印历史 Map 性能分支。

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

注意后续整合：独立待审性能候选 `0b69e34` 有一项差分测试刻意冻结了旧 null 抛错行为，以证明性能批没有改变语义。本修复未修改该分支；两批均放行后的整合需相应更新该“旧错误仍抛出”的断言并重新审核，不能混入本批或放宽其余差分。

## 实际页面阶段

业务修复及 API 专项已先推送 `4a9ea151cb7bb1f31c73faaa8a4cb0a3f132a3c7`。构建通过，随后使用真实构建 `/002`、Worker、Chromium 的新增 `work-print-null-browser.mjs`：

- 1440 PC 和 390 手机宽度均通过：四个合成批次列表正常，只有 null 历史的单显示需重印；较早有效当前记录＋最新 null 的单保持已发起打印，并显示原最近事件人。
- 从实际列表进入批次、单张详情和打印按钮，真实 PDF 包含完整要求，二维码存在；成功后页面变已发起打印，仅增加一条正常对象审计。
- 两轮后原需求/单号/工时/成果/出入库/附件快照相同，所有原 null/有效审计行完全保留，页面无 JS 错误。
- 原 `work-need-groups-print-browser.mjs` 同轮通过：55/7 分组、100 行跨页备注、中韩文文件名、QR、旧单及历史出库来源 PDF、文档指纹和业务记录不变。

未改旧浏览器脚本的断言或等待，也未修其它批次记录的卡片高度问题；本阶段执行上述功能/PDF脚本，不将未执行的完整视觉脚本报告为通过。测试截图及 PDF 仅存本地 `/tmp/ck-work-print-null-browser`、`/tmp/ck-recovery-print`，不上传公开仓库。

## 最终开发验证

固定业务源码：`4a9ea151cb7bb1f31c73faaa8a4cb0a3f132a3c7`；后续 `b84a69cd6a5392fc7189c0b289a566a657084512` 仅增加已验证的实际页面测试及说明，其后仅补本节。生产代码仍仅原历史读取表达式中的两个可选链；共享解析器、文档哈希、原 SQL/排序、打印摘要及全部写入路径与 `840f7aa` 字节一致。

- 构建成功、`git diff --check` 通过；全量 **1644 通过、0 失败、0 跳过、0 取消**，约 298 秒，包含现有部分到仓/权限/事务/草稿/照片/返岗派审单元回归。
- 本批原生本地 workerd D1 **6/0/0**；此前原生 D1 失败场景已转绿，所有旧 null 审计、正常追加对象、幂等回执、权限和原业务快照断言均保留。
- 两份实际浏览器脚本成功：新增空快照 PC 1440/手机宽度 390 的列表→详情→打印→真实 PDF；原分组/旧单/历史出库来源完整 PDF。无页面 JS 错误、无原数据覆盖。
- 本批未执行旧完整密度/缩放视觉脚本，不声称它已通过或其既有高度问题已修复。浏览器为本地 Chromium 视口模拟，不是实体手机或国内网络验收。
- 未合入待审性能分支，未合测试、未部署；测试分支仍 `840f7aa`、main 仍 `4671f49`。提交候选供固定独立 QA，发布需另行安排。

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
