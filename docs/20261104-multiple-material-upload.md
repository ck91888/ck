# 多份作业资料上传：隔离修复记录

基线：测试分支 `9650cd18ff7e89199934b8e8cd5d6a829f0fb350`。
功能分支：`feature/fix-multiple-material-uploads-20261104`。不含受阻 FE-03/04；不部署、不使用真实资料。

## 第一阶段

真实 Worker、内存 SQLite/R2 和实际构建的 `/002/` 页面，合成单组 6 份 PDF。第三次附件请求中断时，原版出现 `Failed to fetch`，服务器保留前 2 份草稿，入库计划为 0，后续文件不再上传。接受弹窗后按钮恢复，不能据截图断言永久卡死。截图像素由总管核实；此环境 Library 本地下载失败，仅取得 OCR，未直接查看像素。

新增附件专用传输器：仅允许有原请求编号的附件上传；不自动重试创建计划等业务写入。原文件、目标及编号保持不变，暂时性连接/网关错误最多尝试 3 次，明确权限/业务拒绝不重试。传输无进展 90 秒或总计 15 分钟停止；有进展时不因普通 60 秒阈值中止。显示文件进度及重试状态，限制全页面最多 2 个附件请求。登录身份变化停止，保留原编号供核查。

未附着资料草稿采用最多 2 路上传，失败后等待在途文件结果，成功的文件继续保留；已有 CKPlanSave 会话维持顺序。普通作业/已有组等共享版本号的入口仍须串行，下一阶段接入同一传输器。

验证：`node --test tests/material-upload.test.mjs`：23 通过、0 失败、0 跳过；构建通过；`node tests/multiple-material-upload-browser.mjs`：1440/390 两宽通过，每组 7 次请求、6 个编号、6 个资料及 1 张计划。首次修复测试曾把正常“已创建”提示当成错误；修正测试区分成功提示后通过，原日志留在隔离运行目录。

未验证：国内真实网络、真实设备、全量回归、原生 D1 及其余上传入口。不能据合成断线认定现场网络根因。

## 第二阶段

普通客服资料、办公室代录仓库反馈、本批总明细、已有组资料及旧批量资料面板接入同一传输器。已有作业共用 revision，维持逐份顺序；上传时固定当前文件及资料类型，失败保留已确认标记、未完成文件和原编号，不再提示刷新后重新选择。登录身份变化时停止，恢复原身份后核查重试。

实际页面 1440/390 × 普通、本批、已有组、批量面板共 8 组通过：已提交但丢响应时 7 次上传请求、6 个编号/附件；连续 3 次失败再手动继续时 9 次请求、6 个编号/附件；组内文件顺序及关联不变。Node/原生 workerd D1 各 3 组通过，覆盖 6 份独立草稿、原编号/原内容重放、冲突内容拒绝、逐版本追加和未授权拒绝。首次 API 夹具误用了 `draft_id` 而实际 schema 为 `draft`，及未配置本地 Miniflare 依赖路径，修正夹具/运行配置后均通过；原失败日志留存，不改业务 schema。

合成性能：6 份文件，每请求额外 200ms 延迟，各 3 轮中位数。1KB：原串行 1357ms → 686ms；1MB：2150ms → 1145ms。物理附件请求峰值 1 → 2，72 份草稿记录无重复。仅说明受控并发在此延迟模型下减少等待；不代表国内真实带宽或现场保证。共享版本的已有作业不做并发。

兼容范围：当前 workChain 打开时旧入/出库资料入口由既有服务端规则拒绝并引导到作业需求。没有给不支持请求幂等的旧附件/Excel明细/照片接口套自动重试。未改 Worker、数据库、对象存储规则或照片压缩。

全量及既有浏览器回归尚在执行。本阶段待独立 QA，不得发布。

## 保存窗口兼容回归

保留 `CKPlanSave.limits.uploadMs` 原配置及上传超时后的显式续作，不让新传输器改变原保存会话的超时合同。其余允许重试的暂时性错误仍有限重试。增加“文件已传送，等待服务器确认”，传输 100% 不等于服务器确认成功。

专项 Node 31/0/0（传输 28 + API 3）；原生 D1 API 3/0/0。既有 `inbound-create-receipt-browser.mjs` 两宽 12 组、`plan-save-resilience-browser.mjs` 10 组、`cargo-create-resilience-browser.mjs` 4 组及照片压缩原脚本通过。照片保持 3,462,248 → 403,880 字节、2048×1536，条码/QR 解码保留；不改压缩源码。

原两个 resilience 脚本初轮分别超时，原因是单次 500 已被新重试恢复，原“显示失败”条件不再触发；仅将故障注入延续到该文件 3 次尝试，保留原断言和等待时长，断言总请求数 3 → 5，仍证明首次成功文件不重传、显式续作只上传缺失文件。另保留保存窗口超时配置兼容回归。没有削弱业务断言或修改受阻 FE-03/04 源码。

## 最终开发验证

业务源码固定提交 `dc47f5eef1a10b630d0a33f108fbe039085bc4df`，本节仅补验证记录。

- 完整 `CK_JSDOM_MODULE=<隔离依赖>/jsdom/lib/api.js node --test --test-concurrency=2 tests/*.test.mjs`：1951 通过、0 失败、0 跳过，380 秒。首轮 1943/0/0 之后新增 API 和兼容专项，末次包含全部 1951 项。
- `node --test tests/material-upload.test.mjs tests/material-upload-api.test.mjs`：31/0/0；`CK_P1_NATIVE_D1=true CODEX_PRIMARY_RUNTIME_NODE_MODULES=<隔离依赖> node --test --test-concurrency=1 tests/material-upload-api.test.mjs`：3/0/0。
- 实际页面两宽六 PDF 创建 2 组、普通/本批/已有组/批量面板 8 组；既有 receipt 12 组、plan-save 10 组、cargo-create resilience 4 组；照片压缩原脚本及 staging 构建通过。所有浏览器为本地 Chromium，非真实手机、非国内网络。
- 本地性能脚本 `tests/material-upload-performance.mjs` 12 轮、72 份合成草稿通过，指标见第二阶段。不测试/更改线上数据，不增加数据库迁移或 Worker 业务改动。

证据由开发环境保留在 `/workspace/scratch/ck-p1-test-runtime/multi-upload-*.log`，包含原始失败。最终主要记录：`multi-upload-full-final.log`、`multi-upload-candidate-targeted.log`、`multi-upload-api-d1-fixed.log`、`multi-upload-grouped-final.log`、`multi-upload-entrances-final.log`、`multi-upload-regression-receipt.log`、`multi-upload-regression-plan-save-fixture-v2.log`、`multi-upload-regression-cargo-fixture-v2.log`、`multi-upload-photo-regression.log`、`multi-upload-performance.log`。全是隔离合成数据，没有用户截图或真实资料。

尚待独立 QA 与部署；测试分支保持 `9650cd1`，main 保持 `4671f49`。现场上传请求的地址/状态/文件大小和国内网络原因尚未知；不能声称复现了现场全部原因，也不把本地模拟收益当现场保证。

## QA 总截止阻断修复

QA 指出的 P2 已在本地 Chromium 确认：原 `a2bfbde` 设置单份上传总时限 200ms、重试间隔为 0，服务端提交后扣留响应，最终 3 次同编号请求约 610ms 才停止，服务器只有 1 份资料。原始失败为 `multi-upload-deadline-browser-original.log`；确定性时钟初始 7 个专项亦全部失败，保留于 `multi-upload-deadline-original.log`。原全量通过不能覆盖此失败。

每次 `send` 现在只建立一个基于 `performance.now()` 的绝对截止，包含全局排队、所有请求及重试等待；XHR 只获得剩余预算。到期统一终止，不再重试；移除已过期队列项、中止当前 XHR、取消等待计时器，保留原文件、编号及已确认标记。原保存会话的 `uploadMs` 同样成为该份资料的统一上限；超时后仍须由用户明确继续，新的继续操作使用原编号核查已有结果。不重试计划创建业务，不改 Worker、对象存储或历史数据。

定时器受浏览器调度影响，通知可能稍晚，但每次排队出队、请求开始和确认均检查绝对截止，不能因迟到回调继续发送或接受超期确认。

新增确定性时钟 9 项（200ms单一总预算、剩余90ms、退避/排队到期、排队/退避取消、进度不续期、已确认批次续传、199ms成功、非法预算），与既有28项传输及3项API合计 **40/0/0**。实际 Chromium 1440/390 × 丢响应、重试后慢确认、退避、排队、取消共 **10组通过**；预算200ms在201–203ms结束，丢确认原编号重放仍是同一资料/对象；取消约31ms完成。证据为 `multi-upload-deadline-targeted.log`、`multi-upload-deadline-browser-final.log`。全量与既有浏览器回归在执行，尚未复审或发布。

后续排队：用户要求新增公司 `리드인`，旧存储值 `CKIK` 兼容显示为 `한국인력개발`，需覆盖历史、筛选、工牌、权限弹窗、CSV/Excel且保留审计原值；本批不混入公司调整。

### 截止时间修复最终开发验证

业务源码固定 `02d0316d53165241e10d18f3e9cd4a125ab45a6e`。完整 Node 回归 **1960/0/0**，350 秒；专项40/0/0，原生 workerd D1 API3/0/0；新截止浏览器10组、既有两宽六PDF2组/四入口8组/receipt12组/plan-save10组/cargo-create4组及原照片压缩脚本全部通过，staging构建通过。修复后无失败、跳过或未收尾的验证任务。

本轮日志：`multi-upload-deadline-full.log`、`multi-upload-deadline-targeted.log`、`multi-upload-deadline-api-d1.log`、`multi-upload-deadline-browser-final.log`、`multi-upload-deadline-sixpdf.log`、`multi-upload-deadline-entrances.log`、`multi-upload-deadline-receipt.log`、`multi-upload-deadline-plan-save.log`、`multi-upload-deadline-cargo.log`、`multi-upload-deadline-photo.log`，均在上述隔离运行目录；原610ms失败与7项时钟失败仍保留。

等待独立QA复审，无部署授权执行。本轮不更改测试/main、线上业务数据、重置功能或受阻FE-03/04；公司配置继续排后续独立批次。仍未验证国内真实网络、远端Worker实际部署及真实设备；浏览器执行/冻结造成的通知延迟受平台调度影响。
