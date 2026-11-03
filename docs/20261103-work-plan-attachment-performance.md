# 作业计划关联与附件检索性能

基线：已发布测试提交 `f640c4fc91cef0f451d30278d774f49b34e6bbb5`。
独立分支：`feature/perf-work-plan-attachments-20261103`。
只使用隔离合成数据，不线上压测，不修改真实业务记录，不直接合测试或部署。

## 阶段一：关联索引

`work-plan-list.js` 预建订单 ID、来源计划、成果计划及历史完成标记的内存索引，替换每组/每计划重复全量扫描。组的订单并集按原数组位置去重和排序，保留重复订单 ID 的不同输入行，也保留同一行同时命中来源与显式链接时的原去重语义。严格相等来源关联与 Set 链接匹配各自的语义保持。

没有修改 SQL、筛选条件、分页位置、组内成员排序或返回字段。`sop.js` 的全量关联后筛选、计数再分页流程未动。材料、状态、实际日期和时间线均参与发布基线输出哈希对照。

本环境单轮隔离合成对照（包括成果、部分上架、历史更正、重复链接、缺失来源及重复订单 ID；此数据形状与此前分析样本不同）：

| 组数 | 原关联耗时 | 新关联耗时 | 减少 | 完整输出与查询参数 |
| --- | ---: | ---: | ---: | --- |
| 100 | 7.648ms | 6.089ms | 20.38% | 相同 |
| 1000 | 90.569ms | 26.305ms | 70.96% | 相同 |
| 10000 | 9625.722ms | 256.696ms | 97.33% | 相同 |

万组关联达到本轮 ≤500ms 的合成目标。计时不含生成输入及哈希；不代表国内网络、真实手机或现场 API 延迟。万组完整中间输出为 12,464,110 字节，新旧完全一致；实际 API 分页载荷及总耗时尚待测量。

验证：`node --test tests/work-plan-indexed.test.mjs tests/work-plan-list.test.mjs`，**13 通过、0 失败、0 跳过**。包括发布版 100/1000/10000 组完整输出和 SQL 参数哈希、重复/并集顺序、来源严格匹配、原成员和资料对象未改、全量筛选后分页、缺失证据及实际业务日期规则。

可复现测速：

```sh
git show f640c4fc91cef0f451d30278d774f49b34e6bbb5:worker-v2/work-plan-list.js > /tmp/ck-work-plan-baseline.mjs
CK_WORK_PLAN_MODULE=/tmp/ck-work-plan-baseline.mjs node tests/work-plan-performance.mjs
node tests/work-plan-performance.mjs
```

`CK_WORK_PLAN_METRICS` 可将指标 JSON 写入指定的隔离目录。未引入 CI 墙钟阈值断言；功能对照用固定发布基线哈希。

下一步：完整 API 合成规模与行为对照；随后附件 `file_key` 非唯一索引独立提交。附件索引、本批全量/原生 D1/浏览器最终回归尚未执行，不据此阶段申请直接发布。其它 P2 不混入本批。

## 阶段一补充：完整 API

阶段源码 `438c321` 已推送。`work-plan-api-performance.mjs` 使用真实办公室登录的 Worker API、隔离 SQLite 和合成订单/作业；旧版 Worker 从已发布工作区加载，新版使用本分支。相同数据分别串行运行，固定服务端日期，计时包含请求、授权、全量组装、筛选、分页及响应序列化，不含建库和合成数据准备。

| 组数/请求 | 原 API | 新 API | 减少 | 返回载荷 |
| --- | ---: | ---: | ---: | ---: |
| 100 首屏 | 49.267ms | 41.440ms | 15.89% | 102800 字节 |
| 1000 首屏 | 330.310ms | 181.507ms | 45.05% | 103874 字节 |
| 10000 首屏 | 31727.134ms | 4000.510ms | 87.39% | 106092 字节 |
| 10000 末尾条件筛选 | 47358.608ms | 3952.784ms | 91.65% | 1802 字节 |

四种请求的新旧完整响应哈希、返回数、总数、偏移及载荷字节逐项相同；每次读前后原业务表快照相同。万组 API 减少 ≥80% 的本地合成目标达成。没有提前分页或缩减字段。较大测试的合成数据准备较慢，不计入上述 API 耗时；不是现场网络或真实设备保证。

最初夹具误填了实际 schema 不存在的字段，准备阶段中止；已按实际表结构修正测试脚本，未更改业务表，失败日志保留。正式数值来自修正后完整成功的 v4 对照。

可通过 `CK_WORK_PLAN_API_MODULE=/绝对路径/已发布工作区/worker-v2/index.js` 选择旧版，`CK_WORK_PLAN_METRICS=/tmp/old.json` 保存结果；不设置模块参数则测当前版本。默认依次测试 100/1000/10000 组及末尾条件筛选。数据全部在 `:memory:`，仅访问 Worker 的本地 `fetch` 方法。

## 阶段二：附件键非唯一索引

阶段一完整 API 证据已提交 `12cbcf8`。本阶段仅新增 `idx_v2_attachments_file_key ON v2_attachments(file_key)`，在原基础迁移之后以幂等原子 DDL 初始化。无需提高基础 schema 版本或重放历史业务触发器；失败不缓存成功状态，下次可重试。没有修改下载授权 SQL、元数据选择 SQL、重复 key 规则、记录或 rowid，也没有引入唯一限制。

旧库 10000 条合成附件，在真实本地 workerd D1 上，同一元数据 SQL 从 **10000 行读取降至 1 行读取**，达到 ≤3 目标；查询计划由扫描改用新索引。创建前后所有附件（含 rowid）和旧 schema 元数据快照相同。

相关 Node **25/0/0**；专项原生 D1 **7/0/0**，覆盖：重复/空/NULL 键和重复插入、两个冷绑定同时初始化、运行时 CHECK 失败使索引及业务一起保持原状、同绑定重试、真实 Worker 升级失败/恢复、私有 GET 与原文件元数据、真实限定大货权限日当对重复 key 的允许/拒绝行为。

权限补测初轮为 Node 24/1、D1 6/1：夹具使用默认具备全部现场部门权限的 EMP，却错误期待其访问直发资料被拒。已改为通过实际签到与当日授权生成仅 `bulk` 权限的日当，先断言真实 scope/权限，再保留原 401 拒绝及原文件 200 断言；业务权限代码未改。原失败日志与后续通过日志分别保留。

```sh
node --test tests/attachment-key-index.test.mjs tests/work-plan-indexed.test.mjs tests/work-plan-list.test.mjs tests/issue-read.test.mjs
CK_PERF_NATIVE_D1=true CODEX_PRIMARY_RUNTIME_NODE_MODULES=/已安装依赖/node_modules node --test --test-concurrency=1 tests/attachment-key-index.test.mjs
```

正在固定源码上执行全量、包含前批竞态的原生 D1 合集，以及作业计划/原业务浏览器回归。此阶段未发布、未合测试，不能将阶段验证当作独立 QA 放行；其它 P2 保持后续批次。

## 最终开发验证

固定业务源码与测试：`160412121a1180f069e6ac3b48422d5fc0ea8a09`；其后仅补本文记录。

- 构建成功；全量 **1630 通过、0 失败、0 跳过、0 取消**。
- 真实本地 workerd D1 合集 **97/0/0/0**：附件索引 7 项，加前批耗材幂等/共享权限/完成事务/退出竞态、部分到仓与开关/旧库升级 90 项。没有使用线上数据库。
- 八组真实 Chromium 脚本串行全部 exit 0：作业计划（1440/390/320 业务流程及九组宽度/缩放视觉）、部分到仓、打印 CSV、自动派审、产出草稿、返岗、照片压缩、反馈创建。原断言、按钮定位和等待时长均未放宽。
- 加入索引后的最终源码另做 100/1000 组真实办公室登录 API 验证，完整响应、总数与字节再次匹配发布基线。万组性能数字来自前述隔离分阶段实测；最终的 `work-plan-list.js` 与测速阶段 `438c321` 字节一致，没有再次引入重复关联扫描。
- 本批业务源码只有三个文件：作业关联模块、新索引初始化模块、Worker 两行引入/调用。UI、业务完成/工时/出入库、权限实现、基础迁移版本、部署配置和发布标识未改。没有提前分页、唯一键、历史数据重写或真实数据压测。
- 分阶段远端保存：`438c321` Map、`12cbcf8` 完整 API 证据、`1604121` 附件索引。工作区干净；测试基线仍 `f640c4f`、main 仍 `4671f49`。未合测试或部署，交固定独立 QA；其他 P2 不属于本候选。

最终合集命令（先执行构建）：

```sh
node worker-v2/build-sop-staging.mjs
CK_JSDOM_MODULE=/已安装依赖/node_modules/jsdom/lib/api.js node --test --test-concurrency=2 tests/*.test.mjs
CK_PERF_NATIVE_D1=true CK_P1_NATIVE_D1=true CK_PARTIAL_NATIVE_D1=true CODEX_PRIMARY_RUNTIME_NODE_MODULES=/已安装依赖/node_modules node --test --test-concurrency=1 tests/attachment-key-index.test.mjs tests/material-txn-atomic.test.mjs tests/completion-owner-races.test.mjs tests/partial-arrival-api.test.mjs tests/partial-arrival-rollout.test.mjs tests/unplanned-completion-atomic.test.mjs
```

浏览器沿用仓库脚本：`work-plan-list-browser.mjs`、`partial-arrival-browser.mjs`、`partial-arrival-documents-browser.mjs`、`dispatch-time-browser.mjs`、`bulk-output-draft-browser.mjs`、`attendance-return-browser.mjs`、`photo-compression-browser.mjs`、`feedback-create-browser.mjs`。启用 `CK_ATTENDANCE_RETURNS=true CK_AUTO_DISPATCH_TIME=true`，使用隔离合成数据。上述不代表国内网络或实体手机验收，也不表示已修完全部历史 BUG。
