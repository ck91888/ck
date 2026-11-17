# 资料数量与筛选的 null 兼容

独立分支 `feature/fix-material-count-null-20261103`，基线为已审测试 `62bcf79f8e1be7417e7d6a9f9774779ecf6982c3`。不混入待审采购分支，不合测试、不部署，不访问线上业务数据库。

## 第一阶段

附件读取用 JavaScript 真值判断 cargo_groups；计数 SQL 原来只检查该字段是否存在，将 JSON null、false、0、空字符串误当作分组。用一个共享 SQL 谓词匹配 JS 真值，在原有计数表达式两处复用，保留 DISTINCT、冻结/预约快照、撤下状态、原联接条件、分页优化与所有权限。有效分组以及异常真值对象/数组/字符串继续走原分组边界，不将未选资料带出，不重写历史记录，也不改变异常结构的处理。

新增真实登录 API 专项 `material-count-null.test.mjs`。修复前 Node 与原生本地 workerd D1 均为 **2 passed / 4 failed / 0 skipped**；修复后同组均为 **6/0/0**。验证四种假值的列表/详情/附件一致及两类筛选、缺失字段兼容、七种异常真值结构不暴露资料，读前后原业务快照不变。既有作业链/撤回恢复与本批 Node 合集 **25/0/0**。

原性能基线包含旧 null 错误，不能整体再生其冻结哈希。新增 `material-count-published-diff.mjs` 从独立固定62bcf79工作区加载旧 Worker，先验证其30份完整响应仍匹配原冻结文件，再独立推导仅 SYN-JSON-NULL 的 material_count 由0改1、相应筛选及分页，逐字段与新 API 比较。30份对照通过，原基线 JSON 保持不变；新文件仅存9个受影响查询及2种权限模式的修正哈希，其余继续使用原值。

```sh
node --test --test-reporter=tap tests/material-count-null.test.mjs tests/work-chain.test.mjs tests/batch-material-state.test.mjs
CK_MATERIAL_NULL_NATIVE_D1=true CODEX_PRIMARY_RUNTIME_NODE_MODULES=/已安装依赖/node_modules node --test --test-reporter=tap tests/material-count-null.test.mjs
CK_MATERIAL_NULL_BASELINE_MODULE=/已发布62bcf79工作区/worker-v2/index.js node tests/material-count-published-diff.mjs
```

`CK_MATERIAL_NULL_WRITE_BASELINE=true` 仅在上述旧/新全文断言全部通过后生成受影响预期，不直接从新实现重录全部响应。生成过程没有修改原冻结文件。阶段尚待完整分组、共享、撤回恢复、权限分页、开关及性能/全量回归，不代表独立审核通过。

仅提交本任务源码、合成测试和说明；原始失败及通过日志在本地 `/workspace/scratch/ck-p1-test-runtime/material-null-*`，不提交数据库、凭据、真实人员资料、图片或运行日志。

第一阶段提交 `d5b898ffd33f69e9b1ad67278612cf03355f1560` 已推送并核远端。含原性能/筛选分页冻结用例的相关 Node 合集 **31/0/0**。

## 第二阶段扩展

API专项扩展为14项，新增真实 API 保存分组与预约，分配及公共文件冻结、后续资料不自动进入本单、禁止移除已引用文件；已审核分配快照及空分配优先；跨需求共享 ID 去重、同 key 不同 ID 分开计；普通撤回以及本批资料移除/恢复；两类资料筛选逐页全文和总数；部门/访客权限；返岗与派审开启时的零写入及旧开关路径。

首轮 Node及原生D1各 **11/3/0**，三项失败均是新增夹具：与原附件ID重复、漏填 sop_records.updated_at、误以为旧模式的有资料筛选会返回没有直连资料的订单。修正为独立ID、补齐必填时间、按原模式排除该订单后，Node **14/0/0**。原失败及其断言保留于日志，不作为业务源码修复；源码仍与第一阶段完全相同。接下来整合总管已发布的采购0fd9593基线并复测完整候选。

## 整合及最终验证

扩展测试提交6358349后，无冲突整合已审且已进入测试分支的采购候选 `0fd959318078ef4ad7ac4e94684d16476510f567`，合并提交 `2b51895f06c5ce7f2554a00a723fb7e15a225dde` 已推送并核远端。资料业务源码仍为第一阶段的两处分组谓词调整，采购更新路由、事务守卫及其测试均与0fd9593原样一致，没有重复发布采购版。

- 整合源码全量 **1694 passed / 0 failed / 0 skipped / 0 cancelled**，约332秒。没有改动旧断言或等待。
- 原生本地 workerd D1 合集 **40 passed / 1 failed / 0 skipped**。本批资料专项14项、原出库性能/分页6项全部通过；采购专项20项通过、1项同请求发货重试失败。该失败仍未修复，见下节，不将总合集描述为全绿。
- 整合后的30份旧/新完整 API全文对照再次通过，原冻结文件仍未修改。独立的 `material-count-api-performance.mjs` 另验证 Node **12种**、原生D1 **4种**完整 API：只准许目标null记录的0→1数量变化及其筛选分页后果；SQL只准许两处分组谓词变化，其他SQL文本/参数、返回字段/顺序和原业务快照全部严格一致。
- 千单D1首/末页读取14068/31761行，万单首/末页41068/121775行，与修改前完全相同，每个请求均为0行写入。count/list仍同原批次，展示计数只投影当前页；原生性能断言与Node的1000→50次投影断言均保留并通过。本地计时存在波动，不据此承诺现场延迟。
- 构建和 `git diff --check` 通过。六份 Chromium 脚本串行均首次 exit 0：新增资料null表单（1440/390，四类假值各验证标记/有无筛选/详情，共8组）、原出库列表、采购编辑、打印null真实PDF、照片压缩与条码/QR、作业计划（2功能流程及9视觉矩阵含320/390）。仅桌面Chromium视口，不代表实体手机或国内网络验收。

```sh
CK_MATERIAL_NULL_NATIVE_D1=true CK_OUTBOUND_PAGE_NATIVE_D1=true CK_PURCHASE_EDIT_NATIVE_D1=true CODEX_PRIMARY_RUNTIME_NODE_MODULES=/已安装依赖/node_modules node --test --test-concurrency=1 tests/material-count-null.test.mjs tests/outbound-page-material.test.mjs tests/purchase-edit-atomic.test.mjs
CK_MATERIAL_NULL_BASELINE_MODULE=/已发布62bcf79工作区/worker-v2/index.js CK_MATERIAL_NULL_METRICS=/tmp/material-count-node.json node tests/material-count-api-performance.mjs
CK_OUTBOUND_PAGE_NATIVE_D1=true CK_OUTBOUND_PAGE_CONFIGS=1000:100,10000:100 CK_MATERIAL_NULL_BASELINE_MODULE=/已发布62bcf79工作区/worker-v2/index.js CK_MATERIAL_NULL_METRICS=/tmp/material-count-d1.json CODEX_PRIMARY_RUNTIME_NODE_MODULES=/已安装依赖/node_modules node tests/material-count-api-performance.mjs
CK_JSDOM_MODULE=/已安装依赖/node_modules/jsdom/lib/api.js node --test --test-concurrency=2 tests/*.test.mjs
node worker-v2/build-sop-staging.mjs
node tests/material-count-null-browser.mjs
```

## 留存的采购基线问题（本批未修复）

D1合集的唯一失败为原 `purchase-edit-atomic.test.mjs` 的 `concurrent retries of one shipment reserve once before rejecting a stale edit`：同一发货请求，一份先保存成功，另一份在途请求返回 `shipment_qty_exceeds_ordered`。库中只有一条发货、数量8，没有双扣库存。

已在未包含资料修复的独立0fd9593原码上，对Node与原生D1分别确定性复现：采购10、同client_req_id及相同快递号请求发货8；在旧请求通过最初回执/快递号检查后、读取 `AS scheduled_qty ... WHERE l.order_id=?` 之前暂停；另一份相同请求提交成功；释放旧请求后，其事务前数量检查用已登记8+请求8比较采购10，误报超量，未在此返回点重新读取已提交回执。两引擎均得同样错误，发货行数仍1。不是资料计数源码引入的采购逻辑回归，也不将它归为环境抖动；本批没有修改采购源码或放宽原测试，交总管/独立QA另行确认修复范围，不能据本报告作整批发布放行。

确定性诊断及两引擎结果只保存在本环境 `/workspace/scratch/ck-p1-test-runtime/material-null-baseline-purchase-replay.{mjs,json,log}`，原D1失败完整保留于 `material-null-integrated-d1.log`。其余最终证据为 `material-null-integrated-full.log`、`material-null-integrated-published-diff.log`、`material-null-performance-{node,d1}.{json,log}`、`material-null-final-*-browser.log` 及 `material-null-final-browser-results.json`。没有提交运行日志或数据库。

最终候选仅追加测试/本说明，业务源码与2b51895一致。相对当前测试0fd9593仅本批1个业务文件及合成测试/预期/说明；不改schema、部署配置、有效分组冻结数据或历史记录，不合测试、不部署，不写线上数据。待固定独立审核，需明确保留上述基线失败。
