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
