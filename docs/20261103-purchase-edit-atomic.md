# 采购编辑并发保护

独立分支 `feature/fix-purchase-edit-atomic-20261103`，基线为已发布测试版 `3cb5b02c28e9b2242b95ae67893f4534b1c9ae43`。不包含待审核的出库列表性能分支，不合并或部署到测试/正式版，不访问线上业务数据库。

## 第一阶段

采购数量及关闭状态的检查原来只在事务前读取。另一请求发货、收货或关闭订单后，旧编辑仍能将数量改小，或者将已关闭订单重新写成 purchasing。

保留原校验、管理员权限及 API，在所有明细及订单头写入的同一 DB.batch 开头增加临时 CHECK 断言：订单仍开放、明细数量相同、每个原明细仍归属该单、每项采购量不低于实时非取消发货累计。断言失败整批回滚。仅识别三个具名 CHECK 冲突，其它数据库异常照常失败，不吞错。新增明细变更的中韩文刷新提示。

断言表按 DB 绑定幂等初始化，成功事务内删除临时行，失败事务回滚，不改写任何既有采购/发货/收货/库存数据。没有增加发货唯一约束，没有修改小数精度或超短收货规则，也没有替历史不一致记录自动补数据。

新增真实登录 Worker API 合成专项 `tests/purchase-edit-atomic.test.mjs`。修复前 Node 与原生本地 workerd D1 均为 **1 passed / 5 failed / 0 skipped**。失败分别为旧编辑遇发货、取消、完成、收货领用及末行冲突；反向竞争发货保护原来就通过。修复后同组在两引擎均为 **6 passed / 0 failed / 0 skipped**。覆盖整批业务表快照不变和库存流水 +8/-8 守恒，未削弱原断言。

本阶段尚待完整边界/故障/旧库初始化回归及全量测试，不能据此发布。修复前后日志仅本地保留于 `/workspace/scratch/ck-p1-test-runtime/purchase-edit-*`，不提交数据库、日志、凭据或任何真实个人/业务数据。

第一阶段业务与专项提交 `9fdbba12504212b1edd3e8201b6e88e2f061e838` 已推送并核验远端。既有采购流程/重试、耗材原子事务合集也为 **38/0/0**。

## 第二阶段：边界及页面

扩展 API 专项在 Node、原生本地 workerd D1 各 **21 passed / 0 failed / 0 skipped**。新增明细增删、移至另一订单、同数量换明细；两笔独立发货的实时累计；同一发货请求的并发重试；返岗及派审旗标开启；最后订单头写入故障回滚与再次保存；相等/更大数量及严格小数边界；取消发货排除；短收2/超收12仍按登记发货8为下限；现场及匿名权限拒绝；两个冷绑定同时初始化旧库。成功/失败后断言表均无遗留行，库存及审计事实快照不变。

首轮扩展为两引擎各 **18/3/0**：三个新合成夹具错误地给同订单插入重复物料，或向已含同物料的另一订单移行，被原有 `(order_id,material_id)` 唯一约束阻止，尚未进入目标竞争窗口。修正夹具使用独立合成物料后，保持原事务错误及全表快照断言，21项原样复测通过。首轮失败日志保留，不将其算作业务修复。

新增 `tests/purchase-edit-browser.mjs` 使用实际构建的 `/003`、登录 Worker 和 Chromium，通过 **1440、390** 两宽。每宽验证：在途编辑与发货/取消/明细删除竞争时拒绝，输入5留在表单、busy恢复、业务快照不变、中韩错误可读；重新打开已发货表单后最小值8，合法保存8成功。共8组页面结果、无浏览器/服务器异常。仅本地桌面 Chromium 手机视口，未声称实体设备或国内网络验收。

staging 静态构建成功，未部署。此阶段仅补充合成测试及说明，业务源码仍与第一阶段完全相同。全量回归运行中，最终以末节记录为准。

```sh
node --test --test-reporter=tap tests/purchase-edit-atomic.test.mjs
CK_PURCHASE_EDIT_NATIVE_D1=true CODEX_PRIMARY_RUNTIME_NODE_MODULES=/已安装依赖/node_modules node --test --test-reporter=tap tests/purchase-edit-atomic.test.mjs
node worker-v2/build-sop-staging.mjs
CODEX_PRIMARY_RUNTIME_NODE_MODULES=/已安装依赖/node_modules node tests/purchase-edit-browser.mjs
CK_JSDOM_MODULE=/已安装依赖/node_modules/jsdom/lib/api.js node --test --test-reporter=tap --test-concurrency=2 tests/*.test.mjs
```

## 最终开发自测候选

第二阶段 `c9eb30943479d6e188da85ac74e571da9b1c28a1` 已推送并核验远端。其后的业务/测试源码没有改动，仅追加本节验证记录。

- 全量 **1674 passed / 0 failed / 0 skipped / 0 cancelled**，约374秒。包含采购21项及原有产出草稿、照片、派审/返岗、部分到仓等回归；未调整既有断言或等待。
- 本批专项 Node **21/0/0**，原生本地 workerd D1 **21/0/0**；既有采购/耗材相关合集 **38/0/0**。
- 真实构建采购表单 Chromium 两宽共8组结果全部通过，构建成功；`git diff --check` 通过。浏览器只使用本地合成数据，不代表国内网络或真实手机验收。
- 相对测试基线3cb5b02共6文件：采购更新路由、独立事务断言模块、双语错误文字、API合成专项、采购表单脚本、本说明。没有修改部署配置、正式版或其它待审性能分支，没有自动处理真实历史数据。

修复前 Node/D1 的1通过5失败，以及首轮扩展夹具18通过3失败均完整保留于本地日志；修复/夹具校正后的最终通过结果分别保存在 `purchase-edit-final-node.log`、`purchase-edit-final-d1.log`、`purchase-edit-browser.log`、`purchase-edit-full.log`。没有提交运行日志、数据库或截图。

仅完成开发自测并提交独立分支，尚待固定独立审核，未合入测试分支或部署。
