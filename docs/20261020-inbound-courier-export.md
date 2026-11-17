# 协同中心入库计划：快递箱数导出

基线 `f38fde2213bb745309ce02358b3622fb24b8e847`；发布标识 `20261020-inbound-courier-export`。仅测试分支 `upgrade/sop-safe-rollout-20260917` 和现有测试 Worker `ck-v2-api-sop-staging`。本次无数据库迁移或业务数据改写。

## 原因与数量口径

`v2_inbound_plan_detail` 的货物明细直接读取 `v2_inbound_plan_lines`。导出已按 `plan_id, unit_type` 汇总这张表，但仅把 `carton` 写入箱数列，漏掉 `courier`。在未修改基线上，合成快递明细的计划8、实际8稳定导出为0、0。

按用户确认，快递对应的数量默认是箱数。导出现在使用：

- 计划箱数 = 原 `carton.planned_qty` 汇总 + `courier.planned_qty` 汇总。
- 实际箱数 = 原 `carton` 实际口径（非零上架汇总，否则卸货实际汇总）+ `courier.actual_qty` 汇总。
- 快递实际数量为0或NULL时计0，不用计划数或 `putaway_qty` 补齐。普通箱及托盘既有上架/卸货优先级保持原样。

当前快递创建/收货流程会维护明细数量；导出只读已保存明细，不重新数运单、扫描事件或收货记录，也不解析货物摘要，不采用关联出库数量或作业结果中的同一数量副本。快递行和独立普通箱行相加，各明细只汇总一次；历史无运单关联及多条快递明细同样适用。没有修改快递创建、扫描、上架、任务工时或执行候选接口。

## 验证

- 新增真实 Worker + SQLite/D1 接口回归12项，先在基线复现，再验证修复：8/8、混合快递/箱/托、多条历史明细、无运单历史记录、NULL实际、实际零、计划零、旧箱/托优先级、旧小数及其它单位、不重复累加作业/出库/收货表示。三运单/两收货/六事件与明细8/8刻意不同，导出仍为8/8。导出前后业务表内容相同。
- 定向测试65项通过：`inbound-export`、`inbound-arrival-filter`、`inbound-flow`、`courier`、`courier-arrival`。
- 完整回归：`CK_JSDOM_MODULE=/tmp/ck-inbound-export-deps/node_modules/jsdom/lib/api.js node --test --test-concurrency=2 tests/*.test.mjs`，**1208通过，0失败、0跳过**。
- `node tests/inbound-export-browser.mjs`：真实构建页面、已认证 Worker、隔离内存数据库；1440中文/390韩文各从第二页导出全部88条，再组合建单日期、预计到达日期、卸货日期、状态、业务、客户及记账筛选导出4条。校验CSV顺序、UTF-8 BOM、CRLF、逗号/引号/换行转义、所有箱/托列在SheetJS解析后为数值类型（含0及小数），并实际查看8/8详情截图。未运行桌面Microsoft Excel。
- 既有 `inbound-arrival-sort-browser`：1440/1280原生100%/125%/150%缩放和390/320手机，分页、CSV和预计到达日期降序通过；`inbound-arrival-filter-browser` 保留筛选、分页、CSV检查；`inbound-stages-browser` 四种尺寸的派工、理货、上架及共享导出通过。
- 构建、相关JS语法与 `git diff --check` 通过。CSV、截图和日志仅保留本地 `/tmp/ck-inbound-export-*`，依赖及生成资产不提交。

## 发布证据边界

测试分支推送后，以最终完整SHA的 `Workers Builds: ck-v2-api-sop-staging` 成功检查和Version ID核对部署，不能仅凭推送或发布标识声称部署成功。测试域名此前被执行环境代理以CONNECT 403拒绝，未绕过；云端页面和真实客户数据不用于本次验收。原客户截图未下载、未提交；本次复现依据用户明确数量口径及合成数据。main与正式环境保持原状。
