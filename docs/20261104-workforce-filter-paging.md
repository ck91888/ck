# FE-07 人力看板筛选与渲染

基线为已推送源码 `6faee8bc11a0e9f95080ef2ccba81ccf2f9f1dba`，分支 `feature/perf-workforce-filter-paging-20261104`。本批只改 `CKLabor` 人力看板，不含受阻 FE-03/04 或重置补丁；不查询线上业务数据、不部署。不重试之前被拒的部署检查访问，FE-06 部署完成状态仍待核验。

## 定位与方案

原 `shared/attendance-ui.js` 在每次筛选时逐人读取多个 DOM 控件，再重建全部人员表格；现场模式同时生成全部卡片和表格。未展开的已删除记录也始终生成全部表格。

采用原生分页，每页50人，沿用 API 原顺序，完整筛选之后才切页；卡片与表格显示相同页。正常/已删除记录独立翻页，未展开已删除列表不生成行。翻页不增加接口读取，不修改记录、排序或权限。控件有双语文字、导航标签和 live 范围提示，末页按钮禁用后保留可用焦点目标。

筛选时一次读取控件值，保留原大小写不敏感、原始空白、公司/部门/当前状态、工牌/姓名/任务及历史段搜索语义。筛选/日期变更回第一页，刷新保留或收缩页码。已删除列表开关不改变正常列表页码。

人数、工时、公司汇总和所有 CSV 使用完整筛选结果，分页不改变统计集合。明细、休息、部门、核实补卡、删除恢复、工牌和派工权限操作沿用原处理及服务端验证。

## 第一阶段

- 原人力看板与自动刷新 DOM 回归52项通过；新增分页12项与原回归合计 **64通过、0失败、0跳过**。
- 新增覆盖顺序与去重、末页/空列表、完整聚合/CSV、组合筛选和特殊姓名、现场卡片一致、独立删除分页、原人员明细/取消和焦点、刷新失败/收缩、日期竞态、只读/派工权限边界。
- 构建及diff检查通过。真实浏览器100/1000/10000人均用内存合成响应，绝不调用Worker或远端业务。初测万人的桌面DOM从219795降为1221节点，现场从313437降为1675节点。初测与Node DOM测试并行，耗时只作初诊，待串行重新测量后报告最终对照。
- 性能脚本只关闭其隔离页面的15秒报表轮询，防止周期刷新干扰渲染测量，不改源码计时/刷新机制。测量包含实际共享UI、CSS与两次requestAnimationFrame，不是API或真实设备延迟。
- 首次性能夹具在about:blank缺少secure-context randomUUID，尚未调用业务UI即失败，改为本地拦截的HTTPS虚拟来源后通过。新增专项两次失败均为预期写错：只匹配`001`而非完整搜索词、忽略原拼接字符串中的连续空白；修正测试预期而非业务匹配规则，原日志保留。

该阶段只完成上述自测；后续验证结果见下文。固定候选须交独立QA，未发布。

## 串行性能与旧/新差分

没有同时运行其他测试，先基线后候选各完整一轮；相同Chromium、CSS、合成数据与四个筛选动作。数值为本地单轮观察，非现场承诺。耗时为调用开始至两次requestAnimationFrame，API载荷生成/传输不在测量内。

| 人数 / 视图 | 首屏毫秒：旧→新 | 清空搜索毫秒：旧→新 | DOM节点：旧→新 |
| --- | --- | --- | --- |
| 100 / 桌面 | 317.1 → 217.4 | 72.8 → 41.7 | 2325 → 1221 |
| 1000 / 桌面 | 617.7 → 278.8 | 592.4 → 41.8 | 22095 → 1221 |
| 10000 / 桌面 | 5664.9 → 219.2 | 6168.7 → 65.7 | 219795 → 1221 |
| 10000 / 390宽现场 | 18314.6 → 262.1 | 7753.2 → 32.5 | 313437 → 1675 |

万人的桌面/现场最大longtask分别由5426/16076毫秒降至91/99毫秒。表格始终最多50行；现场最多50张卡片及50行表格。两个UI读取仍是配置+汇总各一次；桌面CSV仍含10000人，现场无导出入口。

`workforce-paging-diff.mjs`直接执行单独6faee8b工作区的原源码与候选源码，4种角色/现场组合×11种条件共 **44组全文差分通过**。跨全部分页拼接后的人员行HTML/顺序/操作、已删除行、全量统计、公司汇总及CSV字节严格一致；包含特殊姓名、历史任务、员工号、连续空白及组合条件。没有从候选反录预期。

证据位于开发环境`/workspace/scratch/ck-p1-test-runtime/`：`fe07-serial-baseline/result.json`、`fe07-serial-candidate/result.json`、`fe07-diff.json`与同名前缀日志。初测及夹具失败日志保留，不上传运行截图/日志到公开仓库。

## 第二阶段：实际页面与本地业务

已推第一阶段`4d609a1`后，新增`workforce-paging-browser.mjs`使用实际构建的`/shuju/?tab=labor`与`/001/`、真实Worker/办公室与现场登录会话、独立内存SQLite。121名日当、另2名职员全部为脚本创建的合成身份。

1440/390两宽共4组页面流程通过：键盘Enter翻页，50/50/21完整顺序、跨页搜索、第二页明细与Esc焦点、第二页部门修改与休息开始/结束、删除/恢复同一签到原ID与工牌/原打卡时间、在第二页导出全部121人、现场123人卡片/表格同页与原权限边界。办公室有意执行的操作仅影响指定合成记录；现场翻页/明细读取前后业务表快照完全相同。截图已实际查看，390宽分页文字与两个按钮完整可见。

`fe07-browser-final/result.json`、同名日志、合成CSV及截图仅保存在开发环境。首轮实际浏览器也通过；补休息操作与分页可见截图后再次通过，没有修改业务断言或延长等待。

本阶段仅补测试与说明，业务源码保持第一阶段不变。之后的原返岗/实时概览浏览器及全量结果见最终验证。

## 最终验证与边界

| 检查 | 实际结果 |
| --- | --- |
| 完整Node回归，并发2 | **1897通过、0失败、0跳过**，退出0，498215毫秒 |
| 新分页12项与原人力/刷新52项 | 全量中全部通过；阶段合集64/0/0 |
| 旧新全文差分 | 44组通过，包含4种角色/现场组合、11种条件 |
| 规模对照 | 100/1000/10000桌面、10000现场，基线/候选各4组通过 |
| 实际构建页+真实本地Worker | 1440/390、办公室/现场共4组通过，含原操作及全量CSV |
| 原`attendance-return-browser.mjs` | 1440/390/320及丢签退响应重试通过 |
| 原`realtime-polling-browser.mjs` | 1440/390合计18组通过，三来源最大并发1、隐藏停刷及恢复保留 |
| 构建、语法、diff检查 | 通过 |

原源码仍完整读取汇总响应，筛选和统计扫描全量集合；本批减少DOM构建与重复控件查询。API载荷/后端查询、职员资料列表和长期工牌管理弹窗属于原路径，本批未做分页改动。性能为本地合成单轮，实际手机/网络、大型远端库不在验证范围。万条规模未逐页人工浏览，121条的全部分页HTML/顺序和万条CSV已分别核验。

FE-06原加载浏览器的既有资源数量断言失败（旧/新均预期2、实际3）和后续检查未执行边界继续保留，本轮未重跑或修改该脚本。先前被拒的GitHub部署检查与站点版本访问未重试或绕过，FE-06实际部署状态仍未核验；本批没有发布动作。

业务源码只有`shared/attendance-ui.js`中的`CKLabor`变化，Worker、数据库结构、其他应用及部署配置未变，不包含FE-03/04或重置补丁。第一阶段`4d609a1`、第二阶段`9afc7ee`均已推送独立功能分支；之后仅完善本文验证记录。

最终日志位于`/workspace/scratch/ck-p1-test-runtime/fe07-full.log`、`fe07-final-build.log`、`fe07-return-browser.log`、`fe07-realtime-browser.log`；性能/差分/新浏览器证据见前文。没有真实人员、业务单据、凭据或运行截图日志加入公开提交。

```sh
CK_JSDOM_MODULE=<jsdom/lib/api.js> node --test --test-reporter=tap tests/workforce-paging.test.mjs tests/workforce-dashboard-ui.test.mjs tests/workforce-refresh.test.mjs
CODEX_PRIMARY_RUNTIME_NODE_MODULES=<node_modules> node tests/workforce-paging-browser.mjs
CK_JSDOM_MODULE=<jsdom/lib/api.js> CK_FE07_BASELINE_ROOT=<固定6faee8b工作区> node tests/workforce-paging-diff.mjs
CODEX_PRIMARY_RUNTIME_NODE_MODULES=<node_modules> CK_FE07_SOURCE_ROOT=<固定6faee8b工作区> node tests/workforce-paging-performance.mjs
CODEX_PRIMARY_RUNTIME_NODE_MODULES=<node_modules> CK_FE07_EXPECT_PAGED=true node tests/workforce-paging-performance.mjs
CK_JSDOM_MODULE=<jsdom/lib/api.js> node --test --test-reporter=tap --test-concurrency=2 tests/*.test.mjs
```
