# FE-06 实时概览轮询生命周期

基线为已发布测试`3a8205909f63cdd3be0aef0fec9ef7679b9b8c79`，独立分支`feature/fix-realtime-overview-polling-20261104`。不混入受阻FE-03/04，不读写线上业务数据、不部署、不合测试/main。

## 原问题和复现

`showApp`每次新增30秒setInterval，隐藏文档仍按当前tab发请求；慢请求超过周期会重叠，迟到响应缺少页面/身份归属检查。测试站的`loadRealtime`还继续读取签到汇总，因此仅保护原概览API不够。

可控时钟与延迟读的有效基线 **2通过/10失败/0跳过**：初次两条读在90秒均未返回，原代码已启动8条；隐藏、重复showApp、页面缓存返回和身份变化也复现。普通tab切走及正常业务失败后恢复两个控制例通过。

## 最小修复

- 自动刷新仅在文档可见、实时页签有效且当前身份有原经理权限时执行；隐藏、其它原生/附加页签和pagehide停止定时，返回立即刷新。
- 概览、实时单据与测试站签到汇总整条链共用一轮调度。上一轮全部结束后再等待30秒，不累计错过的轮次；慢请求不叠加。
- 每个读取来源单独合并在途调用，手动刷新也不能与同来源并发。离开后返回时等待旧读结束，补一次新读；不取消已经发出的请求，不引入响应缓存。
- 页面代次、当前可见性、DOM及原身份快照共同校验迟到结果，旧成功/失败均不写当前页面；隐藏时旧概览返回不会继续新发签到汇总。
- 重复showApp不新增轮询或时钟实例。失败不会锁死轮询，下一轮可恢复。
- 保持原统计接口、人数/任务/单据和签到计数公式、标签、筛选及权限服务；不改业务写入路径。时钟仍为原每秒显示，轮询为完成后30秒。

## 阶段一

新增16项与原任务显示11项合计 **27/0/0**，构建和diff检查通过。包含初始隐藏、90秒慢读、隐藏/返回ABA、重复启动、失败/拒绝恢复、锁定/非经理/新经理、pagehide/pageshow、手动去重，以及实际sop-entry签到包装的延迟、隐藏后成功/失败、附加页签和原计数公式。

原任务显示夹具使用非可视JSDOM直接调用实时读取；按新可见性契约仅将shuju夹具设为可视，保留全部原断言，其他应用夹具未改。原失败日志保留，未放宽等待或跳过测试。

## 阶段二：真实构建浏览器

在本地实际构建页面、已认证Worker和隔离SQLite合成数据上，1440/390两宽合计18组检查通过。只控制30秒调度、文档可见性和响应延迟，统计仍来自真实API。

- 基线隐藏90秒新增概览3次、单据3次、签到汇总1次（原会话层已合并同时发出的相同签到读），慢概览最多4个在途。候选隐藏90秒三个来源均0新增，恢复各1次。
- 慢概览与隐藏返回、慢签到失败、当前失败恢复、附加职员页签、受控锁定/只读身份快照及pagehide/pageshow均通过。同来源最大并发均1；统计值和业务表快照保持预期，仅脚本主动新增的合成任务计入新统计。
- 原始首轮浏览器失败保留：把职员页自身的`scope:employee`签到查询误计为概览轮询。脚本按请求范围区分，保留原请求数量断言，未改业务源码。另一次基线等待失败系忽略原会话层相同读合并，改为等待实际概览调用链完成，结果仍为3/3/1。原日志均保留。
- 页面截图与`result.json`位于本地`fe06-browser/`；基线结果位于`fe06-browser-baseline/`。不作为真实设备或国内网络结论。

证据日志前缀`fe06-`，仅本地合成数据。候选须固定独立QA，未发布。

## 原页面回归与已保留问题

`filter-request-races-browser.mjs`两宽共82项通过，业务表快照不变，覆盖六列表、分页、导出和身份变化。

原`staging-load-browser.mjs`在候选与3a82059基线均未整组通过：重载资源数量断言为2，实际为3。原断言和失败日志保留，未修改该脚本或这些资源。本地诊断副本仅加失败证据输出，定位为既有未哈希动态模块`/shared/internal-operations.js`、`/shared/badge-identity.js`、`/shared/courier-rules.js`。该断言之前的真实办公室登录、概览首次只读一组、手动刷新概览与签到各一次及原签到标签断言均已通过；后续页面/缩放覆盖未执行，不称加载浏览器全绿。

未新增请求取消或超时。已经发出的读可以正常结束，离开后不更新页面；若底层网络读永久不结束，该来源会继续等待，保留现有传输边界。本地测试覆盖延迟后成功/失败，未覆盖实际设备、国内网络或服务器大库。

## 最终开发自测

| 验证 | 实际结果 |
| --- | --- |
| 新轮询专项16项 + 原任务显示11项 | 27通过、0失败、0跳过 |
| 全量 `tests/*.test.mjs`，并发2 | 1885通过、0失败、0跳过，367329毫秒，退出0 |
| 实际构建页面的FE-06两宽浏览器 | 18组通过，隔离Worker/SQLite，不接线上 |
| 原六列表筛选竞态两宽浏览器 | 82项通过，业务快照不变 |
| 原加载浏览器 | 候选与基线同一既有缓存数量断言失败，详见上文；未称整组通过 |
| staging构建、三文件语法检查、diff检查 | 通过 |

第一阶段`fa7224305849dae89b6c3f6d39bdfa130764e28c`包含全部业务修复；第二阶段`8790a0b187c1dc8161141bb85489eff73c9e3b76`补浏览器测试和证据。之后仅完善本文验证记录。Worker、数据库结构、其他业务应用源码无变化，不包含受阻FE-03/04；未操作线上业务数据、未发布或推测试分支。

日志保留于开发环境`/workspace/scratch/ck-p1-test-runtime/`：`fe06-full.log`、`fe06-stage1-node-final.log`、`fe06-final-build.log`、`fe06-browser-final.log`、`fe06-browser-baseline-confirmed.log`、`fe06-filter-browser.log`。加载原始失败分别为`fe06-load-browser.log`和`fe06-load-browser-baseline.log`，仅增加证据输出的本地副本为`fe06-load-diagnostic.mjs`及同名前缀目录；不提交这些运行日志或截图到公开仓库。

```sh
CK_JSDOM_MODULE=<jsdom/lib/api.js路径> node --test --test-reporter=tap tests/realtime-polling.test.mjs tests/job-display-apps.test.mjs
node worker-v2/build-sop-staging.mjs
CODEX_PRIMARY_RUNTIME_NODE_MODULES=<运行时node_modules> node tests/realtime-polling-browser.mjs
CODEX_PRIMARY_RUNTIME_NODE_MODULES=<运行时node_modules> node tests/filter-request-races-browser.mjs
CK_JSDOM_MODULE=<jsdom/lib/api.js路径> node --test --test-reporter=tap --test-concurrency=2 tests/*.test.mjs
```
