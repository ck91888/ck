# 自动派审工时：开发阶段记录

基线：远端测试分支 `upgrade/sop-safe-rollout-20260917` 的 `7957cf19cba3db69f24889e0f23a79aa60029632`。开发分支：`feature/dispatch-time-auto-20261102`。未修改 main、正式 Worker、既有业务数据或真实历史工时。仓库及父目录未发现 AGENTS.md 或 .agents/skills 指令。

## 当前实现

- 首次成功派出实际大货单自动开始个人派审工时，多单只累计个人连续时段。末单完成、取消或转出后进入待派单；正式转入从交接成功时刻计，不接收旧人工时。
- 本人实际操作与派审互斥。操作、装卸借调、休息及签退通过同事务数据库事件暂停或关闭；退出或归还后重新判断有效签到、授权、未完成职责、休息和实际操作，不直接恢复旧状态。
- 用户已确认：共享关系本身不启动；首次成功实际处理共享单，从动作时刻纳入当天职责。退出共享立即停止按该共享单计时，本人仍有有效单则继续。原负责人的有效个人时间独立计算，不按单叠加或制造部门业务量。
- 用户已确认：跨日签到、浏览、列表、刷新、失败操作均不激活旧单；当天成功的实际续作动作从现在起计，未实际处理的旧挂单不维持全天派审。不回填签到到动作间隔。
- 不使用浏览器心跳、锁屏、关页或短暂断网判定劳动。异常未签退区间在 KST 日期边界或既有04:00切日边界封顶并标待核实，待核实与有效劳动分列，不无限增长。
- EMP 通过原职员与现场权限规则；DAF 通过原日当授权；当天临时工牌必须明确关联稳定档案。账本同时绑定稳定人员 ID 与实际签到 ID，不用姓名推断。
- 人力概览、职员列表、详情和 CSV 单列派审工时；状态包含派审中、实际操作、休息、待派单、待核实。派审不增加操作产量、业务派单数量或实际参与任务数。
- 签退补休息只使用剩余未归属时间，已记派审劳动不能被转换为补休息。更正误签退不会自动重开，需要新的真实业务动作。已有派审劳动的签到不能作废，数据库也有事务内保护。

## 事务与数据

实际运行模块 `worker-v2/dispatch-time-runtime.js` 在真实业务审计、作业状态、人员参与、签到休息、授权和共享关系的同一事务中更新规范化账本：`ck_dispatch_time_days`、`links`、`spans`、`actions`。同一稳定人员最多一个开放时段。业务写入失败整体回滚，请求重试沿用唯一业务事件，不创建虚拟作业、操作人员段、成果或额外业务派单事件。真实处理身份取服务端 sop 事件 actor_id 或原生完成记录 dispatch_actor_id。

`dispatch-time.js` 为先行参考状态模型和内部事务原语；其基础 JSON 账本不是实际接口的数据源。`attendanceReport` 可选传入派审段；未启用时旧调用与返回兼容。有效劳动只占用原未归属时间；已有实际操作、休息优先，异常重叠标记而不重复计算。待核实时段不作为有效劳动或用于补休息。

测试站配置与构建已准备启用 `SOP_AUTO_DISPATCH_TIME_ENABLED`；仅测试环境可生效，当前尚未合测试分支或部署。不增加开始派审按钮。照片压缩和产出草稿沿用现有功能。

## 开发自测结果

业务源码固定于 `3db3991da3c244b8880f248620b626806814008f`。最终验证记录提交只更新本报告和既有浏览器合成夹具的可选功能开关，不修改业务源码。

| 验证 | 结果 | 本地证据 |
| --- | --- | --- |
| 固定构建后全量 `tests/*.test.mjs`，含 JSDOM | 1444通过、0失败、0跳过 | `/tmp/ck-dispatch-time-full.log` |
| 真实登录 Worker API | 26通过、0失败、0跳过（已包含在全量） | `/tmp/ck-dispatch-time-runtime.log` |
| 本地 workerd D1 | 真实派单、休息、恢复、审核结束与职员归集通过 | `/tmp/ck-dispatch-time-d1.log` |
| 新功能浏览器 | 1440/390/320职员与关联日当看板、状态筛选、独立时段、CSV、签到保护、本人下场/退出、休息和末单待派单通过；页面/弹窗无溢出 | `/tmp/ck-dispatch-time-browser.log`、`/tmp/ck-dispatch-time-browser/` |
| 开启自动计时后的产出草稿浏览器回归 | 普通、外部、分组、一人多单；预填、重开、双击幂等、跨设备冲突、响应丢失重试和最终修正全部通过 | `/tmp/ck-dispatch-time-output-regression.log` |
| 开启自动计时后的照片压缩浏览器回归 | PC/手机/缩放矩阵、EXIF、透明图、故障恢复、原API上传与条码/二维码解码通过；合成图3462248→403880字节，2048×1536 | `/tmp/ck-dispatch-time-photo-regression.log` |
| Worker包构建与deploy dry-run | 通过；只打包验证，没有部署 | `/tmp/ck-dispatch-time-build.log`、`/tmp/ck-dispatch-time-dry-run.log` |

API覆盖：四类大货入口、末单完成/取消、失败回滚、本人下场与退出、日当长期/当天临时牌关联、休息签退、资格撤销、共享首次处理/退出、正式转单、多设备重试/版本竞争、不同单并发首次派工、跨日续单、异常封顶、误签退更正、真实装卸借调、职员工时/工作量归集与未派单预填不激活。全部为合成身份和隔离数据库，不补真实历史数据。

已通过本地D1发现并修复触发器UPDATE别名兼容问题；已合成复现并修复共享仍有效时正式转单误将旧负责人重新认作协作者的问题。新回归确保旧人停止、接收人仅从交接时刻开始。当前无已知未修复失败。

复现命令（依赖位于本执行环境临时目录，未提交依赖或凭据）：

```sh
node worker-v2/build-sop-staging.mjs
CK_JSDOM_MODULE=/tmp/ck-output-deps/node_modules/jsdom/lib/api.js node --test tests/*.test.mjs
CODEX_PRIMARY_RUNTIME_NODE_MODULES=/tmp/ck-dispatch-d1-deps/node_modules node tests/dispatch-time-d1.mjs
node tests/dispatch-time-browser.mjs
CK_AUTO_DISPATCH_TIME=true node tests/bulk-output-draft-browser.mjs
CK_AUTO_DISPATCH_TIME=true node tests/photo-compression-browser.mjs
```

构建会清空测试资产目录，必须在读取这些资产的测试之前完成；浏览器串行执行。D1用Miniflare4和真实本地workerd，Node SQLite单测不能替代这一步。

开发自测完成，下一步交固定独立审核任务复核最终候选。尚未合测试分支或部署，测试分支仍为7957cf1，main仍为4671f49。测试站现场验收尚未执行。独立审核通过后，由总管通知用户再安排测试发布；绝不更新正式版。
