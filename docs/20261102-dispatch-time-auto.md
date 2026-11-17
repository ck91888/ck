# 自动派审工时：最终开发验证报告

开发分支：`feature/dispatch-time-auto-20261102`。基线为已审核测试分支的 `7957cf19cba3db69f24889e0f23a79aa60029632`。最终业务源码固定于 `01f5b6a7eebdd7ecb234d5dd45169c7ed4848ddc`；随后验证记录提交只更新本报告，不改变业务或测试源码。仓库及父目录未发现 AGENTS.md 或 .agents/skills 指令。

## 已实现行为

- 当天首次成功派出实际大货作业单，从服务器动作时刻开始个人派审计时；多个订单不叠加个人时间。末单结束、取消、转出后待派单，下次实际派单再开始。
- 本人实际操作、装卸借调、休息与派审互斥。退出或归还后按有效签到、当前授权、当天已实际接手的未完成职责、休息和其它操作重新判断。签退关闭；更正误签退不自动恢复派审，需要新的成功业务动作。
- 共享关系本身不启动。首次成功实际处理共享单才加入该人的职责，退出后停止按该共享单计时；仍有本人有效职责可继续。绑定具体共享关系，退出后重建关系不会恢复旧职责。正式转单结束旧人的该单职责，接收人从交接成功时刻起计，旧人不会被重新当作协作者。
- 跨日签到、浏览、刷新、失败操作和旧挂单均不启动。当天成功实际续作从现在计，不回填签到至动作间隔。锁屏、关页、断网和浏览器心跳均不是停计依据。
- 异常未签退时段在韩国日期边界或原04:00切日边界封顶并标待核实，不无限增长。待核实与有效劳动分列。
- EMP沿用职员身份和权限；DAF沿用日当部门授权；当天临时工牌须明确关联稳定档案。账本绑定稳定人员ID和实际签到ID，不按姓名归属。重复有效身份签到立即停止计时并提示待核实，不补冲突间隔。
- 日当人力、职员列表、个人明细和CSV单列派审时间。派审不增加实际操作产量、参与数或部门业务派单数。签退补休息只使用剩余未归属时间，不扣已有操作或派审劳动。

## 事务、配置与迁移

实际运行模块为 `worker-v2/dispatch-time-runtime.js`，使用 `ck_dispatch_time_days`、`links`、`spans`、`actions`。数据库触发器与真实业务审计、作业状态、人员参与、签到休息和授权变化同事务；单人仅一个开放时段，业务失败整体回滚，重试不重复计时。真实处理人来自原服务端审计身份，不创建虚拟作业、操作参与段或额外成果。

自动计时只在测试环境且签到、权限、升级和工作链均启用时运行。每个请求真实读取配置，每个写事务原子带入本请求开关，不缓存旗标为授权。只读查询直接执行，不续时、不改业务或账本；正常读取只增加一次固定旗标查询。DB包装仅共享原绑定已成功的DDL版本，不共享权限、业务结果或在途Promise。

v1升级增补 `requires_action`、`share_pair_id`，在同一事务替换旧触发器/视图并增加索引，保留旧劳动。两个冷启动同时升级时，只对指定重复列冲突重新读取结构并收敛；其它错误报出并整批回滚，不吞错。实际SDK测试沿用已有SOP迁移预先安装要求。

`dispatch-time.js` 是内部参考状态模型及事务原语；实际接口使用上述规范化账本。现有照片压缩和产出草稿业务源码保持原实现。

## 独立审核问题的修复证据

| 问题 | 当前实现及验证 |
| --- | --- |
| D1触发器UPDATE别名不兼容 | 已替换为无别名写法；真实D1新库、旧库和完整API通过 |
| 跨实例自动/共享开关失配 | 不缓存旗标决定；覆盖先关闭后其它绑定开启、以及业务批次前被重新开启的竞态 |
| 重复签到INSERT、身份字段变化漏重算 | 新签到/身份/工牌关联事件重核；合成第10分钟冲突时关闭，20分钟报告仍仅10分钟派审并待核实 |
| 有派审劳动的签到仍可作废 | 原API阻止，数据库INSERT/UPDATE守卫也阻止；无个人操作/休息的纯派审签到同样受保护 |
| 缺依赖导致无关请求迁移500 | 四类缺依赖的新库请求均安全跳过不可用账本 |
| v1缺列且旧trigger保留 | 增量迁移、替换视图/trigger，旧劳动保持；原部门修改及新派单可用 |
| 两冷启动ALTER竞态 | Node及真实D1通过；D1强制两方同时读旧结构，明确一次重复ALTER，回滚后重新读结构收敛 |
| 无关迁移错误 | 保留原错误，ALTER与旧trigger整体回滚，旧劳动不变；之后正常重试可升级 |
| 历史容量与反向别名扫描 | 按签到主键、人员及实际工牌索引查找；只扫描开放操作/待归还借调。NOCASE仅筛候选，原严格身份比较仍保留 |
| 共享中转单、退出后重建共享 | 旧人停止；重建关系本身不激活，新的成功处理才绑定新共享ID |

兼容回归保留原只读无写入、历史量增长不增加按单请求、取消拒绝和事务回滚断言。原竞态夹具按目标业务SQL识别注入，不再依赖第一条SQL或单条run；查询预算仅增加一次真实旗标读取。早期0b642f8全量的13项失败已修复，未当作通过记录。

## 最终开发自测

业务源码：`01f5b6a7eebdd7ecb234d5dd45169c7ed4848ddc`。全部为隔离合成数据，没有使用真实人员、照片或业务单。

| 验证 | 结果与本地证据 |
| --- | --- |
| 固定构建后的全量，含JSDOM | **1459通过、0失败、0跳过**；`/tmp/ck-dispatch-time-full.log` |
| 业务API及审核专项 | 26+15项通过，已含在全量；`/tmp/ck-dispatch-time-latest-focused.log` |
| 真实本地workerd D1新库/旧库 | 通过；`/tmp/ck-dispatch-time-d1-upgrade.log` |
| 两冷启动D1强制竞态 | 通过，旧劳动保留；`/tmp/ck-dispatch-time-d1-concurrent.log` |
| 完整API包在workerd原生D1 SDK中 | EMP、关联临时工牌DAF、嵌套守卫、派单/休息/恢复/审核通过；`/tmp/ck-dispatch-time-workerd.log` |
| 1万历史签到+1万关闭操作的实际登录派单 | 关闭计时40083行、开启40287行，**新增204行**；十次账本重算510行、服务端约5ms。基线自身约4万行扫描未冒充已消除；`/tmp/ck-dispatch-time-d1-history.log` |
| 新功能PC/手机浏览器 | 1440/390/320，看板/状态筛选/明细/CSV/签到保护/本人下场与退出/休息/末单待派单均通过；`/tmp/ck-dispatch-01f5b6a-browser.log` |
| 开启自动计时的产出草稿浏览器 | 四类入口，刷新恢复、重复保存、跨设备冲突、响应丢失重试、最终修正通过；`/tmp/ck-dispatch-01f5b6a-draft.log` |
| 开启自动计时的照片浏览器 | PC/手机/缩放、EXIF、透明图、失败恢复、上传及条码/二维码解码通过；合成图3462248→403880字节，2048×1536；`/tmp/ck-dispatch-01f5b6a-photo.log` |
| 构建和Worker dry-run | 通过，只打包未部署；`/tmp/ck-dispatch-time-build.log`、`/tmp/ck-dispatch-01f5b6a-dry-run.log` |

合成界面截图：`/tmp/ck-dispatch-time-browser/`。D1指标来自本地真实workerd，不是国内现场或线上生产规模验收。

复现主要命令（依赖在本执行环境临时目录，未提交依赖或凭据）：

```sh
node worker-v2/build-sop-staging.mjs
CK_JSDOM_MODULE=/tmp/ck-output-deps/node_modules/jsdom/lib/api.js node --test tests/*.test.mjs
CODEX_PRIMARY_RUNTIME_NODE_MODULES=/tmp/ck-dispatch-d1-deps/node_modules CK_DISPATCH_D1_UPGRADE=true CK_DISPATCH_D1_CONCURRENT=true node tests/dispatch-time-d1.mjs
CODEX_PRIMARY_RUNTIME_NODE_MODULES=/tmp/ck-dispatch-d1-deps/node_modules CK_DISPATCH_D1_HISTORY=true node tests/dispatch-time-d1.mjs
CODEX_PRIMARY_RUNTIME_NODE_MODULES=/tmp/ck-dispatch-d1-deps/node_modules CK_ESBUILD_NODE_MODULES=/tmp/ck-output-deps/node_modules node tests/dispatch-time-workerd.mjs
node tests/dispatch-time-browser.mjs
CK_AUTO_DISPATCH_TIME=true node tests/bulk-output-draft-browser.mjs
CK_AUTO_DISPATCH_TIME=true node tests/photo-compression-browser.mjs
```

构建会清空资产目录，须确认完成后再启动依赖资产的测试；浏览器串行执行。

## 当前交付范围

等待固定独立审核任务复审最终候选。尚未合测试分支或部署，没有线上业务写入，没有补真实历史工时；main及正式版未修改。测试站现场验收未执行。独立审核通过后由总管通知用户安排测试发布。

用户已确认的“误签退更正＋返岗多出勤段”排在后续 `feature/attendance-return-20261102`，基线需确认已审核。本分支未实施返岗，不放松每日唯一约束、不清空原签退、不补离岗劳动缺口。现账本按稳定人员和实际签到绑定，后续须显式对接出勤子段，不能把重新签到等同恢复旧操作或派审。
