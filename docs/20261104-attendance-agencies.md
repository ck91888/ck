# 日当人力公司选项与历史显示兼容

独立分支 `feature/attendance-agency-labels-20261104`，初始基线测试版 `9650cd18ff7e89199934b8e8cd5d6a829f0fb350`，后续已整合已审测试版812046a，见下文。不部署、不迁移或删除任何实际业务记录。初始本地 origin 跟踪引用滞后，空分支已安全快进至只读核验的远端精确9650cd1后才改源码。

新增 `리드인` 为有效日当人力公司。旧 `CKIK` 继续作为内部提交/存储/筛选值，所有可见公司名改为 `한국인력개발`；不把两个名称加入成两家公司，也不接受显示名作为新的存储类别。保留旧长期工牌默认、按公司补打、每日身份、权限和人数统计。

共享 `attendance-agencies.js` 仅生成纯文本显示名称，按组合名称中的完整公司项转换 `CKIK／其他` 等值，保留分隔符、空白及其他名称；不改原字符串/记录，不把CKIKX等未知名称误改。加入staging构建白名单并在日当及派工UI前加载；财务模块同样使用该显示函数。

覆盖位置：签到单选、固定工牌签到预选、当日公司更正、补打选择与工牌、看板/详情/公司统计/历史事件/长期工牌管理、派工权限弹窗、原始日汇总CSV、财务Excel的每日/分段/说明三表及筛选标签。出勤分段原始CSV没有公司字段，保持原列结构。API与数据库审计仍为原值，显示转换在渲染/导出单元格边界完成。

第一阶段：`node --test tests/attendance-agencies.test.mjs` 5通过、0失败、0跳过；staging构建通过。验证临时/长期新公司、旧CKIK精确查找/财务筛选/补打、默认修改不影响旧签到、版本冲突与无效公司拒绝、组合公司及Excel三表、不变原始数据。实际浏览器、原生D1及全量回归待后续；未测现场真实设备。

## 公司名称阶段验证（跨日阻断前）

此阶段业务源码固定 `967ff5d8101d21cf2f5c74b4252261843c58604f`，截至34973e6仅增加浏览器合成测试及记录；后续跨日阻断修复见下文。

- 全量：`CK_JSDOM_MODULE=<隔离jsdom>/lib/api.js node --test --test-concurrency=2 tests/*.test.mjs`，**1925通过、0失败、0跳过**，343秒。
- 专项：`node --test tests/attendance-agencies.test.mjs`：5/0/0；`CK_AGENCY_NATIVE_D1=true CODEX_PRIMARY_RUNTIME_NODE_MODULES=<隔离依赖> node --test --test-concurrency=1 tests/attendance-agencies.test.mjs`：5/0/0（其中3项真实Worker/D1业务、2项纯显示/工作簿函数）。
- `attendance-agencies-browser.mjs`：1440/390两宽完整签到点和办公室流程，6个唯一内部公司值、旧默认预选、真实新公司签到/更正/登记、按旧值补打、派工权限、历史事件、公司筛选、历史CSV及财务Excel三表通过。请求/数据库保留CKIK，旧历史行完整快照不变，手机签到点无横向溢出；打印窗口为合成stub，不验证真实出纸。
- 既有 `labor-export-browser.mjs` 两宽10组通过，含207个人日/208时段、完整分页、小时精度、取消/失败/身份锁定；`daily-dispatch-permissions-browser.mjs` 原2业务场景及8种PC/手机布局通过。构建包含新共享资源，未改权限或工时规则。

原始失败保留：`agency-labels-browser.log`（未执行独立签到点登录）、`agency-labels-browser-authenticated.log`（嵌套定位器写法）、`agency-labels-browser-selector-fixed.log`（夹具预带办公室Cookie，签到点登录依既有保护清除该办公室会话）。修正夹具的登录、相对定位与入口Cookie后，`agency-labels-browser-scope-fixed.log` 四阶段全部通过；未改等待时长、业务断言或访问控制源码。

日志均在开发环境 `/workspace/scratch/ck-p1-test-runtime/agency-labels-*.log`：另含 `targeted`、`d1`、`full`、`regression-finance`、`regression-dispatch`。只使用合成记录和本地服务，不提交实际人员数据、Cookie、真实导出或照片。

上述34973e6阶段基于9650cd1；后续跨日修复已整合812046a。本轮不部署、不合main、不操作线上业务数据；现场设备/打印机未测。

## 跨日财务公司归属阻断

QA发现的存量问题在34973e6同序复现：同一稳定人员前夜23:00–次日01:00的原签到公司CKIK，次日09:00–10:00原签到公司가온。返岗开启的两种补休模式均把两个时段归가온，CKIK筛选漏人；关闭返岗两组对照通过，原日志 `agency-ownership-original.log` 保留。问题源于返岗读模型用最新父签到作为整人日锚点，财务子段沿用该锚点公司，且导出提前按锚点公司筛选。

修复只读归属：每段携带其关联原签到的公司；财务合并人日保留每段公司，全部导出呈现CKIK／가온（显示为한국인력개발／가온），按公司筛选后仅汇总该公司时段及原精度工时。复用完整人日已计算的子段工时，不重新计算补休配额或工资；选定公司的源时段不受其他公司工时覆盖。其他公司的实际出勤不能变成虚构离岗，离岗提示是筛选范围内的真实空档，不按公司相加。旧响应可按originalRecords的父记录恢复来源；多父记录缺来源时明确拒绝，不猜最新公司。签到、当前公司字段和权限语义不变，无新增SQL查询或数据库迁移。

已安全整合当前测试分支812046a，构建白名单合并保留attendance-agencies与material-upload两份资源，上传业务源码保留。整合提交3ba518b首次推送被GitHub500明确拒绝；复查远端仍34973e6。随后7d44ded推送成功并核验远端，未覆盖并行改动。

首轮修复专项37/0/0，含返岗开关×补休开关四组真实Worker API对照、跨日及缺签退旧回归、公司小时守恒、来源不明拒绝、其他公司不变离岗及输入不变。原生D1、实际下载与完整整合回归待执行；所有数据均隔离合成。

## 跨日修复最终验证及环境恢复

在7d44ded上补充来源引用守卫：多父签到既无明确子段公司、又缺父记录引用时拒绝导出，而非回退最新父公司。新增测试覆盖缺父数据与缺引用两种情况，并保留旧版本API矩阵复现能力。最终只提交源码、合成测试和本记录，无实际人员数据、Cookie、凭据、真实资料或生成的工作簿。

- 最终完整回归日志 `agency-ownership-full-final.log`：**1971通过、0失败、0取消、0跳过**，409秒。原会话50849在环境断开后无法恢复，**进程退出码不可取**，不记为已取得exit 0。环境恢复后确认日志完整结束、未发现对应Node仍运行，三个未提交文件完整；核查后未改业务代码。
- 守卫专项 `agency-ownership-final-guard-targeted.log`：37/0/0；原生D1最终补跑命令 `CK_AGENCY_NATIVE_D1=true CODEX_PRIMARY_RUNTIME_NODE_MODULES=<隔离依赖> node --test --test-concurrency=1 tests/labor-export-agency-ownership.test.mjs`：**6/0/0、实际退出码0**，16.5秒，其中4项真实Worker/workerd D1矩阵、2项纯函数。日志 `agency-ownership-d1-final.log`。
- `labor-export-agency-ownership-browser.mjs`：返岗开关×补休开关四组合×1440/390两宽，8组合各下载全部、CKIK、가온三份工作簿，**共24份实际XLSX**；每日与分段公司、小时、离岗范围及历史原行快照均通过。日志 `agency-ownership-browser-arrays-fixed.log`。
- 既有公司界面两宽4阶段、财务两宽10组（207个人日/208时段、精度、取消/失败/身份锁定）、上传两宽6份PDF及截止时间10组均通过。分别为 `agency-ownership-agencies-browser.log`、`agency-ownership-finance-regression.log`、`agency-ownership-upload-regression.log`、`agency-ownership-upload-deadline-regression.log`。
- 首轮新增下载脚本仅因XLSX VM数组与Node数组原型不同而deepStrictEqual失败，值相同；使用Array.from转为宿主数组，保留原期望值及等待条件。原 `agency-ownership-browser.log` 与修复前真实归属失败日志均保留。

日志位于 `/workspace/scratch/ck-p1-test-runtime/`，只作隔离验证证据，不上传生成文件。末次构建资产包含守卫及两份新增共享资源，源码语法与git diff检查通过。

待固定候选独立QA复审。本轮仅推公司功能分支，不部署、不更改测试分支/main或线上业务数据；上传测试站部署状态仍待既有渠道确认，不重试此前Forbidden/代理403。真实设备与实际打印出纸未测。
