# 日当人力公司选项与历史显示兼容

独立分支 `feature/attendance-agency-labels-20261104`，初始基线测试版 `9650cd18ff7e89199934b8e8cd5d6a829f0fb350`，后续已整合已审测试版812046a，见下文。不部署、不迁移或删除任何实际业务记录。初始本地 origin 跟踪引用滞后，空分支已安全快进至只读核验的远端精确9650cd1后才改源码。

新增 `리드인` 为有效日当人力公司。旧 `CKIK` 继续作为内部提交/存储/筛选值，所有可见公司名改为 `한국인력개발`；不把两个名称加入成两家公司，也不接受显示名作为新的存储类别。保留旧长期工牌默认、按公司补打、每日身份、权限和人数统计。

共享 `attendance-agencies.js` 仅生成纯文本显示名称，按组合名称中的完整公司项转换 `CKIK／其他` 等值，保留分隔符、空白及其他名称；不改原字符串/记录，不把CKIKX等未知名称误改。加入staging构建白名单并在日当及派工UI前加载；财务模块同样使用该显示函数。

覆盖位置：签到单选、固定工牌签到预选、当日公司更正、补打选择与工牌、看板/详情/公司统计/历史事件/长期工牌管理、派工权限弹窗、原始日汇总CSV、财务Excel的每日/分段/说明三表及筛选标签。出勤分段原始CSV没有公司字段，保持原列结构。API与数据库审计仍为原值，显示转换在渲染/导出单元格边界完成。

第一阶段：`node --test tests/attendance-agencies.test.mjs` 5通过、0失败、0跳过；staging构建通过。验证临时/长期新公司、旧CKIK精确查找/财务筛选/补打、默认修改不影响旧签到、版本冲突与无效公司拒绝、组合公司及Excel三表、不变原始数据。实际浏览器、原生D1及全量回归待后续；未测现场真实设备。

## 最终开发验证

业务源码固定 `967ff5d8101d21cf2f5c74b4252261843c58604f`，后续仅增加浏览器合成测试及本记录。

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

已安全整合当前测试分支812046a，构建白名单合并保留attendance-agencies与material-upload两份资源，上传业务源码保留。整合提交3ba518b首次推送被GitHub500明确拒绝；复查远端仍34973e6，本地提交完整，待本阶段再次推送。

首轮修复专项37/0/0，含返岗开关×补休开关四组真实Worker API对照、跨日及缺签退旧回归、公司小时守恒、来源不明拒绝、其他公司不变离岗及输入不变。原生D1、实际下载与完整整合回归待执行；所有数据均隔离合成。
