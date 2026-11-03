# 测试重置最小安全限制

基线为已发布测试 `e8492601fa010063d38b88c757345df7098b4c7b`。独立分支 `feature/disable-unsafe-test-reset-20261103`，不包含未放行的生命周期清理或代次屏障分支。当前仅本地合成验证，未发布、未改线上数据。

## 方案和影响

初始限制修改 `worker-v2/test-data-reset.js` 和 `worker-v2/staging-entry.js`；维护认证复审后另修改 `worker-v2/sop-session.js` 和 `worker-v2/sop.js` 的内部认证选项：

- 移除服务端start/step的建运行、备份、删除、推进和标完成代码。已验证的负责人调用也一律503；开关、合法确认值、旧请求、已完成run和直接函数调用都不能重新开启执行。HTTP方法、来源、测试环境/域名和负责人权限限制保留，未经授权仍403，不合环境仍404。
- 状态接口只读，返回disabled和暂停原因；控制表不存在时不新建表或运行。已有进度、未完成标记、错误和备份保持原样。没有自动恢复、再次清空或手动数据修正逻辑。
- 业务安全检查独立于旧重置开关、升级开关和请求域名：在指定测试库标记及staging环境中，只读查询已有活动run。旧开关关闭也不能释放已有维护阻断；同库别名和新DB包装同样检查。不缓存“无活动”结果，不执行初始化或修补。
- 有活动run时拒绝办公室、现场、签到、对象及普通GET业务请求；后台scheduled不调用业务切日。维护状态读取失败也拒绝业务，后台抛错而不继续写入。办公室登录/身份/退出和原操作人上下文走其专用路径，不走业务初始化，不重建被清掉的出勤子段。
- 无活动run时继续原业务入口；正式环境和其它数据库标记不启用此维护检查。没有修改main、正式配置、业务字段、工时规则、照片或文件对象。

现有前端入口和按钮未修改，仍可能显示“清空/继续/重试”；点击由服务端拒绝并显示暂停错误，不会返回清空成功。这里只实施服务端安全限制，不能把它描述为前端按钮已禁用。

**对正在进行的重置：** 它将维持当前未完成位置，不能继续step，也不会被标完成或清掉活动标志。新业务暂停会持续到单独审计的恢复方案完成，关闭开关不能解除。已经删除的内容不会自动恢复，既有备份和R2对象不会删除。本补丁不能撤销旧版本已经运行的请求，不是混合版本在途隔离协议；也不授权任何线上清理或恢复。

## 复现和验证

修复前Node及原生workerd D1各 **0通过/3失败/0跳过**：负责人可直接start、旧run可通过step推进、旧开关关闭时新业务绕过维护检查。原日志保留。

阶段 `9296e71`：原三场景Node/D1各 **3/0/0**，已推送。阶段 `74661da`：相关Node **67/0/0**、原生D1组合 **12/0/0**，已推送。

组合验证包括：

- 两个批准域名及同Worker别名、重置开关true/false/缺失与升级开关false，直接POST start/step均不能推进；直接函数对任意参数拒绝且不提交SQL。
- 活动run阻断普通办公室、现场派单、签到点、文件上传/获取、普通GET和scheduled。检查前后逐表快照一致、进度与备份不变、一个合成R2对象保留。
- 同绑定先无活动再出现既有活动记录，以及另一DB包装读取，都不会用缓存绕过。
- 冷身份不重建已清空子段；维护状态未知时fail closed。
- 无活动时办公室创建、现场派单、签到点登记成功；后台正常委托到原scheduled。状态预览不创建控制schema，负责人/密码/来源/域名权限保留。

旧重置执行测试已按明确暂停策略改为“拒绝执行、保留原记录”的断言。原快递历史升级和保护、借调、反馈联动作业/成果/工时保护仍保留；没有保留测试后门来运行已禁用的清理算法。借调与反馈的两个真实业务测试也验证禁用请求不改变原记录。

扩展测试首轮有测试括号和请求helper遗漏，修正后上述67项通过，失败日志保留。全量首轮发现密码预览测试仍直接查询自动创建的控制表；改为更严格的“控制表本身没有创建”，密码权限断言不变，单独14项通过。首轮全量停止并保留日志；修正后全量另行执行。

## 初始限制回归记录

staging构建成功。修正后的完整全量 **1838通过/0失败/0跳过**，完整TAP结束，耗时617121.7ms。浏览器 **4组通过/0失败**：1440和390宽度，各含无活动run/已有活动run两种状态。开发自测完成，候选仍须独立QA，不得直接发布。

页面使用实际构建首页和本地Worker，验证打开/取消不改数据、start/step返回503、重试不推进、合成对象保留、关闭旧开关仍阻断活动run、无活动时正常业务可写。活动run下冷绑定后刷新仍可识别办公室身份，不重建已清空子段；旧开关关闭后状态接口按原规则404，页面确认按钮不可点击，业务仍503。四组均无页面异常，弹窗未超出视口；已实际查看桌面及390截图。截图和日志仅存隔离工作目录，不上传合成运行库、凭据或对象。

环境恢复后旧工具会话不再存在，但通过PID及日志确认原全量进程仍在运行，保留该进程而未重复启动。最终以完整TAP结尾和进程结束确认上述全量结果；没有把会话丢失或不完整日志当作通过。

复现入口：

```sh
node --test --test-reporter=tap tests/test-data-reset.test.mjs tests/test-reset-restriction.test.mjs tests/feedback-link.test.mjs tests/crew-borrow.test.mjs
CK_P1_NATIVE_D1=true CODEX_PRIMARY_RUNTIME_NODE_MODULES=<含miniflare的目录> node --test --test-reporter=tap tests/test-reset-restriction.test.mjs
node worker-v2/build-sop-staging.mjs
CK_JSDOM_MODULE=<jsdom/lib/api.js路径> node --test --test-reporter=tap --test-concurrency=2 tests/*.test.mjs
CODEX_PRIMARY_RUNTIME_NODE_MODULES=<含playwright的目录> node tests/test-reset-restriction-browser.mjs
```

全部数据、账号和对象为隔离合成夹具；数据库ID仅用于门禁匹配，实际绑定是内存SQLite或本地workerd D1。浏览器拦截并在本地履行所有请求，批准域名仅作为虚拟同源地址，不访问线上站点，不绕过代理403。

未覆盖远端容量/锁等待、实体设备、国内网络及旧版本已在途操作排空；没有在线重置、改活动标志或业务数据、发布、合测试/main。后续完整隔离与恢复协议仍属独立未关闭工作。

## 维护认证复审修复

独立QA拒绝595bb05后，本地Node/原生workerd D1均重现 **0通过/4失败/0跳过**：GET登录会签发会话，GET退出会删除会话，同站其它来源POST退出会删除管理员会话，关闭访问控制后的兼容登录会回填已归档业务单号。旧版在途操作未排空这一限制保持未关闭。

阶段 `61b2686` 仅修上述认证回归并推送：原四例在Node/D1均 **4/0/0**。

- 维护认证先检查POST、原有可选Origin同源规则、Sec-Fetch-Site跨站标记和JSON类型，再读取/修改会话及操作人上下文。GET登录、退出、身份及操作人确认均403；跨来源（包括同站其它子域）、null来源和跨站标记均拒绝。未提供Origin的原程序化POST语义保留，不能用跨站标记绕过。
- 访问控制模式仍沿用原office凭据、限流、会话范围、Strict/HttpOnly/Secure cookie、操作人确认和版本校验；现场/签到点不会获得新增维护登录、身份或确认权限。
- 兼容模式仍沿用同一授权配置校验、用户角色和签名cookie。内部 `authenticationOnly` 选项仅允许认证会话在校验后直接返回，跳过会恢复业务单号的通用业务初始化；请求JSON中的同名字段不能控制它。无活动run时的原初始化路径不变，没有改变普通业务行为。
- 错误凭据、不合法配置和伪造角色不签发新会话；viewer仍无维护状态权限。有效维护登录、冷身份读取和退出保留业务、归档编号、备份及已清空出勤子段，不重新初始化。

扩展Node相关 **76/0/0**，包含权限、密码、SOP及操作人原有验证；原生D1认证与原限制组合 **23/0/0**。staging构建通过。认证页面 **4组通过**（1440/390 × 访问控制/兼容），使用现有构建认证脚本和真实本地Worker：错误密码拒绝、有效登录、原操作人确认、带实际Strict受害cookie的另一子域GET退出被拒且会话不变、GET登录被拒、冷刷新不回填、正常退出均通过。页面均无异常；认证页是隔离夹具，不能声称实际兼容首页所有导航已覆盖。原真实构建重置页面另 **4组通过**。

认证页面首轮因本地路由履行未暴露自动Sec-Fetch-Site头而失败，原日志保留。最终脚本验证实际受害cookie到达Worker、403、会话集合不变和身份仍有效；未伪造浏览器头部。服务端针对Sec-Fetch-Site的拒绝仍由Node/D1原断言覆盖。未削弱任何原业务/权限断言。

增强验证阶段 `74c82fc` 已推送，业务源码与61b2686一致。本轮完整全量已结束，退出码0，**1849通过/0失败/0跳过**，耗时301066.1ms；未改断言或等待条件来通过全量。全部复现仅用隔离合成数据，没有在线重置、部署或更改真实记录。新增复现入口：

```sh
node --test --test-reporter=tap tests/test-reset-auth-restriction.test.mjs tests/test-reset-restriction.test.mjs tests/access-control.test.mjs tests/admin-password.test.mjs tests/sop.test.mjs tests/office-operator.test.mjs
CK_P1_NATIVE_D1=true CODEX_PRIMARY_RUNTIME_NODE_MODULES=<含miniflare的目录> node --test --test-reporter=tap tests/test-reset-auth-restriction.test.mjs tests/test-reset-restriction.test.mjs
CODEX_PRIMARY_RUNTIME_NODE_MODULES=<含playwright的目录> node tests/test-reset-auth-browser.mjs
```
