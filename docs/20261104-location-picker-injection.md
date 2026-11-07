# FE-01 位置编码选择器注入修复

基线：已发布测试 `4d738bfb573aa51779785054155788288295135c`。独立分支 `feature/fix-location-picker-injection-20261104`。本批只处理FE-01，不包含FE-03收货查询、FE-04流水导出、FE-05照片缓存或待发布的重置补丁。未读取线上业务数据，不发布、不合测试/main。

## 原问题与复现

`003/app.js` 的位置选择器将服务端保存的位置编码经`jsq`后拼入HTML的`onclick`。该函数仅处理JavaScript单引号和反斜杠，不能处理HTML属性边界；双引号编码既能破坏正常选择，也能生成额外事件属性。

通过真实本地Worker的办公室管理员会话保存合成编码，在实际构建003页面读取并打开位置选项，鼠标进入即执行无害标记：`marker=1`，注入的onpointerenter属性可见。该原浏览器失败、截图及标记JSON保留在隔离测试目录，不上传数据库或会话。

修复前DOM专项 **2通过/2失败/0跳过**：额外事件属性出现；双引号编码不能正确选中，选中值仍为前一个单引号编码。过滤、顺序和重复绘制控制例正常。权限/编码API基线Node及原生workerd D1各 **3/0/0**，证明无需为解决输出问题改写服务端编码或扩权。

## 最小修复

业务源码仅修改`003/app.js`中的`renderLocationPicker`：

- 位置编码与名称通过`textContent`写入，按钮通过`addEventListener`传递原始编码，不将编码拼入内联事件或动态属性。
- 保留原button类型、CSS类、选中状态、顺序、搜索、空列表、仅有效位置规则及原`selectLocation`校验。
- 保留耗材/物品位置控件、键盘操作、正常编辑及刷新。使用既有通用DOM接口，未引入新依赖或全局监听。
- 不改变编码格式、大小写、Unicode、实体文本或现有服务端trim/长度规则，不清洗历史数据；服务端、权限、数据库、构建配置和其它渲染器未改。其它原有内联事件未据此宣称全站已消除。

## 开发验证

业务源码及专项固定在阶段`9c7e4d256ffe699468110f0c77107aef06da9ef0`，已推送。

- 相关Node **35/0/0**：位置DOM与API、扫码启动/失败/手动回退、CSV/Excel原始编码读取、耗材和物品库存、既有访问控制。最终DOM四项均通过。
- 原生workerd D1 API **3/0/0**：保存/列表/编辑保留引号、反斜杠、HTML实体文本、特殊字符、韩文/中文/emoji和Unicode分隔符；办公室管理员可编辑，现场/签到点/匿名及伪造role不能编辑，旧现场读权限仍仅返回有效位置。后端文件字节未改。
- 实际构建003页面 **2组通过**（1440/390）：正常管理员登录会话、真实本地Worker、八种编码、无事件属性或执行标记、鼠标与Enter选择、选中状态、搜索空结果、停用位置隐藏、耗材/物品两个控件、正常编辑名称时编码不变、刷新再进入以及原编码仍保存在数据库；无页面异常。
- 原`purchase-shipment-replay-browser.mjs` **4组通过**：PC/390各含并发与丢响应重试，原发货回执、数量、库存与冲突语义未变。使用原脚本、原等待和断言。
- 全量Node **1846通过/0失败/0跳过**，退出码0，耗时268670毫秒；保留原测试断言与等待，未为取得通过放宽测试。
- 构建及diff检查通过。已实际查看1440及390宽截图，位置字段与表单布局正常。上述为开发侧自测，尚待固定独立QA审核。

复现入口（仅隔离数据）：

```sh
CK_JSDOM_MODULE=<jsdom/lib/api.js路径> node --test --test-reporter=tap tests/location-picker.test.mjs tests/location-picker-api.test.mjs tests/material-location-scanner.test.mjs tests/materials-audit.test.mjs tests/access-control.test.mjs
CK_P1_NATIVE_D1=true CODEX_PRIMARY_RUNTIME_NODE_MODULES=<含miniflare的目录> node --test --test-reporter=tap tests/location-picker-api.test.mjs
node worker-v2/build-sop-staging.mjs
CODEX_PRIMARY_RUNTIME_NODE_MODULES=<含playwright的目录> node tests/location-picker-browser.mjs
CODEX_PRIMARY_RUNTIME_NODE_MODULES=<含playwright的目录> node tests/purchase-shipment-replay-browser.mjs
CK_JSDOM_MODULE=<jsdom/lib/api.js路径> node --test --test-reporter=tap --test-concurrency=2 tests/*.test.mjs
```

浏览器的全部请求在本地履行，禁止外部联网；原始复现与修复后日志以`fe01-`为前缀留在隔离运行目录。没有上传凭据、真实个人信息、照片或业务数据。未测实体设备、Safari或国内网络，不据此宣称现场验收完成。

## 后续分批边界

FE-03仍是独立项：`openReceiptByCode`在查询完成后才关闭相机并重绘；原审计用同单号两个在途响应复现第二个响应清空已填数量。本批没有改动该函数或提交其修复。FE-04、FE-05亦未纳入。本候选须交固定独立QA后才考虑测试发布，重置补丁继续等待其独立活动状态证据。
