# 作业资料查看、下载与浏览器打印（测试环境）

在已发布分组打印 `0d7cea9a7762e45521371472fdc12f5984c0809e` 之上继续修复。该打印发布的 Cloudflare check 为 https://github.com/ck91888/ck/runs/119143518341 ，Worker版本为 `36742627-45ea-4aac-be26-147efd9afca8`。

分组编辑已有附件的文件名可点击查看，旁边提供明确的“打开/打印”和“下载”。新选择的组内文件等待主动保存再上传；新建页面草稿文件仍随页面提交保存。资料链接不提交表单，也不改变作业、出库或审核状态。

作业详情、分组详情/编辑、现场资料和出库资料使用同源链接。PDF由现有授权接口返回原始字节、`application/pdf`、`inline`，使用浏览器原生阅读器及其打印按钮；旧泛型MIME的PDF按附件元数据文件名识别。图片提供同源打印预览页。下载明确使用`attachment`及UTF-8文件名；Office/压缩包仅提供下载。未改变权限判定，不在URL中加入token，不公开资料，不调用转换服务，不改变浏览器安全设置。

## 验证

- `node tests/work-material-preview-browser.mjs`：真实Chromium + Worker + SQLite + 合成R2，覆盖作业资料、组内已保存附件、分组详情/现场渲染组件及完整构建的002出库详情页。6次打开两个中韩文件名PDF，检查原生阅读器及可见Print工具栏；逐字节比对原始PDF和下载文件、保留150×210mm原始页面（Chromium生成尺寸约426×595.92pt）；图片加载及打印按钮；多份中韩文件名下载；Office/ZIP无预览入口；未登录的打开/预览/下载均被拒绝。
- 查看/打印/下载前后，作业记录、出库记录、附件记录及审计记录完全相同。选择未保存组内文件，上传数为0；主动保存后上传1次并关联本组。
- `node tests/work-need-groups-print-browser.mjs` 再次通过。`CK_PLANS_SKIP_VISUAL=1 node tests/work-need-print-browser.mjs` 原有打印/重试/单据状态流程通过。
- 相关运行时49项通过。补齐测试所需的jsdom 30.1.3后，全量 `CK_JSDOM_MODULE=/tmp/ck-recovery-deps/node_modules/jsdom/lib/api.js node --test --test-concurrency=8 tests/*.test.mjs` 共1274项全部通过，失败/跳过均为0。staging资产构建、JS语法、`git diff --check`和指定 `wrangler.sop-staging.toml` 的`deploy --dry-run`通过。
- 本地证据：`/tmp/ck-recovery-materials`、`/tmp/ck-recovery-print`。所有数据均为隔离合成数据，没有写入测试站业务数据。

## 边界

测试站HTTPS此前被环境proxy以CONNECT403拒绝，未重试被拒路线或绕过。线上发布以本次精确commit的Cloudflare check及Worker版本为准。未执行真实物理打印、用户电脑/手机浏览器和真实业务单验收。既有两个浏览器附加脚本有与本次功能无关的旧布局预期：`work-need-print-browser.mjs`最后列表紧凑高度断言约159px超出140px；`cargo-groups-browser.mjs`未展开既有高级录入面板，等待隐藏的粘贴框。未为此扩展修改业务布局；本次新增真实浏览器验收覆盖实际可见入口。
