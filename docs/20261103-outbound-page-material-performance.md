# 出库列表当前页资料数量优化

独立分支：`feature/perf-outbound-page-materials-20261103`。
基线：已发布且已审 `3cb5b02c28e9b2242b95ae67893f4534b1c9ae43`。开发环境恢复后已执行命令并核远端测试 SHA，main 保持 `4671f49`。此前 `/tmp` 临时实验脚本未保留，`/workspace/scratch` 指标仍在；本批将可复现合成夹具与回归保存进源码。

## 阶段一与范围

仅改变作业链已启用、未指定 `has_material=0/1` 的出库列表。先按原 WHERE、日期基准、排序、LIMIT/OFFSET 物化当前页，再对页内行复用原 `workMaterialCountFor` 表达式。count 和 list 仍在同一个原 D1 batch，无新增请求、索引、表、迁移、缓存或写入。

“有/无资料”条件仍在全量筛选、计数和分页之前，保留其原 SQL 路径；旧开关关闭、装车 `load_trip` 提前入口不改。权限入口、资料去重/分组快照/移除规则、分页参数及元数据、排序键和最新变更摘要均不动，不新增同秒排序键。

直接以 WITH 开头的初稿被现有运行时归类为写入，导致配置同步而让只读事务断言失败：首轮相关 Node **10 通过、4 失败**。此问题已在提交前修正，使用外层 SELECT 包含物化页；不修改共享读写识别、安全守卫或放宽测试。初稿失败日志保留。

## 阶段验证

- 基线性能回归先复现 **0/1/0**：1000 单返回50条，旧查询实际执行1000次展示数量投影。
- 修正后相关 Node **14/0/0**，原生本地 workerd D1 专项 **6/0/0**。包含22种完整 API/分页筛选冻结哈希、旧入口/权限模式、同秒排序、原资料 SQL 逐行差分、真正只读 count/list 批次、共享资料恢复/新增后的新鲜计数、故障读后恢复和原记录不变。
- 冻结响应另用独立已发布 `3cb5b02` 工作区的真实 Worker 核验，共30份完整响应匹配（22场景＋8入口/权限模式），不是根据新实现重写预期。
- 新旧资料计数表达式保持原样，包括直连、需求、历史来源、入库共享、分组分配/预留快照、公共文件、ID 去重、同 key 不同 ID、移除/恢复及 `cargo_groups:null` 的既有规则。
- Node 探针确认展示投影从1000次变为50次；本地原生 D1 读取显著减少、读取语句 `rows_written=0`。接下来记录完整规模对照、全量和实际页面回归，阶段通过不代表独立 QA 放行。

本批未合测试、未部署，不查询或改写线上数据，只提交源码、合成测试和说明。其它 BUG、照片压缩、考勤、派审、打印历史及产出草稿不混改。

## 阶段二：真实登录完整 API 对照

首阶段 `9dc1372abfc6c2d7ed2c02f50193a289bf6543b6` 已推送。`outbound-page-api-performance.mjs` 从独立发布版工作区加载旧 Worker，使用相同合成夹具分别串行运行旧/新 API。每请求3次，取中位数；含请求授权、完整计数/组装/分页/序列化，不含建库和快照。

本地 Node SQLite 适配器：

| 出库单 / 无关历史需求 | 请求 | 发布版 API | 本批 API | 减少 |
| --- | --- | ---: | ---: | ---: |
| 100 / 100 | 首屏 | 33.451ms | 10.948ms | 67.27% |
| 1000 / 100 | 首屏 | 198.992ms | 17.105ms | 91.40% |
| 10000 / 100 | 首屏 | 1997.993ms | 33.816ms | 98.31% |
| 10000 / 100 | 末页 | 2051.498ms | 123.804ms | 93.97% |
| 100 / 1000 | 首屏 | 211.225ms | 78.997ms | 62.60% |
| 100 / 10000 | 首屏 | 2097.826ms | 726.561ms | 65.37% |
| 100 / 10000 | 末页 | 2141.521ms | 1823.523ms | 14.85% |

夹具另有5条资料语义需求、分组快照/共用/移除资料、出库单直连文件及历史变更记录，所以与前轮只读简化样本数据形状不同。两种历史规模及所有首/末页共12种请求，完整响应哈希、字节、排序、总数、offset、规范化 SQL/参数全部相同。资料过滤分支原 SQL 未改，采样有小幅波动，不承诺该分支加速。页内复杂资料关联仍有成本，本批只去掉页外数量投影。

原生本地 workerd D1 的实际 Worker API 对照也通过，四请求全部响应/查询相同，整个计时请求的 `rows_written=0`：

| 出库单 | 请求 | 发布版 API | 本批 API | 发布版读取 | 本批读取 |
| --- | --- | ---: | ---: | ---: | ---: |
| 1000 | 首屏 | 252.749ms | 39.847ms | 232381 | 14068 |
| 1000 | 末页 | 246.825ms | 58.335ms | 232381 | 31761 |
| 10000 | 首屏 | 2055.902ms | 63.610ms | 2223967 | 41068 |
| 10000 | 末页 | 2527.116ms | 183.658ms | 2223967 | 121775 |

这些是本地合成完整 API 结果，原生 D1 的耗时还包含本地代理通信，不是线上数据库、国内网络或实体手机测量。所有请求前后原需求、审计、资料、出库、工时与成果快照不变。新增开启返岗/自动派审后，列表仍只读、配置值不变的回归，Node 单独 **1/0/0**；最终原生专项及全量将包括该新增断言。

```sh
CK_OUTBOUND_PAGE_API_MODULE=/已发布3cb5b02工作区/worker-v2/index.js CK_OUTBOUND_PAGE_METRICS=/tmp/outbound-old.json node tests/outbound-page-api-performance.mjs
CK_OUTBOUND_PAGE_BASELINE_METRICS=/tmp/outbound-old.json CK_OUTBOUND_PAGE_METRICS=/tmp/outbound-new.json node tests/outbound-page-api-performance.mjs
```

`CK_OUTBOUND_PAGE_NATIVE_D1=true` 使用实际本地 workerd D1；`CK_OUTBOUND_PAGE_CONFIGS=1000:100,10000:100` 选择出库数:无关历史需求数。需要将 `CODEX_PRIMARY_RUNTIME_NODE_MODULES` 指向本地 Miniflare 依赖。测试不访问站点、不读线上数据。

## 最终开发验证

固定业务/测试源码：`30e760b711392733469a405494dd5101e55b840c`，业务 SQL 与首阶段 `9dc1372` 一致。之后只更新本说明。

- 构建成功、`git diff --check` 通过；全量 **1659 通过、0 失败、0 跳过、0 取消**，约353秒，包含既有返岗、自动派审、部分到仓、产出草稿、打印历史容错、事务权限与照片压缩单元回归。
- 最终原生本地 workerd D1 专项 **6/0/0**，包括新增开启返岗/自动派审后的只读分类、整个列表请求写入数0、配置内容不变。原始计数与 list 仍同一个 batch 的两条 SELECT，未以改动共享运行时掩盖初稿问题。
- 四份实际 Chromium 脚本串行均 exit 0：`outbound-list-browser.mjs`（1440/390 出库数量/资料筛选/反馈导航）、`work-print-null-browser.mjs`（PC/手机的空快照列表、详情、真实PDF与审计不变）、`photo-compression-browser.mjs`（条码/QR、取消/失败重试、格式/旋转及PC/手机矩阵）、`work-plan-list-browser.mjs`（2功能流程、9组宽度/缩放，含320/390）。旧脚本断言和等待没有修改。
- 照片回归保持合成图3462248字节压到403880字节、2048×1536，条码及QR可识别，上传/预览仍使用压缩后的文件。没有上传真实照片或提交图片/PDF。
- 完整 API 对照：Node 12种请求、本地原生D1 4种请求，全文/字节/顺序/分页/资料数量及规范化SQL/参数均与已发布版相同，原资料、审计、出库、工时/成果快照不变。耗时只是本地合成实测，不代表现场网络或实体手机验收。

本批相对 `3cb5b02` 仅1个业务文件的列表SQL、3个新合成测试/测速模块、冻结基线JSON及此说明。不修改资料表达式、权限、count/WHERE/排序参数、写入路径、schema/索引、部署配置或其它业务模块。不优化 `has_material=0/1` 的全量资料筛选成本，页内复杂关联仍可能较慢。

最终命令（先构建）：

```sh
node worker-v2/build-sop-staging.mjs
CK_JSDOM_MODULE=/已安装依赖/node_modules/jsdom/lib/api.js node --test --test-concurrency=2 tests/*.test.mjs
CK_OUTBOUND_PAGE_NATIVE_D1=true CODEX_PRIMARY_RUNTIME_NODE_MODULES=/已安装依赖/node_modules node --test --test-concurrency=1 tests/outbound-page-material.test.mjs
node tests/outbound-list-browser.mjs
node tests/work-print-null-browser.mjs
node tests/photo-compression-browser.mjs
node tests/work-plan-list-browser.mjs
```

原始失败/通过日志、Node与原生完整API指标、最终全量/原生/浏览器结果均在本环境 `/workspace/scratch/ck-p1-test-runtime/outbound-page-*`：`red.log` 为旧查询1000次投影复现失败，`stage-node.log` 为初稿只读分类失败，`stage-node-v2.log` 为修正后的14/0/0；最终为 `final-full.log`、`final-d1.log`、四份 `final-*-browser.log` 和 `final-browser-results.json`。原证据未删，不把失败改标通过。

分阶段源码及证据已推送独立分支，工作待固定独立QA，不合测试、不部署；测试基线仍 `3cb5b02`、main仍 `4671f49`。没有查询/压测线上数据库，没有写真实业务数据或上传凭据、图片、数据库与无关文件。
