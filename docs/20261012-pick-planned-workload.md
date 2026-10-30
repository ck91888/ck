# 代发拣货开工计划工作量

状态：本地实现与隔离验收，待指定时间发布。用户要求不早于 **2026-10-12 22:00 KST / 13:00 UTC**，届时由父任务续接；本文不是部署成功证明。

范围：PR #7 的 `upgrade/sop-safe-rollout-20260917`，基于 `854b7cb0fd7d8196a5f93c69362c8468cac833af`；仅 `sop-test` / `ck-v2-api-sop-staging`。不合并 main，不触碰正式环境或真实数据，无迁移、回填、考勤更正或绩效规则调整。

## 行为与存储

`/001 → 代发拣货 → 分配人员并开始` 保留原工牌扫描、派审员、主操作员及多人流程，开工确认时填写：

| 字段 | 存储键 | 校验与含义 |
| --- | --- | --- |
| 产品总件数 | `product_quantity` | 正整数，全部产品件数合计 |
| SKU种类数 | `sku_count` | 正整数，不同 SKU 的种类数 |
| 涉及货位数 | `location_count` | 正整数，不同货位数，非库存数 |
| 是否叉车配货 | `forklift` | 明确选择是／否，布尔值，无默认否 |

服务端要求前三项为 JavaScript 安全整数且大于零，拒绝字符串、空值、布尔值、小数、负数、零及越界整数。叉车必须是真正布尔值。

一次拣货任务／趟次对应原 `v2_ops_jobs` 的 `pick_direct`，可包含多张单和多人。四项只在该任务开工时填写一次，保存在 `sop_records(kind='dispatch', id=job_id).state.planned_pick_workload`，同时写入现有开工审计。额外保存 `basis='planned_at_start'`、`scope='whole_task'`、服务端操作者／时间，以及开工时本任务全部 `pick_doc_nos`。不使用其他作业的结束实绩字段。

计划与任务、全部单号、全部人员时段、单号人员关联、审计及幂等结果由同一 D1 batch 提交。中途失败全部回滚；成功响应丢失后原请求可重试，改动计划不能复用同一请求号。另一个请求不能重复占用同一单。

预先登记但尚未开工的趟次仍须填写，沿用原任务 ID；即使只扫描其中一张，弹窗列出整个趟次的单号，服务端快照保存全部单号。之后加单不会自动改写开工计划。

## 历史兼容与展示

- 已存在作业时段的历史任务不要求追填；恢复、加人、续作和结束沿用原路径及权限／出勤／占用校验，缺失显示“未记录 / 미기록”。不写零。
- 原裸扫码接口允许已开工历史趟次续作；从未开工的趟次必须通过原子派工入口。通用新建接口不能绕过代发拣货入口。现有两个原生拣货开始 action 均校验四项。
- 原任务人员变更、个人休息、整任务暂停／恢复与结束保留计划。后续人员不再次填写。
- 作业详情和职员每日作业明细标注“任务计划工作量（非个人产量） / 작업 계획량 (개인 실적 아님)”。每名参与者明细引用同一整任务计划，不按人数分摊，也不汇总为个人产量；原实作工时、任务数和结束实绩独立保留。

## 验收

全部数据来自独立 SQLite `:memory:`、合成职员和真实 Worker handler；浏览器仅连接本地构建站点。

- 全量 1001 项通过，0 失败、0 跳过；其中新增工作量专项 12 项。
- 专项覆盖缺失与非法输入、明确叉车否／是、多人多单唯一快照、服务端来源、三处事务故障回滚、并发／响应丢失重试、同请求改量拒绝、重复派工、未签到、EMP 大小写精确匹配、DA 规范化、人员增减、休息／暂停／恢复、历史续作、原待开工趟次、职员明细与实绩分离。
- 实际 `/001` 浏览器 4 组业务流程通过，加 8 组尺寸／缩放实际开工与详情：1440/1280px × 100%/125%/150%，390/320px × 100%。原生 Chromium zoom，校验 CSS 视口与 devicePixelRatio，不使用 CSS 缩放替代。
- 职员明细 9 场景与 8 组原生缩放／手机矩阵通过；工牌身份、日当大小写、首页刷新／草稿保护、个人休息与任务暂停／恢复浏览器回归通过。
- 实际看图修正：旧 `dialog.ck-workflow label` 覆盖手机紧凑网格；长单号标签与任务标题溢出／裁切；320px 确认区取消按钮需继续滚动。最终使用专属样式、长号换行、完整任务单号展示、手机确认／取消分行。中韩文保持主次，手机数值输入与 48px 操作按钮可用。

可复现命令：

```sh
node worker-v2/build-sop-staging.mjs
CK_JSDOM_MODULE=/tmp/ck-attendance-test-deps/node_modules/jsdom/lib/api.js node --test --test-concurrency=2 tests/*.test.mjs
node tests/pick-workload-browser.mjs
CK_EMPLOYEE_VISUAL=1 node tests/employee-workday-browser.mjs
node tests/attendance-dispatch-identity-browser.mjs
node tests/daily-badge-browser.mjs
node tests/daily-assignment-browser.mjs
node tests/native-pause-browser.mjs
```

本次执行器证据：`/tmp/ck-pick-workload/`、`/tmp/ck-pick-employee-workday/` 及 `/tmp/ck-pick-*-browser.log`。交接清单另外保存每个发布文件的路径、模式、Git blob SHA、字节数，以及完整 tree、候选 commit、基线和发布时间限制；这些本机证据不是线上部署结果。

## 发布时间与接续

仅本地提交。**2026-10-12 13:00 UTC 前禁止更新远端发布分支或部署。** 父任务在指定时间续接，发布前重新核对远端 head；若已变化，先检查并集成，不能强推覆盖。测试通过与时间条件同时满足后才可更新指定测试分支，并核实该精确提交的 Cloudflare 成功检查、Worker version 与完成时间。禁止扩大到 main、生产或真实数据修复。
