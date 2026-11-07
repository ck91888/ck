# 04:00待核实工时的管理看板归集修复

基线：`e8492601fa010063d38b88c757345df7098b4c7b`。独立分支：`feature/fix-cutoff-dashboard-hours-20261103`。只用本地合成数据，没有读取线上业务、发布、合入main或执行重置。待发布的重置禁用补丁953f6f4保留在原独立分支，没有混入本候选。

## 复现与原因

在固定韩国时间2026-11-03 05:00，创建合成普通任务及原生派工作业，作业时段从11-02 09:00延续到次日04:00系统收尾，保留原始分钟、工时及截止审计。

| 选择韩国日期 | 原sop_dashboard实际人时 | 原工时报表有效分钟 | 修复后实际人时 | 修复后待核实人时 |
| --- | ---: | ---: | ---: | ---: |
| 11-02 | 15 | 0 | 0 | 15 |
| 11-03 | 4 | 0 | 0 | 4 |

修复前Node、原生workerd D1各 **0通过/2失败/0跳过**，两引擎均实际返回15和4，原日志保留。原因是`worker-v2/sop.js`的管理看板直接按起止时间累加，而工时报表已经排除`system_cutoff:*`待核实时段。个人出勤、实际作业报表及批次明细本已有各自的排除规则；本次修复的是派工管理看板的实际人时字段，不重新定义所有报表口径。

## 最小改动

- `worker-v2/dashboard-time.js`新增看板专用区间计算：按韩国所选日、一次读取截止时刻裁剪，普通进行中时段继续实时计时；沿用原`forcedClock`判定，将系统截止证据归入待核实，不计实际人时。异常/倒序时间不生成NaN或虚假劳动，未来结束时间仅计至本次读取。原报表的已结束段策略保持不变。
- `worker-v2/sop.js`保持原SQL、部门过滤、权限和作业范围；返回`pending_person_hours`、`pending_minutes`及`pending_segment_count`，待核实作业进入原数据质量区。同作业多名人员的待核实人时分别累加，提示按作业去重；与原已有提示合并。
- `shared/sop-native.js`在管理看板增加待核实人时和口径说明，保留原实际人时、当前人员、任务状态、成果、排名、刷新和待办CSV。

运行时代码仅这三文件。无schema、配置、迁移或写入逻辑改动，不重写原工时、分钟、成果、数量、签到或截止审计。通过既有主管核实入口确认真实结束后，原待核实段才按真实区间成为有效工时；本补丁不自动核实或补历史。

看板原有进行中计时和报表原有已闭合口径继续保留，因此当前开放半小时仍可在看板显示0.5，而已结束段报表为0。人员名单及进行中任务等字段沿用当前状态语义，本次没有重建其历史快照。成果分配与真实作业工时不是同一统计量，原产出规则未更改。

## 验证记录

- 阶段`5d05843`已推送：Node相关 **28/0/0**、原生D1原复现 **2/0/0**。
- 扩展Node相关 **79/0/0**，含SOP、截止、日期边界、身份与真实一人多单流程；一人多单系统截止后仍仅一条统一劳动时钟，批次明细0分钟与看板0实际/1待核实人时一致。
- 最终专项Node **9/0/0**、原生D1 **9/0/0**。补充了原管理汇总及两个实际作业口径报表的同区间对照，均排除未核实段。
- 验证普通任务/原生派工、跨午夜15+4小时、正常已结束与当前开放混合、多人分别计时/提示去重、主管真实结束核实及幂等、审计原值保留、关闭截止开关后的旧标记、生产入口可用性不变、认证角色及部门隔离、伪造部门/角色无效、无数据写回、无NaN、日期及当前读取截止裁剪。
- staging构建通过，diff检查通过。Chromium **2组通过**：1440/390宽度，使用现有构建工作流模块、最小会话适配器和真实本地Worker，覆盖所选日前后实际/待核实分列、韩国日期切换、刷新、原待办CSV导出、HTML标签安全和前后数据库快照一致；无页面错误，已查看截图。不是完整登录或实际手机/现场网络验收。
- 增强验证阶段`cfd8d43`已推送，业务源码与5d05843一致。完整Node全量 **1839通过/0失败/0跳过**，退出码0，耗时384908.5ms。候选交独立QA；开发自测不等于发布授权。

相关复现入口：

```sh
node --test --test-reporter=tap tests/cutoff-dashboard.test.mjs tests/sop.test.mjs tests/system-cutoff.test.mjs tests/dashboard-boundaries.test.mjs tests/attendance-dashboard-integrity.test.mjs tests/bulk-work-batch.test.mjs
CK_P1_NATIVE_D1=true CODEX_PRIMARY_RUNTIME_NODE_MODULES=<含miniflare的目录> node --test --test-reporter=tap tests/cutoff-dashboard.test.mjs
node worker-v2/build-sop-staging.mjs
CODEX_PRIMARY_RUNTIME_NODE_MODULES=<含playwright的目录> node tests/cutoff-dashboard-browser.mjs
CK_JSDOM_MODULE=<jsdom/lib/api.js路径> node --test --test-reporter=tap --test-concurrency=2 tests/*.test.mjs
```

## 边界

无线上数据库读取、数据更正、重置、清理、部署或正式版改动；未衡量远端大库、新旧浏览器混合加载或真实设备网络。重置发布仍等待单独的活动状态只读证据；旧版本在途操作与数据恢复不在本次修复范围。其它未关闭审计项仍单独处理。
