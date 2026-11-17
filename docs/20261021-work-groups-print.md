# 作业单打印分组修复（测试环境）

基线 `154c6c1049c3ce80b009d75263a321d25eae0e5f`。只发布 `upgrade/sop-safe-rollout-20260917` / `ck-v2-api-sop-staging`。

作业单在原总备注后按顺序输出组号、组名、完整箱唛范围、预计箱数、完整多行备注及附件文件名。保留原二维码、交接说明和签字栏；旧无分组单保持原内容。长备注允许自然跨页。打印指纹包含上述分组内容，执行或审核状态不影响打印版本；旧无分组单的指纹算法保持兼容。

验证：`node --test tests/work-need-print.test.mjs tests/cargo-groups.test.mjs` 20项通过。`node tests/work-need-groups-print-browser.mjs` 使用真实 Chromium、Worker、SQLite、纯合成数据，验证62箱两组（55/7）、100行中韩备注跨页生成PDF并逐行提取核对、3个中韩PDF名、旧单、二维码、分组修改需重印及打印不改业务行。证据输出 `/tmp/ck-recovery-print`。

现有 `work-need-print-browser.mjs` 的打印与重试场景完成，最终非打印列表紧凑高度断言失败（实测约159px；与本次模板/指纹无关），不调整列表布局。测试站已知 proxy CONNECT403，未绕过；发布需核对本提交的 Cloudflare check。
