# 测试版管理员密码

适用 Worker：`ck-v2-api-sop-staging`。适用入口：`https://sop-test.ck91888.cn/` 和原测试域名。正式 Worker 不配置本项。

先确认测试站 `release.json` 的版本为 `20261005-test-admin-password-min5`。已存的5位 Secret 发布后直接生效，无需重新填写；尚未配置时，由负责人在 Cloudflare → Workers & Pages → `ck-v2-api-sop-staging` → Settings → Variables and Secrets 中新增：

- 类型：**Secret（加密机密）**。
- 名称：**`SOP_ADMIN_PASSWORD`**。
- 值：负责人自己设定的密码，5至128个字符，大小写敏感，前后不能有空格，不能包含换行等控制字符。填写密码本身，不填哈希或 JSON。不要把值发到聊天、截图或报告。

由负责人私下填写并自行提交/部署；助手不读取、生成、迁移或代填真实密码。提交后刷新测试站，用新密码登录办公室；签到电脑也需用新密码重新启用。旧管理员授权码、旧 JSON 管理员 key 和相关办公室/签到点会话不再生效。员工现场工牌仍按原签到及权限规则使用，人员和历史记录保留。

新 Secret 尚未配置时，发布不会立即关闭原有恢复入口。已有 `SOP_ADMIN_CODE_SHA256` 或有效 JSON 管理员 key 仍按原规则使用。JSON 内容无效时按空人员名单处理，不影响原哈希入口。

新 Secret 存在但为空、过短、过长或含不允许的空白/控制字符时拒绝所有管理员旧码和密码登录，不回退旧码。修正该 Secret 并部署可恢复新密码入口。若负责人自行删除该 Secret 并部署，会重新启用原有兼容入口，因此删除不是“停用所有管理员”的操作。

启用新密码后，管理员登录及会话校验不依赖 `SOP_USERS_JSON`，可在原 JSON 已丢失时恢复测试版办公室及签到点。原 JSON 的其他历史个人授权路径不会被重建；如仍需要那些路径，应由负责人找到备份或另行确认人员、角色、部门后恢复结构，不凭猜测覆盖人员数组。此改动不从误填的 JSON 值提取密码。

登录继续使用现有限制：按IP及入口在固定十分钟窗口内累计尝试，超过30次返回429；成功登录会清除当前窗口的计数。独立 HttpOnly 会话及权限检查保留。本次未新增其他防暴力措施。新会话使用带版本的 HMAC 校验材料，响应不含密码或校验材料。密码变更后旧新密码会话也失效。

新密码不写入 TOML 普通变量。Wrangler 常规部署保留已有加密 Secret，除非另外执行删除机密操作；本次不修改或上传测试、正式配置文件。后台普通变量仍受现有配置的部署规则约束，不能把密码改成普通变量。数据库、R2 文件桶及正式配置不变。本次不执行在线清空、恢复或真实业务数据删除。

Cloudflare 官方说明：[后台变量与 Wrangler 配置](https://developers.cloudflare.com/workers/wrangler/configuration/#source-of-truth)、[Secret 配置](https://developers.cloudflare.com/workers/configuration/secrets/)。
