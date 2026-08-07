# v5.0 变更日志

## 新增功能

- 插件生态：PluginManager/PluginContext，`/api/plugins` 系列接口，`plugins/` 目录，支持 Git 仓库安装 / 启停 / 重载
- 撤回消息监听：`recall_monitor_enabled`，自动重发被撤回的图片/文件/贴纸并发送撤回通知
- AI 图片生成：`/api/send/ai-image`，收到"请生成一张图片..."提示词自动作图并发送到群聊
- @全体成员协议：`AT_ALL_SPECIAL_ID`，`/api/send/at-all` 增强错误码，`/api/diag/at-all` 诊断接口
- 艾特昵称反查：`api_send` 中 at/atspam 模式按昵称查 `user_db`
- 引用消息解析：`cloud_custom_data` → quote，图片直链解析后入缓存
- 自动回复规则管理：支持优先级 / 启停 / 增删改（`/api/auto-reply/rules` 系列）
- 群成员铭牌系统：自定义铭牌 / 认证蓝标 / 自定义头像，支持导出导入
- `config.json` 支持 `PORT` / `RECALL_MONITOR_ENABLED` 配置项
- 前端新增：插件页、规则编辑器、撤回监听开关、AI 图片面板、铭牌管理（保留干净 Apple 界面）

## 兼容性

- 配置格式与 v5.0 完全兼容，直接复制 config.json 即可

---

# v4.0 变更日志

## 新增功能

- 贴纸页双击贴纸直接发送（使用当前刷屏次数与附加文本/@ 设置）
- 贴纸显示改用 QQ 官方表情库（koishijs/QFace）真实图片，无对应资源的贴纸降级为名称占位块
- 贴纸搜索框下方增加操作提示文案

## 连接稳定性修复 (核心)

### Bug 1: WebSocket 静默断连
- **原因**: `websockets.connect()` 未设置 ping_interval，长时间无消息时服务器可能单方面断开
- **修复**: 增加 `ping_interval=20, ping_timeout=10, max_size=4MB`
- **影响**: 连接不再无故断开

### Bug 2: 连接无超时保护
- **原因**: `api_connect` 中 `async_call(sender.connect())` 无超时，后端挂起时前端永久转圈
- **修复**: 后端 `asyncio.wait_for(connect, timeout=15)`，前端 `Promise.race(timeout=25s)`
- **影响**: 前端不再卡死

### Bug 3: 鉴权错误不透传
- **原因**: `sign_token()` 失败只打印日志，前端只看到"连接失败请检查配置"
- **修复**: `sign_token()` 返回 `(success, error_msg)` 元组，区分配置错误/网络错误/超时
- **影响**: 前端能显示具体原因（如"鉴权失败: 无效签名"）

### Bug 4: 重连不可靠
- **原因**: 指数退避首次等4s+；`_auto_reconnect` 通过 `create_task` 调度时循环可能已停
- **修复**: 固定2s间隔；加 `_reconnect_lock` 防并发；事件循环状态检查
- **影响**: 断连后更快恢复

### Bug 5: disconnect 状态不一致
- **原因**: `ws.close()` 异常被吞，connected 标志未正确清除
- **修复**: `_safe_close_ws()` 安全关闭，状态完整重置
- **影响**: 断开后界面状态正确

### Bug 6: 前端初始化崩溃
- **原因**: `init()` 中 `connect()` 无 try-catch，自动连接失败时整个页面初始化中断
- **修复**: 全链路 try-catch，连接失败也继续加载页面
- **影响**: 页面不再白屏

### Bug 7: 消息发送无超时
- **原因**: 所有 `ws.send()` 无超时，网络卡顿时永久阻塞
- **修复**: 统一加 `asyncio.wait_for(send, timeout=10)`
- **影响**: 发送卡住时能快速失败

### Bug 8: 接收循环重连丢失
- **原因**: `_receive_loop` 异常退出后，`create_task(self._auto_reconnect())` 在已停止的循环中丢失
- **修复**: 加锁防止并发重连；检查事件循环是否运行
- **影响**: 断连后能可靠触发重连

## 新增功能

- `/api/status` 返回 `last_error` 和 `reconnect_attempts` 字段
- 前端设置面板显示"最后错误"信息
- 前端连接按钮防重复点击 (`state.connecting`)
- 启动时检查配置完整性并警告
- `max_reconnect_attempts` 从 5 提高到 10
- `reconnect_delay` 固定 2s（不再指数退避）

## 兼容性

- Python >= 3.10
- 依赖: flask, websockets, requests, pillow, qcloud-cos-sdk (可选)
- 浏览器: Chrome/Edge/Safari 最新版
- 配置格式与 v4.0 完全兼容，直接复制 config.json 即可
