<p align="center">
  <img src="https://img.shields.io/badge/version-v5.0-blue.svg" alt="Version">
  <img src="https://img.shields.io/badge/license-GPL--3.0-green.svg" alt="License">
  <img src="https://img.shields.io/badge/python-%3E%3D3.10-yellow.svg" alt="Python">
  <img src="https://img.shields.io/badge/platform-Termux%20%7C%20Linux-orange.svg" alt="Platform">
</p>

<h1 align="center">🦞 元宝 Bot Web 控制台</h1>

<p align="center">
  一个运行在 Termux（Android）上的 Python Web 控制台<br>
  通过 WebSocket 操控「元宝派」聊天平台，提供完整的群管理 & 自动化能力
</p>

---

## 📢 声明

> 本项目基于 [anxi78/yuanbao_bot_client](https://github.com/anxi78/yuanbao_bot_client) 修改，原项目采用 **GPL-3.0** 协议，版权归原作者所有。
>
> 本项目代码同样基于 **GPL-3.0** 协议开源。

---

## ✨ 功能一览

| 模块 | 说明 |
|------|------|
| 💬 消息管理 | 收发群消息、@全体成员、图片/贴纸/文件发送 |
| 🔄 撤回监听 | 被撤回的图片/文件自动重发 + 撤回通知（可选发到原群或中转群） |
| 🎨 AI 图片生成 | 收到 "请生成一张图片…" 自动调用 AI 作图并发送到群聊 |
| 🤖 自动回复 | 支持优先级排序、启停控制、增删改查 |
| 🏷️ 铭牌系统 | 群成员自定义铭牌、认证蓝标、自定义头像，支持导入导出 |
| 🔌 插件生态 | `PluginManager` 支持安装/启停/重载，内置生命周期钩子 |
| 📝 消息落盘 | 每条群消息自动写入本地文件，按日期滚动，无数量上限 |
| 📊 诊断接口 | 增强错误码、重连统计、状态监控 |

---

## 📁 项目结构

```
.
├── app.py                  # 主程序（~177KB）
├── config.json             # 配置文件
├── requirements.txt        # Python 依赖
├── templates/
│   └── index.html          # 前端单页应用（~200KB 纯 HTML/JS）
├── plugins/                # 插件目录（可扩展）
├── logs/                   # 消息日志（自动创建）
│   ├── messages_YYYYMMDD.log   # JSONL 格式
│   └── messages_YYYYMMDD.txt   # 人类可读格式
└── README.md
```

---

## 🛠️ 安装与使用

### 1️⃣ 安装 Termux

推荐从官方源下载：

- **F-Droid**：<https://f-droid.org/packages/com.termux/>
- **GitHub Releases**：<https://github.com/termux/termux-app/releases>

### 2️⃣ 下载项目

下载并解压项目安装包到设备存储目录，或依次下载各项目文件到设备存储目录

### 3️⃣ 安装依赖

```bash
pkg install python-pip
pip install -r requirements.txt
```

### 4️⃣ 修改配置

建议下载 **MT 管理器**（<https://mt2.cn/>），打开项目目录下 `config.json`，修改以下字段：

| 配置项 | 说明 |
|--------|------|
| `APP_KEY` | 龙虾密钥（可在关联界面获取） |
| `APP_SECRET` | 龙虾密钥（可在关联界面获取） |
| `DEFAULT_GROUP_CODE` | 默认派派号 |
| `IMAGE_GROUP_CODE` | 中转派派号 |

> 💡 **提示**：如果没有 `APP_KEY` 和 `APP_SECRET`，可以先点击关联界面，把两个 `app` 开头的值复制到配置文件对应位置，运行程序后点击"我已操作"即可。

### 5️⃣ 启动程序

```bash
cd ~/storage/shared/你的项目文件夹
python app.py
```

### 6️⃣ 打开控制台

浏览器访问：<http://127.0.0.1:5000>

---

## 📡 API 接口

### 消息日志接口

| 接口 | 方法 | 用途 |
|------|------|------|
| `/api/msg-log/stats` | GET | 获取统计信息 |
| `/api/msg-log/files` | GET | 获取历史文件列表 |
| `/api/msg-log/recent?count=100&fmt=text` | GET | 读取最近 N 条消息 |
| `/api/msg-log/download?fmt=txt` | GET | 下载今日日志文件 |
| `/api/msg-log/clear-today` | POST | 清空今日日志 |

### 状态接口

| 接口 | 方法 | 说明 |
|------|------|------|
| `/api/status` | GET | 返回连接状态，含 `last_error` 和 `reconnect_attempts` 字段 |

---

## ⚙️ 配置说明

### 重连策略

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `max_reconnect_attempts` | `20` | 最大重连次数 |
| `reconnect_delay` | `2s` | 重连间隔（固定，非指数退避） |

### 心跳配置

| 参数 | 默认值 | 说明 |
|------|--------|------|
| 心跳间隔（v5.0） | `10s` | 自动刷新开关可切换为每 2 秒 |
| 心跳间隔（v4.0 及之前） | `1s` | 早期版本默认值 |

---

## 📋 兼容性

- **Python**：>= 3.10
- **依赖**：`flask`、`websockets`、`requests`、`Pillow`、`cos-python-sdk-v5`（可选）
- **浏览器**：Chrome / Edge / Safari 最新版
- **系统**：Termux（Android）、Linux 服务器

---

## ⚠️ 注意事项

- 消息文件记录**无上限**，长时间运行请定期清理或挂载大磁盘。
- 写入在**后台线程**完成，不阻塞消息接收。
- 程序退出时自动 **flush** 剩余消息到磁盘。
- 日志文件位置：`./logs/messages_YYYYMMDD.log` 和 `.txt`，与 `app.py` 同级。

---

## 📜 更新日志

### v5.0（2026-08-07）

- ✅ 设置页新增 `config.json` 可视化编辑器，自动回复规则支持弹窗编辑
- ✅ 新增撤回消息监听，被撤回内容自动重发并通知
- ✅ 新增 AI 图片生成功能
- ✅ 支持 @全体成员 协议，增强错误码与诊断接口
- ✅ 新增群成员铭牌系统（铭牌/蓝标/头像，支持导入导出）
- ✅ 新增自动回复规则管理（优先级/启停/增删改）
- ✅ 新增插件生态 `PluginManager`（安装/启停/重载/生命周期钩子）
- ✅ 撤回通知可选发送到中转群或原群
- ✅ 优化稳定性

### v4.0（2026-07-xx）

- ✅ 贴纸发送页面新增双击自动发送
- ✅ 新增派名显示功能
- ✅ 展开编辑器
- ✅ 默认心跳间隔改为 10 秒
- ✅ 顶部新增「自动」刷新开关（每 2 秒）

### v3.0（2026-07-24）

- ✅ 修复获取群成员列表异常问题
- ✅ @用户操作简化，无需填写英文冒号
- ✅ @用户后默认保留内容
- ✅ 移除白名单/黑名单群聊输入框
- ✅ 新增 @元宝开关（消息转发到中转群，默认开启）
- ✅ 打开即自动连接 Bot
- ✅ 刷屏默认间隔 0.1s，心跳默认 1s
- ✅ 修复图片发送功能
- ✅ 消息列表 & 代理队列实时自动刷新

---

## 📄 开源协议

本项目基于 **GPL-3.0** 协议开源，详情见 [LICENSE](LICENSE) 文件。

---

<p align="center">
  由 <a href="https://github.com/anxi78/yuanbao_bot_client">anxi78/yuanbao_bot_client</a> 修改而来<br>
  使用 opencode（deepseek v4 flash）辅助编程
</p>
