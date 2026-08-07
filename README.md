# yuanbao_bot_client-Web
# 元宝 Bot Web 控制台v5.0

###声明：本项目基于anxi78 的yuanbao_bot_client修改，原项目采用GPL–3.0协议，原项目地址github.com/anxi78/yuanbao_bot_client，版权归原作者所有。本项目由opencode软件deepseek v4 flash模型编程。

#项目介绍：
这是一个运行在 Termux（Android） 上的
  Python Web控制台项目，项目名为「龙虾控制台」

 #基本情况
 - 语言/框架：Python +
  Flask，前端是单文件
  templates/index.html（约 200KB 的纯
  HTML/JS）
  - 核心文件：app.py（约 177KB 主程序）、c
  onfig.json（配置）、templates/index.html
  （界面）、plugins/（插件目录，目前为空）
  - 依赖：requests、websockets、cos-python
  -sdk-v5、flask
 #它是做什么的？
基于对 anxi78/yuanbao_bot_client（GPL-3.
  0）的修改，是一个 元宝/龙虾 Bot
  的网页管理后台，通过 WebSocket 连接 Bot
  并操控一个叫「派派」的聊天平台：
  - 收发群消息、@全体成员、图片/贴纸/文件
  发送
  - 撤回监听：被撤回的图片/文件自动重发 +
  撤回通知
  - AI 图片生成：收到"请生成一张图片…"自动
  作图发到群
  - 自动回复规则（优先级/启停/增删改）
  - 群成员铭牌系统（自定义铭牌、蓝标、头像
  ，可导入导出）
  - 插件生态：PluginManager
  支持安装/启停/重载
  - 消息落盘：每条群消息写入
  logs/messages_YYYYMMDD.log/.txt
  
# 使用说明：
1：安装termux终端模拟器软件，https://m.ddooo.com/softdown/244642.htm
2：安装依赖：(打开软件后依次输入后回车)
             pkg install python-pip
             pip install requests
             pip install websockets
             pip install flask
2：下载项目压缩包文件至存储目录
3：修改龙虾秘钥：建议下载mt管理器
https://mt2.cn/ ， 用软件打开文件夹下config.json文件，修改APP_KEY，APP_SECRET[如果没有的话可以点关联界面，然后把那两个app开头的复制到配置文件里对应的位置，然后直接运行文件(运行文件的方法见4和5)，再点击我已操作就行了]，DEFAULT_GROUP_CODE(默认派派号)，IMAGE_GROUP_CODE(中转派派号) ，点击保存。
4：打开termux软件，cd到你的项目文件夹(例如：cd ~/storage/shared/XXX)
5：输入python app.py，回车，即可运行程序
6：浏览器输入http://127.0.0.1:5000     

# 更新内容(v5.0)
1：设置页新增 config.json 可视化编辑器，自动回复规则可点击「展开编辑」弹窗编辑

2：新增撤回消息监听，被撤回的图片/文件/贴纸自动重发并发送撤回通知

3：新增 AI 图片生成，收到"请生成一张图片..."提示词自动作图并发送到群聊

4：支持 @全体成员 协议，增强错误码与诊断接口

5：新增群成员铭牌系统：自定义铭牌 / 认证蓝标 / 自定义头像，支持导出导入

6：新增自动回复规则管理：优先级 / 启停 / 增删改

7：新增插件生态（PluginManager），支持插件安装/启停/重载，内置插件生命周期钩子

8：撤回通知可以选择发到中转派或者原派

9：优化稳定性

# 更新内容(v4.0)
1：贴纸发送页面新增双击自动发送功能

2：新增派名显示功能

3：展开编辑器

4：增加稳定性

5：默认心跳间隔改为10 秒

6：顶部加「自动」刷新开关（每 2 秒）。

# 更新日期
2026年8月7日

# 版本号：蒲v5.0(20260807)


## 功能说明

在原有龙虾控制台基础上新增 **消息自动落盘** 功能：

- 接收到的每条群聊消息自动写入本地文件
- 前端实时显示最多 **500 条**
- 后台文件记录 **不限制数量**，永久累积
- 按日期滚动：`logs/messages_YYYYMMDD.log` (JSONL) + `.txt` (可读)


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
- 配置格式与 v5.0 完全兼容，直接复制 config.json 即可

## 部署

1. 将 `app.py` 和 `templates/index.html` 放到服务器
2. 确保同目录有 `config.json`（参考原版配置）
3. 启动：`python app.py`
4. 浏览器打开 `http://IP:5000`，点底部「📒 记录」Tab 查看

## 日志文件位置

启动后自动创建 `logs/` 目录（与 app.py 同级），例如：
- `logs/messages_20260727.log` — JSONL 格式，便于程序解析
- `logs/messages_20260727.txt` — 人类可读，支持 `tail -f`

## 新增 API

| 接口 | 用途 |
|---|---|
| `GET /api/msg-log/stats` | 统计信息 |
| `GET /api/msg-log/files` | 历史文件列表 |
| `GET /api/msg-log/recent?count=100&fmt=text` | 读最近 N 条 |
| `GET /api/msg-log/download?fmt=txt` | 下载今日文件 |
| `POST /api/msg-log/clear-today` | 清空今日 |

## 注意

- 文件记录无上限，长时间运行请定期清理或挂载大磁盘
- 写入在后台线程完成，不阻塞消息接收
- 程序退出时自动 flush 剩余消息


## 历史版本(v3.0)
1:修复获取群成员列表异常问题
2：增加输入框，简便@用户操作，@艾特用户不用填写英文冒号
3：@用户后默认保留内容
4：删除设置中“白名单群聊”“黑名单群聊”输入框
5：@元宝开关，支持直接将目标群的消息转发到中转群，不@元宝，默认开启，可手动关闭
6：打开网址或软件后自动连接龙虾bot，省去手动连接步骤
7：刷屏默认间隔更改为0.1s
8：心跳默认间隔更改为1s
9：修复图片发送功能
10:消息列表实时自动刷新，代理模式处理队列实时自动刷新
11：优化脚本稳定性
12：更新日期：2026年7月24日（由豆包办公模式和元宝超级元宝模式完成）

## 许可

本项目代码基于GPL-3.0协议开源。