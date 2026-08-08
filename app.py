#!/usr/bin/env python3
"""
元宝 Bot Web 控制台 - v5.2
"""
import sys
import os
import json
import asyncio
import threading
import time
import hashlib
import hmac
import random
import string
import uuid
import tempfile
import re
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Tuple, Any
from collections import deque
import requests
from flask import Flask, jsonify, request, render_template, Response, stream_with_context, send_file
import websockets

# ═══════════════════════════════════════════
#  消息本地文件记录器
# ═══════════════════════════════════════════
class MessageLogger:
    """将接收到的群聊消息实时写入本地文件（不限制数量）

    - 按日期滚动：logs/messages_YYYYMMDD.log (JSONL) + .txt (可读)
    - 后台线程 + 队列批量刷盘，不阻塞主事件循环
    - 文件记录无上限，永久累积（直到磁盘满）
    - 可通过 enable()/disable() 随时开关写入（默认开启）
    """

    MAX_FRONTEND_CACHE = 500  # 前端实时显示最大条数

    def __init__(self, base_dir: str = None, enabled: bool = True):
        if base_dir is None:
            base_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)
        self._queue: deque = deque()
        self._lock = threading.Lock()
        self._running = True
        self._total_written = 0
        self._today_date = datetime.now().strftime("%Y%m%d")
        self.enabled = enabled  # 开关状态（默认开启）
        self._thread = threading.Thread(target=self._write_loop, daemon=True)
        self._thread.start()

    def enable(self):
        """开启本地消息记录"""
        self.enabled = True
        print("[MessageLogger] ✅ 本地消息记录已开启")

    def disable(self):
        """关闭本地消息记录（已入队的消息会被丢弃，不再写入文件）"""
        self.enabled = False
        # 清空待写队列，避免内存膨胀
        with self._lock:
            self._queue.clear()
        print("[MessageLogger] ⏹️ 本地消息记录已关闭")

    def _get_paths(self):
        today = datetime.now().strftime("%Y%m%d")
        if today != self._today_date:
            self._today_date = today
        stem = os.path.join(self.base_dir, f"messages_{self._today_date}")
        return f"{stem}.log", f"{stem}.txt"

    def log(self, entry: dict):
        """将一条消息入队（线程安全，非阻塞）

        当 enabled=False 时直接丢弃，不入队、不写入。
        """
        if not self.enabled:
            return
        with self._lock:
            self._queue.append(entry)

    def _write_loop(self):
        """后台线程：攒批 → 双写 JSONL + TXT → 跨天自动切文件

        当 self.enabled=False 时，清空待写缓冲区并休眠，不执行任何文件写入。
        线程始终保持运行，以便开关重新打开后立即恢复写入。
        """
        buf_jsonl = []
        buf_txt = []
        last_flush = time.time()
        FLUSH_INTERVAL = 2.0
        MIN_BATCH = 10

        while self._running:
            try:
                now = time.time()

                # 开关关闭时：丢弃缓冲区和队列中的内容，短暂休眠后继续循环
                if not self.enabled:
                    buf_jsonl.clear()
                    buf_txt.clear()
                    with self._lock:
                        self._queue.clear()
                    time.sleep(0.5)
                    continue

                with self._lock:
                    while self._queue:
                        entry = self._queue.popleft()
                        buf_jsonl.append(json.dumps(entry, ensure_ascii=False))
                        buf_txt.append(self._format_txt(entry))

                # 跨天检测
                today = datetime.now().strftime("%Y%m%d")
                if today != self._today_date:
                    self._flush(buf_jsonl, buf_txt)
                    buf_jsonl.clear()
                    buf_txt.clear()
                    self._today_date = today

                # 批量刷盘
                if buf_jsonl and (len(buf_jsonl) >= MIN_BATCH or now - last_flush >= FLUSH_INTERVAL):
                    self._flush(buf_jsonl, buf_txt)
                    buf_jsonl.clear()
                    buf_txt.clear()
                    last_flush = now

                if not buf_jsonl:
                    time.sleep(0.2)
            except Exception as e:
                print(f"[MessageLogger] ❌ 写入异常: {e}")
                time.sleep(1)

        self._flush(buf_jsonl, buf_txt)

    def _flush(self, buf_jsonl: list, buf_txt: list):
        if not buf_jsonl:
            return
        jsonl_path, txt_path = self._get_paths()
        try:
            with open(jsonl_path, 'a', encoding='utf-8') as f:
                f.write('\n'.join(buf_jsonl) + '\n')
                f.flush()
            with open(txt_path, 'a', encoding='utf-8') as f:
                f.write('\n'.join(buf_txt) + '\n')
                f.flush()
            self._total_written += len(buf_jsonl)
        except Exception as e:
            print(f"[MessageLogger] ❌ 刷盘失败: {e}")

    def _format_txt(self, entry: dict) -> str:
        ts = entry.get("timestamp", "")
        gc = entry.get("group_code", "")
        name = entry.get("sender_name", "")
        uid = entry.get("sender_id", "")
        content = entry.get("content", "")
        media = entry.get("media_info") or {}
        media_tag = ""
        if media.get("type") == "image":
            media_tag = " [图片]"
        elif media.get("type") == "sticker":
            media_tag = " [贴纸]"
        elif media.get("type") == "file":
            media_tag = f" [文件:{media.get('file_name','')}]"
        return f"[{ts}] [群:{gc}] {name}({uid}): {content}{media_tag}"

    def stats(self) -> dict:
        jsonl_path, txt_path = self._get_paths()
        jsonl_size = os.path.getsize(jsonl_path) if os.path.exists(jsonl_path) else 0
        txt_size = os.path.getsize(txt_path) if os.path.exists(txt_path) else 0
        with self._lock:
            pending = len(self._queue)
        total_size = 0
        file_count = 0
        for fn in os.listdir(self.base_dir):
            if fn.startswith("messages_") and (fn.endswith(".log") or fn.endswith(".txt")):
                fp = os.path.join(self.base_dir, fn)
                total_size += os.path.getsize(fp)
                file_count += 1
        return {
            "today_written": self._total_written,
            "pending": pending,
            "today_jsonl_size": jsonl_size,
            "today_txt_size": txt_size,
            "total_size": total_size,
            "file_count": file_count,
            "today_date": self._today_date,
        }

    def list_files(self) -> list:
        files = []
        for fn in sorted(os.listdir(self.base_dir), reverse=True):
            if fn.startswith("messages_") and (fn.endswith(".log") or fn.endswith(".txt")):
                fp = os.path.join(self.base_dir, fn)
                files.append({
                    "name": fn,
                    "size": os.path.getsize(fp),
                    "mtime": datetime.fromtimestamp(os.path.getmtime(fp)).strftime("%Y-%m-%d %H:%M:%S"),
                })
        return files

    def read_recent(self, count: int = 100, fmt: str = "text") -> list:
        jsonl_path, txt_path = self._get_paths()
        target = jsonl_path if fmt == "jsonl" else txt_path
        if not os.path.exists(target):
            return []
        with open(target, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        return [l.rstrip('\n') for l in lines[-count:]]

    def shutdown(self):
        self._running = False
        deadline = time.time() + 3
        while time.time() < deadline and self._queue:
            time.sleep(0.05)

# ═══════════════════════════════════════════
#  配置加载
# ═══════════════════════════════════════════
def load_config() -> dict:
    """加载配置文件，自动处理路径"""
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.json')
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"配置文件不存在: {config_path}")
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)

app_config = load_config()
PORT = app_config.get('PORT', 5000)
APP_KEY = app_config.get('APP_KEY', '')
APP_SECRET = app_config.get('APP_SECRET', '')
API_DOMAIN = app_config.get('API_DOMAIN', '')
WS_URL = app_config.get('WS_URL', '')

# ═══════════════════════════════════════════
#  协议常量
# ═══════════════════════════════════════════
CMD_TYPE_REQUEST = 0
CMD_TYPE_RESPONSE = 1
CMD_TYPE_PUSH = 2
CMD_AUTH_BIND = "auth-bind"
CMD_PING = "ping"
MODULE_CONN_ACCESS = "conn_access"
BIZ_MODULE = "yuanbao_openclaw_proxy"
BIZ_CMD_SEND_C2C = "send_c2c_message"
BIZ_CMD_SEND_GROUP = "send_group_message"
BIZ_CMD_GET_MEMBERS = "get_group_member_list"
BIZ_CMD_QUERY_GROUP_INFO = "query_group_info"

# 默认元宝 ID
DEFAULT_YUANBAO_ID = "szUvRH8s4ekettawNjDREmAG4W7h+Lhb8Sy9tq/otZU="
IMAGE_GROUP_CODE = app_config.get('IMAGE_GROUP_CODE', '')
YUANBAO_BOT_ID = app_config.get('YUANBAO_ID', DEFAULT_YUANBAO_ID)
YUANBAO_NICKNAME = '元宝'

# HTTP 请求超时
HTTP_TIMEOUT = 30

# @所有人 特殊用户 ID（服务端识别此 ID 即触发 @全体成员 推送）
AT_ALL_SPECIAL_ID = "NTNX+5sHarbiWHHk+P1yHw=="

# ═══════════════════════════════════════════
#  简易 Protobuf 编解码器
# ═══════════════════════════════════════════
class SimpleProtobufCodec:
    """最小化 Protobuf 编解码，仅覆盖本服务需要的字段"""

    # ── 编码 ──
    @staticmethod
    def encode_varint(value: int) -> bytes:
        result = []
        while value > 127:
            result.append((value & 0x7F) | 0x80)
            value >>= 7
        result.append(value)
        return bytes(result)

    @staticmethod
    def encode_string(field_num: int, value: str) -> bytes:
        tag = (field_num << 3) | 2
        encoded = value.encode('utf-8')
        return bytes([tag]) + SimpleProtobufCodec.encode_varint(len(encoded)) + encoded

    @staticmethod
    def encode_message_field(field_num: int, encoded_msg: bytes) -> bytes:
        tag = (field_num << 3) | 2
        return bytes([tag]) + SimpleProtobufCodec.encode_varint(len(encoded_msg)) + encoded_msg

    @staticmethod
    def encode_uint32(field_num: int, value: int) -> bytes:
        tag = (field_num << 3) | 0
        return bytes([tag]) + SimpleProtobufCodec.encode_varint(value)

    @staticmethod
    def encode_head(cmd_type: int, cmd: str, seq_no: int, msg_id: str, module: str) -> bytes:
        data = b''
        data += bytes([(1 << 3) | 0]) + SimpleProtobufCodec.encode_varint(cmd_type)
        data += SimpleProtobufCodec.encode_string(2, cmd)
        data += bytes([(3 << 3) | 0]) + SimpleProtobufCodec.encode_varint(seq_no)
        data += SimpleProtobufCodec.encode_string(4, msg_id)
        data += SimpleProtobufCodec.encode_string(5, module)
        return data

    @staticmethod
    def encode_conn_msg(head: bytes, data: bytes = b'') -> bytes:
        result = SimpleProtobufCodec.encode_message_field(1, head)
        if data:
            result += SimpleProtobufCodec.encode_message_field(2, data)
        return result

    @staticmethod
    def encode_send_group_msg_req(msg_id: str, group_code: str, from_account: str,
                                   text: str, ref_msg_id: str = "") -> bytes:
        data = b''
        data += SimpleProtobufCodec.encode_string(1, msg_id)
        data += SimpleProtobufCodec.encode_string(2, group_code)
        data += SimpleProtobufCodec.encode_string(3, from_account)
        data += SimpleProtobufCodec.encode_string(5, str(random.randint(1, 999999999)))
        msg_content = SimpleProtobufCodec.encode_string(1, text)
        msg_body_elem = SimpleProtobufCodec.encode_string(1, "TIMTextElem")
        msg_body_elem += SimpleProtobufCodec.encode_message_field(2, msg_content)
        data += SimpleProtobufCodec.encode_message_field(6, msg_body_elem)
        if ref_msg_id:
            data += SimpleProtobufCodec.encode_string(7, ref_msg_id)
        return data

    @staticmethod
    def encode_send_c2c_msg_req(msg_id: str, to_account: str, from_account: str, text: str) -> bytes:
        data = b''
        data += SimpleProtobufCodec.encode_string(1, msg_id)
        data += SimpleProtobufCodec.encode_string(2, to_account)
        data += SimpleProtobufCodec.encode_string(3, from_account)
        data += bytes([(4 << 3) | 0]) + SimpleProtobufCodec.encode_varint(random.randint(1, 999999999))
        msg_content = SimpleProtobufCodec.encode_string(1, text)
        msg_body_elem = SimpleProtobufCodec.encode_string(1, "TIMTextElem")
        msg_body_elem += SimpleProtobufCodec.encode_message_field(2, msg_content)
        data += SimpleProtobufCodec.encode_message_field(5, msg_body_elem)
        return data

    @staticmethod
    def encode_get_group_member_list_req(group_code: str) -> bytes:
        return SimpleProtobufCodec.encode_string(1, group_code)

    # ── 解码 ──
    @staticmethod
    def decode_varint(data: bytes, pos: int) -> tuple:
        value = 0
        shift = 0
        while pos < len(data):
            byte = data[pos]
            pos += 1
            value |= (byte & 0x7F) << shift
            if not (byte & 0x80):
                break
            shift += 7
        return value, pos

    @staticmethod
    def decode_conn_msg(data: bytes) -> Optional[dict]:
        result = {"head": {}, "data": b""}
        i = 0
        while i < len(data):
            if i >= len(data):
                break
            tag = data[i]
            i += 1
            field_num = tag >> 3
            wire_type = tag & 7
            if wire_type != 2:
                break
            length, i = SimpleProtobufCodec.decode_varint(data, i)
            if i + length > len(data):
                break
            field_data = data[i:i + length]
            i += length
            if field_num == 1:
                result["head"] = SimpleProtobufCodec.decode_head(field_data)
            elif field_num == 2:
                result["data"] = field_data
        return result

    @staticmethod
    def decode_head(data: bytes) -> dict:
        head = {"cmd_type": 0}
        i = 0
        while i < len(data):
            tag = data[i]
            i += 1
            field_num = tag >> 3
            wire_type = tag & 7
            if wire_type == 0:
                val, i = SimpleProtobufCodec.decode_varint(data, i)
                if field_num == 1:
                    head["cmd_type"] = val
                elif field_num == 3:
                    head["seq_no"] = val
            elif wire_type == 2:
                length, i = SimpleProtobufCodec.decode_varint(data, i)
                if i + length > len(data):
                    break
                field_data = data[i:i + length]
                i += length
                if field_num == 2:
                    head["cmd"] = field_data.decode('utf-8', errors='replace')
                elif field_num == 4:
                    head["msg_id"] = field_data.decode('utf-8', errors='replace')
                elif field_num == 5:
                    head["module"] = field_data.decode('utf-8', errors='replace')
        return head

    @staticmethod
    def decode_get_group_member_list_rsp(data: bytes) -> dict:
        result = {"code": 0, "message": "", "member_list": []}
        i = 0
        while i < len(data):
            if i >= len(data):
                break
            tag = data[i]
            i += 1
            field_num = tag >> 3
            wire_type = tag & 7
            if wire_type == 0:
                val, i = SimpleProtobufCodec.decode_varint(data, i)
                if field_num == 1:
                    result["code"] = val
            elif wire_type == 2:
                length, i = SimpleProtobufCodec.decode_varint(data, i)
                if i + length > len(data):
                    break
                field_data = data[i:i + length]
                i += length
                if field_num == 2:
                    result["message"] = field_data.decode('utf-8', errors='replace')
                elif field_num == 3:
                    member = SimpleProtobufCodec._decode_member(field_data)
                    if member:
                        result["member_list"].append(member)
        return result

    @staticmethod
    def _decode_member(data: bytes) -> Optional[dict]:
        """解码单个群成员。
        field_num 3 = 成员类型: 1=真人成员(含群主/管理/普通), 2=内置元宝AI, 3=API机器人
        """
        member = {}
        i = 0
        while i < len(data):
            tag = data[i]
            i += 1
            field_num = tag >> 3
            wire_type = tag & 7
            if wire_type == 0:
                val, i = SimpleProtobufCodec.decode_varint(data, i)
                if field_num == 3:
                    member["member_type"] = val
            elif wire_type == 2:
                length, i = SimpleProtobufCodec.decode_varint(data, i)
                if i + length > len(data):
                    break
                field_data = data[i:i + length]
                i += length
                if field_num == 1:
                    member["user_id"] = field_data.decode('utf-8', errors='replace')
                elif field_num == 2:
                    member["nick_name"] = field_data.decode('utf-8', errors='replace')
        # 兜底：bot_ 开头的 ID 强制设为机器人
        uid = member.get("user_id", "")
        if uid.startswith("bot_") and member.get("member_type") != 3:
            member["member_type"] = 3
        return member if member else None

    @staticmethod
    def _decode_group_info(data: bytes) -> dict:
        """解码 GroupInfo 嵌套消息"""
        info = {}
        i = 0
        while i < len(data):
            tag = data[i]
            i += 1
            field_num = tag >> 3
            wire_type = tag & 7
            if wire_type == 0:
                val, i = SimpleProtobufCodec.decode_varint(data, i)
                if field_num == 4:
                    info["group_size"] = val
            elif wire_type == 2:
                length, i = SimpleProtobufCodec.decode_varint(data, i)
                if i + length > len(data):
                    break
                field_data = data[i:i + length]
                i += length
                s = field_data.decode('utf-8', errors='replace')
                if field_num == 1:
                    info["group_name"] = s
                elif field_num == 2:
                    info["group_owner_user_id"] = s
                elif field_num == 3:
                    info["group_owner_nickname"] = s
        return info

    @staticmethod
    def decode_query_group_info_rsp(data: bytes) -> dict:
        """解码 QueryGroupInfoRsp protobuf 数据"""
        result = {"code": 0, "message": "", "group_info": {}}
        i = 0
        while i < len(data):
            tag = data[i]
            i += 1
            field_num = tag >> 3
            wire_type = tag & 7
            if wire_type == 0:
                val, i = SimpleProtobufCodec.decode_varint(data, i)
                if field_num == 1:
                    result["code"] = val
                elif field_num == 4:
                    result["group_size"] = val
            elif wire_type == 2:
                length, i = SimpleProtobufCodec.decode_varint(data, i)
                if i + length > len(data):
                    break
                field_data = data[i:i + length]
                i += length
                if field_num == 2:
                    result["message"] = field_data.decode('utf-8', errors='replace')
                elif field_num == 3:
                    result["group_info"] = SimpleProtobufCodec._decode_group_info(field_data)
        return result

    # ── 高级元素编码 ──
    @staticmethod
    def encode_tim_face_elem(sticker_id: str, package_id: str, name: str,
                              width: int = 128, height: int = 128, formats: str = "png") -> bytes:
        data_json = json.dumps({
            "sticker_id": sticker_id, "package_id": package_id,
            "width": width, "height": height, "formats": formats, "name": name,
        }, ensure_ascii=False)
        msg_content = b''
        msg_content += bytes([(9 << 3) | 0]) + SimpleProtobufCodec.encode_varint(0)
        msg_content += SimpleProtobufCodec.encode_string(4, data_json)
        elem = b''
        elem += SimpleProtobufCodec.encode_string(1, "TIMFaceElem")
        elem += SimpleProtobufCodec.encode_message_field(2, msg_content)
        return elem

    @staticmethod
    def encode_tim_image_elem(url: str, uuid: str = "", size: int = 0,
                               width: int = 0, height: int = 0, image_format: int = 255) -> bytes:
        """编码 TIMImageElem 图片消息元素

        v3.1: 与 sender.py 的 encode_tim_image_elem 保持一致的 proto 结构:
          image_info_array (field 8) 内含:
            type (field 1) = 1 (原始图片)
            size (field 2)
            width (field 3)
            height (field 4)
            url (field 5)
          msg_content:
            uuid (field 2)
            image_format (field 3) = 255
            image_info_array (field 8)
        """
        img_info = b''
        img_info += bytes([(1 << 3) | 0]) + SimpleProtobufCodec.encode_varint(1)  # type=1
        img_info += bytes([(2 << 3) | 0]) + SimpleProtobufCodec.encode_varint(size)
        img_info += bytes([(3 << 3) | 0]) + SimpleProtobufCodec.encode_varint(width)
        img_info += bytes([(4 << 3) | 0]) + SimpleProtobufCodec.encode_varint(height)
        img_info += SimpleProtobufCodec.encode_string(5, url)
        mc = b''
        if uuid:
            mc += SimpleProtobufCodec.encode_string(2, uuid)
        mc += bytes([(3 << 3) | 0]) + SimpleProtobufCodec.encode_varint(image_format)
        mc += SimpleProtobufCodec.encode_message_field(8, img_info)
        elem = b''
        elem += SimpleProtobufCodec.encode_string(1, "TIMImageElem")
        elem += SimpleProtobufCodec.encode_message_field(2, mc)
        return elem

    @staticmethod
    def encode_tim_file_elem(url: str, uuid: str = "", file_size: int = 0, file_name: str = "") -> bytes:
        mc = b''
        if uuid:
            mc += SimpleProtobufCodec.encode_string(2, uuid)
        mc += SimpleProtobufCodec.encode_string(10, url)
        if file_size:
            mc += bytes([(11 << 3) | 0]) + SimpleProtobufCodec.encode_varint(file_size)
        if file_name:
            mc += SimpleProtobufCodec.encode_string(12, file_name)
        elem = b''
        elem += SimpleProtobufCodec.encode_string(1, "TIMFileElem")
        elem += SimpleProtobufCodec.encode_message_field(2, mc)
        return elem


# ═══════════════════════════════════════════
#  增强发送器
# ═══════════════════════════════════════════
class EnhancedSpamSender:
    """核心 Bot 客户端：鉴权、心跳、收发消息、代理转发"""

    def __init__(self):
        self.token: Optional[str] = None
        self.bot_id: Optional[str] = None
        self.instance_id: Optional[str] = None
        self.ws = None
        self.connected = False
        self.seq_no = 0
        self.group_code: Optional[str] = None
        self.codec = SimpleProtobufCodec()
        self.user_db: Dict[str, str] = {}
        self.pending_requests: Dict[str, asyncio.Future] = {}
        self.msg_cache: List[dict] = []
        self._seen_proxy_msg_ids: set = set()
        self.heartbeat_task = None
        self.receive_task = None
        self.auto_reply_enabled = False
        self.heartbeat_interval = app_config.get('HEARTBEAT_INTERVAL', 10)
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 20
        # ── v4.0: 重连稳定性 ──
        self._should_reconnect = False       # 仅自动连接时允许自动重连（手动断开不重连）
        self._reconnect_in_progress = False  # 防止并发重连任务
        self._reconnect_lock = None          # 懒初始化（需在事件循环中创建）

        # 自动回复配置
        self.AUTO_REPLY_RULES = app_config.get('AUTO_REPLY_RULES', [])
        self.DEFAULT_REPLY = app_config.get('DEFAULT_REPLY', '')
        self.AUTO_REPLY_GROUP_TEXT = app_config.get('AUTO_REPLY_GROUP_TEXT', '@我干啥')
        self.AUTO_REPLY_C2C_TEXT = app_config.get('AUTO_REPLY_C2C_TEXT', '我是Bot')

        # 代理模式配置
        self.forward_at_yuanbao = app_config.get('FORWARD_AT_YUANBAO', True)

        # 群聊管理
        self.groups: Dict[str, Dict] = {}
        self.current_group: Optional[str] = None

        # 代理模式状态
        self.auto_reply_text: Optional[str] = None
        self.auto_reply_at_only: bool = False
        self._proxy_queue: deque = deque()
        self._proxy_worker_task: Optional[asyncio.Task] = None
        self._proxy_worker_running: bool = False

        # 元宝图片回复等待（对应 sender.py 的 _pending_image_future）
        self._pending_image_future: Optional[asyncio.Future] = None
        # AI 图片生成请求（/api/send/ai-image）：{"future": Future, "target_group": str}
        self._ai_image_request: Optional[dict] = None

        # 撤回监听开关
        self.recall_monitor_enabled: bool = True
        # 撤回通知开关（是否发送撤回通知到群）
        self.recall_notify_enabled: bool = False
        # 撤回通知目标：original 原群 / relay 中转群
        self.recall_notify_target: str = "original"
        # 撤回消息缓存
        self.recall_cache: list = []

        # 贴纸库
        self.STICKERS = self._init_stickers()

    @staticmethod
    def _init_stickers() -> dict:
        """集中管理贴纸数据"""
        stickers = {}
        sticker_data = [
            ("六六六", "278"), ("我想开了", "262"), ("害羞", "130"), ("比心", "252"),
            ("委屈", "125"), ("亲亲", "146"), ("酷", "131"), ("睡", "145"),
            ("发呆", "152"), ("可怜", "157"), ("摊手", "200"), ("头大", "213"),
            ("吓", "256"), ("吐血", "203"), ("哼", "185"), ("嘿嘿", "220"),
            ("头秃", "218"), ("暗中观察", "221"), ("我酸了", "224"), ("打call", "246"),
            ("庆祝", "251"), ("奋斗", "151"), ("惊讶", "143"), ("疑问", "144"),
            ("仔细分析", "248"), ("撅嘴", "184"), ("泪奔", "199"), ("尊嘟假嘟", "276"),
            ("略略略", "113"), ("困", "180"), ("折磨", "181"), ("抠鼻", "182"),
            ("鼓掌", "183"), ("斜眼笑", "204"), ("辣眼睛", "216"), ("哦哟", "217"),
            ("吃瓜", "222"), ("狗头", "225"), ("敬礼", "227"), ("哦", "231"),
            ("拿到红包", "236"), ("牛吖", "239"), ("贴贴", "272"), ("爱心", "138"),
            ("晚安", "170"), ("太阳", "176"), ("柠檬", "266"), ("大冤种", "267"),
            ("吐了", "132"), ("怒", "134"), ("玫瑰", "165"), ("凋谢", "119"),
            ("点赞", "159"), ("握手", "164"), ("抱拳", "163"), ("ok", "169"),
            ("拳头", "174"), ("鞭炮", "191"), ("烟花", "258"),
        ]
        for name, sid in sticker_data:
            stickers[name] = {
                "sticker_id": sid, "package_id": "1003", "name": name,
                "width": 128, "height": 128, "formats": "png"
            }
        return stickers

    # ── 工具方法 ──
    def _generate_msg_id(self) -> str:
        return uuid.uuid4().hex

    def _get_beijing_time(self) -> str:
        return (datetime.now(timezone.utc) + timedelta(hours=8)).strftime("%Y-%m-%dT%H:%M:%S+08:00")

    # ── 鉴权 ──
    def sign_token(self) -> bool:
        """向服务端换取 token"""
        url = f"https://{API_DOMAIN}/api/v5/robotLogic/sign-token"
        nonce = ''.join(random.choices(string.hexdigits.lower(), k=32))
        timestamp = self._get_beijing_time()
        plain = f"{nonce}{timestamp}{APP_KEY}{APP_SECRET}"
        signature = hmac.new(APP_SECRET.encode(), plain.encode(), hashlib.sha256).hexdigest()
        self.instance_id = str(random.randint(1, 1000))
        headers = {
            "Content-Type": "application/json",
            "X-AppVersion": "1.0.11",
            "X-OperationSystem": "linux",
            "X-Instance-Id": self.instance_id,
            "X-Bot-Version": "2026.3.22"
        }
        body = {"app_key": APP_KEY, "nonce": nonce, "signature": signature, "timestamp": timestamp}
        try:
            response = requests.post(url, headers=headers, json=body, timeout=HTTP_TIMEOUT)
            result = response.json()
            if result.get("code") == 0:
                self.token = result["data"]["token"]
                self.bot_id = result["data"]["bot_id"]
                return True
            print(f"[鉴权] 失败: {result.get('message', '未知错误')}")
            return False
        except Exception as e:
            print(f"[鉴权] 异常: {e}")
            return False

    # ── 连接管理 ──
    async def connect(self) -> bool:
        """连接并启动后台任务，支持自动重连

        v4.0: 使用 websockets 内置 ping/pong 保活，并在创建新任务前清理旧任务，
        避免重连后残留多个心跳/接收循环导致的反复断连。
        """
        if not self.token and not self.sign_token():
            return False
        try:
            # 清理可能残留的旧任务（重连场景下由 disconnect 处理，这里做兜底）
            await self._cancel_bg_tasks()
            self.ws = await websockets.connect(
                WS_URL,
                ping_interval=None,    # 关闭 websocket 内置 ping，使用应用层 CMD_PING 心跳
                ping_timeout=None,
                close_timeout=5,
                max_queue=2 ** 20,
            )
            await self.ws.send(self._build_auth_bind_msg())
            await self.ws.recv()
            self.connected = True
            self.reconnect_attempts = 0
            self.heartbeat_task = asyncio.create_task(self._heartbeat())
            self.receive_task = asyncio.create_task(self._receive_loop())
            print(f"[连接] ✅ 已连接 (heartbeat={self.heartbeat_interval}s)")
            # ── 插件 on_connect 事件（v4.4）──
            if getattr(self, 'plugin_manager', None) is not None:
                self.plugin_manager.fire_event('connect')
            return True
        except Exception as e:
            print(f"[连接] 失败: {e}")
            return False

    async def _cancel_bg_tasks(self):
        """取消并等待后台心跳/接收任务结束（不关闭 ws）"""
        for task in (self.heartbeat_task, self.receive_task):
            if task is not None and not task.done():
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
        self.heartbeat_task = None
        self.receive_task = None

    async def disconnect(self, manual=False):
        """优雅断开连接

        manual=True 表示用户主动断开：关闭自动重连。
        重连过程中的内部清理调用 manual=False，保留 _should_reconnect 以便继续重连。
        """
        self.connected = False
        # ── 插件 on_disconnect 事件（v4.4）──
        if getattr(self, 'plugin_manager', None) is not None:
            self.plugin_manager.fire_event('disconnect')
        if manual:
            self._should_reconnect = False
        for task in (self.heartbeat_task, self.receive_task, self._proxy_worker_task):
            if task and not task.done():
                task.cancel()
        self.heartbeat_task = self.receive_task = self._proxy_worker_task = None
        self._proxy_worker_running = False
        for item in self._proxy_queue:
            if not item["future"].done():
                item["future"].cancel()
        self._proxy_queue.clear()
        self._seen_proxy_msg_ids.clear()
        if self._pending_image_future and not self._pending_image_future.done():
            self._pending_image_future.cancel()
            self._pending_image_future = None
        if self._ai_image_request:
            if not self._ai_image_request["future"].done():
                self._ai_image_request["future"].cancel()
            self._ai_image_request = None
        if self.ws:
            try:
                await self.ws.close()
            except Exception:
                pass
            self.ws = None

    # ── 自动重连（v4.0 重构）──
    def _schedule_reconnect(self):
        """安排在事件循环中执行一次重连（幂等，避免并发重连任务）"""
        if not self._should_reconnect:
            return
        if self._reconnect_in_progress:
            return
        self._reconnect_in_progress = True
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.ensure_future(self._auto_reconnect())
            else:
                asyncio.run_coroutine_threadsafe(self._auto_reconnect(), loop)
        except RuntimeError:
            self._reconnect_in_progress = False

    async def _auto_reconnect(self):
        """自动重连逻辑：指数退避 + 随机抖动 + token 刷新 + 彻底清理旧连接

        v4.0 修复：
        - 重连前先彻底 disconnect（关闭旧 ws、取消旧任务、清空代理队列 future）
        - 每次重连刷新 token，避免 token 过期导致连上即断
        - 单个重连协程串行退避，避免递归 + 并发任务雪崩
        """
        attempt = 0
        try:
            while self._should_reconnect and attempt < self.max_reconnect_attempts:
                delay = min(2 ** attempt, 60) + random.uniform(0, 1)
                print(f"[重连] 第 {attempt + 1}/{self.max_reconnect_attempts} 次，等待 {delay:.1f}s...")
                await asyncio.sleep(delay)
                if not self._should_reconnect:
                    break
                try:
                    # 彻底清理旧连接/旧任务
                    await self.disconnect()
                    # 刷新 token，避免过期
                    if not self.sign_token():
                        print("[重连] token 刷新失败，稍后重试")
                        attempt += 1
                        continue
                    if await self.connect():
                        print("[重连] ✅ 重连成功")
                        return
                except Exception as e:
                    print(f"[重连] 异常: {e}")
                attempt += 1
            if attempt >= self.max_reconnect_attempts:
                print(f"[重连] 已达最大重试次数 {self.max_reconnect_attempts}，停止重连")
        finally:
            self._reconnect_in_progress = False

    # ── 鉴权消息构建 ──
    def _build_auth_bind_msg(self) -> bytes:
        auth_info = (self.codec.encode_string(1, self.bot_id) +
                     self.codec.encode_string(2, "web") +
                     self.codec.encode_string(3, self.token))
        device_info = (self.codec.encode_string(1, "2.0.1") +
                       self.codec.encode_string(2, "Linux") +
                       self.codec.encode_string(3, "2026.3.23-2") +
                       self.codec.encode_string(4, "16"))
        auth_data = (self.codec.encode_string(1, "ybBot") +
                     self.codec.encode_message_field(2, auth_info) +
                     self.codec.encode_message_field(3, device_info))
        head = HeadBuilder.request(CMD_AUTH_BIND, self.seq_no, self._generate_msg_id(), MODULE_CONN_ACCESS)
        self.seq_no += 1
        return self.codec.encode_conn_msg(head, auth_data)

    # ── 心跳 ──
    async def _heartbeat(self):
        print(f"[心跳] 启动 (间隔 {self.heartbeat_interval}s)")
        while self.connected:
            await asyncio.sleep(self.heartbeat_interval)
            if not self.connected:
                break
            # 重试 3 次，间隔 1s，避免网络毛刺误触发断连
            ok = False
            for attempt in range(4):
                if not self.connected:
                    return
                try:
                    head = HeadBuilder.request(CMD_PING, self.seq_no, self._generate_msg_id(), MODULE_CONN_ACCESS)
                    self.seq_no += 1
                    await self.ws.send(self.codec.encode_conn_msg(head))
                    ok = True
                    break
                except Exception as e:
                    if attempt < 3:
                        print(f"[心跳] 发送失败 (第{attempt+1}次): {e}，1s 后重试")
                        await asyncio.sleep(1)
                    else:
                        print(f"[心跳] 连续 4 次发送失败，判定连接已断开: {e}")
                        self.connected = False
            if not ok:
                # 心跳失败：主动触发重连（与接收循环去重，避免并发）
                self._schedule_reconnect()
                return

    # ── 自动回复 ──
    def get_auto_reply(self, text: str, is_group: bool = False, group_code: str = "") -> str:
        """根据规则匹配自动回复文本（按优先级降序匹配，已禁用的规则跳过）"""
        text = text.strip()
        if not text:
            return self.DEFAULT_REPLY or (self.AUTO_REPLY_GROUP_TEXT if is_group else self.AUTO_REPLY_C2C_TEXT)

        rules = [r for r in self.AUTO_REPLY_RULES if r.get('enabled', True)]
        rules.sort(key=lambda r: r.get('priority', 999))
        for rule in rules:
            if rule.get('group_only') and not is_group:
                continue
            match_type = rule.get('match_type', '')
            pattern = rule.get('pattern', '')
            reply = rule.get('reply_text', '')

            if match_type == 'exact' and text == pattern:
                return reply
            elif match_type == 'contains' and pattern and pattern in text:
                return reply
            elif match_type == 'contains_any':
                for p in rule.get('patterns', []):
                    if p and p in text:
                        return reply
            elif match_type == 'startswith' and pattern and text.startswith(pattern):
                return reply
            elif match_type == 'endswith' and pattern and text.endswith(pattern):
                return reply
            elif match_type == 'regex' and pattern:
                try:
                    if re.search(pattern, text):
                        return reply
                except Exception:
                    pass

        return self.DEFAULT_REPLY or (self.AUTO_REPLY_GROUP_TEXT if is_group else self.AUTO_REPLY_C2C_TEXT)

    async def _process_auto_reply(self, push_json: dict):
        """处理收到的消息，判断是否触发自动回复"""
        if not self.auto_reply_enabled or not self.bot_id:
            return
        try:
            from_account = push_json.get("from_account", "")
            if from_account == self.bot_id:
                return

            text_parts = []
            is_at_me = False
            for elem in push_json.get("msg_body", []):
                etype = elem.get("msg_type", "")
                content = elem.get("msg_content", {})
                if etype == "TIMTextElem":
                    t = content.get("text", "")
                    if t:
                        text_parts.append(t)
                    if f"@{self.bot_id}" in t:
                        is_at_me = True
                elif etype == "TIMCustomElem":
                    try:
                        cd = json.loads(content.get("data", "{}"))
                        if cd.get("elem_type") == 1002:
                            t = cd.get("text", "")
                            if t:
                                text_parts.append(t)
                            if cd.get("user_id") == self.bot_id or f"@{self.bot_id}" in t:
                                is_at_me = True
                    except Exception:
                        pass

            text = " ".join(text_parts).strip()
            if is_at_me:
                for prefix in (f"@{self.bot_id}", "@"):
                    if text.startswith(prefix):
                        text = text[len(prefix):].lstrip()
                        break

            group_code = push_json.get("group_code", "")
            callback_cmd = push_json.get("callback_command", "")
            is_group = bool(group_code and "Group" in callback_cmd)

            need_reply = (is_group and is_at_me) or (not is_group and bool(from_account))

            if need_reply:
                reply = self.get_auto_reply(text, is_group, group_code)
                if reply:
                    if is_group:
                        orig = self.group_code
                        self.group_code = group_code
                        try:
                            await self.send_group_message(reply)
                        finally:
                            self.group_code = orig
                    else:
                        await self.send_c2c_message(from_account, reply)
        except Exception as e:
            print(f"[自动回复] 异常: {e}")

    # ── URL 解析 ──
    def resolve_image_url(self, resource_url: str) -> Optional[str]:
        """将元宝图片资源保护 URL 转换为 COS 预签名直链

        从 URL 中提取 resourceId，调用 /api/resource/v1/download 接口，
        获取可直接下载的 COS 预签名 URL。（与 sender.py 一致）
        """
        match = re.search(r'resourceId=([^&]+)', resource_url)
        if not match:
            # 如果已经是可以直接下载的 URL，直接返回
            if resource_url.startswith("http") and "resourceId" not in resource_url:
                return resource_url
            print(f"[图片] 无法从 URL 中提取 resourceId: {resource_url[:80]}")
            return None

        resource_id = match.group(1)
        url = f"https://{API_DOMAIN}/api/resource/v1/download?resourceId={resource_id}"
        headers = {
            "X-ID": self.bot_id or "",
            "X-Token": self.token or "",
            "X-Source": "web",
            "X-AppVersion": "1.0.11",
            "X-OperationSystem": "linux",
            "X-Instance-Id": self.instance_id or "",
            "X-Bot-Version": "2026.3.22",
        }

        try:
            resp = requests.get(url, headers=headers, timeout=30)
            result = resp.json()
            cos_url = result.get("realUrl") or result.get("url") or ""
            if cos_url and cos_url.startswith("http"):
                return cos_url
            if result.get("code", 0) == 0:
                data = result.get("data", {})
                cos_url = data.get("url") or data.get("realUrl") or ""
                if cos_url:
                    return cos_url
            print(f"[图片] 获取图片直链失败: {result}")
            return None
        except Exception as e:
            print(f"[图片] 获取图片直链错误: {e}")
            return None

    # ── 代理模式工作循环 ──
    async def _proxy_worker_loop(self):
        """后台任务：从队列取消息 → 转发到中转群

        v3.0 双模式：
        - forward_at_yuanbao=True  → @元宝模式：转发后等元宝回复，再回原群
        - forward_at_yuanbao=False → 直转模式：转发完立即完成，不等任何人回复
        """
        type_names = {"sticker": "贴纸", "image": "图片", "file": "文件"}
        self._proxy_worker_running = True
        mode = "yuanbao" if self.forward_at_yuanbao else "direct"
        print(f"[Auto-Proxy] 🔄 worker 启动 | 模式={mode}")
        try:
            while self._proxy_queue:
                item = self._proxy_queue[0]
                media = item.get("media_info", {})
                media_type = media.get("type", "")
                sender_name = item.get('ref_sender_name', '未知用户')

                item["forwarded"] = False
                item["reply_received"] = False

                # ── Step 1: 转发到中转群 ──
                ok = await self._forward_single_item(item, media, media_type, sender_name, type_names)

                if ok:
                    item["forwarded"] = True
                    content_preview = (item.get('original_content', '') or '')[:40]
                    print(f"[Auto-Proxy] 📤 已转发到中转群 | {sender_name}: {content_preview}")

                    # ── Step 2: 仅 @元宝模式才通知元宝 + 等回复 ──
                    if self.forward_at_yuanbao:
                        if media_type in ("sticker", "image", "file"):
                            try:
                                await self.send_group_message(
                                    f"@{YUANBAO_NICKNAME} 收到一条{type_names[media_type]}消息",
                                    at_user=YUANBAO_BOT_ID, at_nickname=YUANBAO_NICKNAME,
                                    target_group=IMAGE_GROUP_CODE
                                )
                            except Exception as e:
                                print(f"[Auto-Proxy] ⚠️ @元宝通知发送失败: {e}")

                        try:
                            await asyncio.wait_for(item["future"], timeout=120.0)
                            print(f"[Auto-Proxy] ✅ 元宝回复已转发到原群")
                        except asyncio.TimeoutError:
                            print(f"[Auto-Proxy] ⏰ 等待元宝回复超时(120s)，继续下一条")
                        except asyncio.CancelledError:
                            print(f"[Auto-Proxy] ⚠️ 等待被取消")
                        except Exception as e:
                            print(f"[Auto-Proxy] ⚠️ 等待异常: {e}")
                    else:
                        if not item["future"].done():
                            item["future"].set_result(True)
                        print(f"[Auto-Proxy] ⚡ 直转完成（无需等回复）")
                else:
                    print(f"[Auto-Proxy] ❌ 转发失败 | {sender_name}")
                    if not item["future"].done():
                        item["future"].cancel()

                if self._proxy_queue and self._proxy_queue[0] is item:
                    self._proxy_queue.popleft()

        except asyncio.CancelledError:
            print("[Auto-Proxy] worker 被取消")
        except Exception as e:
            print(f"[Auto-Proxy] ❌ worker 异常退出: {e}")
            self._proxy_queue.clear()
        finally:
            self._proxy_worker_running = False
            self._proxy_worker_task = None
            print(f"[Auto-Proxy] worker 已停止")

    async def _forward_single_item(self, item, media, media_type, sender_name, type_names) -> bool:
        """转发单条消息到中转群，返回是否成功"""
        try:
            if media_type == "sticker":
                return await self._forward_sticker(item, media, sender_name)
            elif media_type == "image":
                return await self._forward_image(item, media, sender_name)
            elif media_type == "file":
                return await self._forward_file(item, media, sender_name)
            else:
                return await self._forward_text(item, sender_name)
        except Exception as e:
            print(f"[Auto-Proxy] 转发异常: {e}")
            return False

    async def _forward_sticker(self, item, media, sender_name) -> bool:
        sticker_name = media.get("sticker_name", "")
        sticker_id = media.get("sticker_id", "")
        package_id = media.get("package_id", "")
        if sticker_name in self.STICKERS:
            return await self.send_sticker(sticker_name, text=f"来自{sender_name}的消息", target_group=IMAGE_GROUP_CODE)
        elif sticker_id:
            msg = self._build_raw_sticker_msg(sticker_name or "贴纸", sticker_id, package_id, group_code=IMAGE_GROUP_CODE)
            await self.ws.send(msg)
            return True
        else:
            return await self.send_group_message(f"来自{sender_name}的消息: [贴纸]", target_group=IMAGE_GROUP_CODE)

    async def _forward_image(self, item, media, sender_name) -> bool:
        """转发图片：直接使用原始 URL 构建 TIMImageElem 发送

        v3.1 重写：参考 sender.py 的代理模式图片转发逻辑，
        直接使用收到的 image_urls 构建消息，不再下载+COS上传。
        同时传递正确的 uuid/size/width/height 信息。
        """
        urls = media.get("image_urls", [])
        if not urls:
            print(f"[Auto-Proxy] ⚠️ 图片无可用 URL，降级为文本转发")
            return await self.send_group_message(f"来自{sender_name}的消息: [图片]", target_group=IMAGE_GROUP_CODE)

        uuid_val = media.get("image_uuid", "")
        width = media.get("image_width", 0)
        height = media.get("image_height", 0)
        size = media.get("image_size", 0)

        images = []
        for url in urls:
            resolved_url = self.resolve_image_url(url) if "resourceId" in url else url
            if resolved_url:
                images.append((resolved_url, uuid_val, size, width, height))
            else:
                images.append((url, uuid_val, size, width, height))

        if not images:
            print(f"[Auto-Proxy] ⚠️ 图片 URL 解析全部失败，降级为文本转发")
            return await self.send_group_message(f"来自{sender_name}的消息: [图片]", target_group=IMAGE_GROUP_CODE)

        try:
            msg = self._build_image_msg(images, group_code=IMAGE_GROUP_CODE)
            await self.ws.send(msg)
            print(f"[Auto-Proxy] 🖼️ 图片已转发 | {sender_name} | {len(images)}张 | {size/1024:.1f}KB")
            return True
        except Exception as e:
            print(f"[Auto-Proxy] ⚠️ 图片发送失败: {e}")
            return await self.send_group_message(f"来自{sender_name}的消息: [图片]", target_group=IMAGE_GROUP_CODE)

    async def _forward_file(self, item, media, sender_name) -> bool:
        """转发文件：直接使用原始 URL 构建 TIMFileElem 发送"""
        url = media.get("file_url", "")
        if not url:
            print(f"[Auto-Proxy] ⚠️ 文件无可用 URL，降级为文本转发")
            return await self.send_group_message(f"来自{sender_name}的消息: [文件]", target_group=IMAGE_GROUP_CODE)

        uuid_val = media.get("file_uuid", "") or uuid.uuid4().hex
        fname = media.get("file_name", "") or "file.bin"
        fsize = media.get("file_size", 0)

        try:
            msg = self._build_file_msg(url, file_id=uuid_val, file_size=fsize,
                                       file_name=fname, group_code=IMAGE_GROUP_CODE)
            await self.ws.send(msg)
            print(f"[Auto-Proxy] 📎 文件已转发 | {sender_name} | {fname} ({fsize/1024:.1f}KB)")
            return True
        except Exception as e:
            print(f"[Auto-Proxy] ⚠️ 文件发送失败: {e}")
            return await self.send_group_message(f"来自{sender_name}的消息: [文件]", target_group=IMAGE_GROUP_CODE)

    async def _forward_text(self, item, sender_name) -> bool:
        """转发文本消息：始终 @元宝（如果开启），让元宝必然看到并回复"""
        content = item['original_content']
        if self.forward_at_yuanbao:
            return await self.send_group_message(
                f"来自{sender_name}的消息: {content}",
                at_user=YUANBAO_BOT_ID, at_nickname=YUANBAO_NICKNAME,
                target_group=IMAGE_GROUP_CODE
            )
        else:
            return await self.send_group_message(
                f"来自{sender_name}的消息: {content}",
                target_group=IMAGE_GROUP_CODE
            )

    # ── 消息接收循环 ──
    async def _receive_loop(self):
        print("[接收] 启动消息接收循环")
        try:
            while self.connected and self.ws:
                raw = await self.ws.recv()
                if not isinstance(raw, bytes):
                    continue
                conn_msg = self.codec.decode_conn_msg(raw)
                if not conn_msg:
                    continue
                head = conn_msg.get("head", {})
                cmd_type = head.get("cmd_type")
                cmd = head.get("cmd", "")
                msg_id = head.get("msg_id")

                if cmd_type == CMD_TYPE_PUSH and cmd == "inbound_message":
                    await self._handle_push_message(conn_msg)
                elif cmd_type == CMD_TYPE_RESPONSE:
                    self._handle_response(msg_id, conn_msg)
        except Exception as e:
            print(f"[接收] 异常: {e}")
        finally:
            if self.connected:
                print("[接收] 连接断开，尝试重连...")
                self.connected = False
                self._schedule_reconnect()

    async def _handle_push_message(self, conn_msg):
        """处理服务端推送的消息"""
        try:
            push_json = json.loads(conn_msg.get("data", b""))
        except Exception as e:
            print(f"[推送] JSON解析失败: {e}")
            return

        # 撤回事件检测——在消息解析之前拦截
        callback_command = push_json.get("callback_command", "")
        if callback_command == "Group.CallbackAfterRecallMsg":
            if self.recall_monitor_enabled:
                await self._handle_recall_notification(push_json)
            return

        text_content, media_info = self._extract_message_content(push_json)
        # 将图片 resource URL 解析为直链，否则浏览器无法加载
        if media_info and media_info.get("type") == "image":
            urls = media_info.get("image_urls", [])
            resolved = []
            for url in urls:
                if "resourceId" in url:
                    r = self.resolve_image_url(url)
                    if r:
                        resolved.append(r)
                else:
                    resolved.append(url)
            if resolved:
                media_info["image_urls"] = resolved
        group_code = push_json.get("group_code", "")
        from_account = push_json.get("from_account", "")

        cache_entry = {
            "time": datetime.now().strftime("%H:%M:%S"),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "sender_id": from_account,
            "sender_name": push_json.get("sender_nickname", ""),
            "group_code": group_code,
            "content": text_content,
            "msg_type": push_json.get("callback_command", ""),
            "msg_id": push_json.get("msg_id", ""),
            "msg_seq": push_json.get("msg_seq", 0),
            "media_info": media_info,
        }

        # ── 解析引用消息（cloud_custom_data）──
        cloud_custom_data = push_json.get("cloud_custom_data", "")
        if cloud_custom_data:
            try:
                ccd = json.loads(cloud_custom_data)
                quote = ccd.get("quote")
                if quote and isinstance(quote, dict):
                    desc = quote.get("desc", "").strip()
                    if quote.get("type") == 2:  # MT_PIC
                        desc = desc or "[图片]"
                    if desc:
                        cache_entry["quote"] = {
                            "sender_id": quote.get("sender_id", ""),
                            "sender_nickname": quote.get("sender_nickname", ""),
                            "desc": desc,
                        }
            except Exception:
                pass

        self.msg_cache.append(cache_entry)
        if len(self.msg_cache) > 500:
            self.msg_cache = self.msg_cache[-500:]

        # ── 实时写入本地文件（受开关控制）──
        if msg_logger is not None and msg_logger.enabled:
            msg_logger.log({
                "timestamp": cache_entry["timestamp"],
                "sender_id": from_account,
                "sender_name": cache_entry["sender_name"],
                "group_code": group_code,
                "content": text_content,
                "msg_type": cache_entry["msg_type"],
                "msg_id": cache_entry["msg_id"],
                "media_info": media_info,
            })

        self._update_group_info(group_code, text_content)
        await self._process_auto_reply(push_json)
        self._maybe_enqueue_proxy(cache_entry, push_json, text_content, media_info, from_account)

        # ── 插件 on_message 钩子（v4.4）──
        if getattr(self, 'plugin_manager', None) is not None:
            self.plugin_manager.dispatch_message(cache_entry)

        # 检测元宝图片回复（转发代理）
        self._maybe_handle_yuanbao_image_reply(push_json, text_content)
        # 检测元宝文本回复
        self._maybe_handle_yuanbao_reply(push_json, text_content)
        # 检测 /ai-image 流程的元宝图片回复（不依赖代理模式）
        self._maybe_handle_ai_image_reply(push_json, text_content)

    async def _handle_recall_notification(self, push_json: dict):
        """处理撤回事件

        逻辑：
        1. 总是标记原消息为已撤回 + 记录到 recall_cache
        2. 仅在 recall_notify_enabled 开启时发送撤回通知
        3. 通知目标由 recall_notify_target 决定：original → 原群 / relay → 中转群
        """
        group_code = push_json.get("group_code", "")
        recaller_id = push_json.get("from_account", "")
        recaller_name = push_json.get("sender_nickname", recaller_id)
        recall_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        recall_time_short = datetime.now().strftime("%H:%M:%S")

        recall_list = push_json.get("recall_msg_seq_list", [])
        if not recall_list:
            print(f"[撤回] 无撤回消息列表")
            return

        # 确定通知目标群
        notify_group = group_code
        if self.recall_notify_target == "relay":
            notify_group = IMAGE_GROUP_CODE or group_code

        for item in recall_list:
            recalled_msg_id = item.get("msg_id", "")
            recalled_msg_seq = item.get("msg_seq", 0)

            # 查找原消息：优先 msg_id，回退 msg_seq
            original_entry = None
            if recalled_msg_id:
                for entry in reversed(self.msg_cache):
                    if entry.get("msg_id") == recalled_msg_id:
                        original_entry = entry
                        break

            if not original_entry and recalled_msg_seq:
                seq_key = str(recalled_msg_seq)
                for entry in reversed(self.msg_cache):
                    cached_seq = entry.get("msg_seq")
                    if cached_seq and str(cached_seq) == seq_key:
                        original_entry = entry
                        break

            if original_entry:
                # ── 总是执行：标记原消息为已撤回 ──
                original_entry["recalled"] = True
                original_entry["recaller_name"] = recaller_name
                original_entry["recaller_id"] = recaller_id
                original_entry["recall_time"] = recall_time_short

                # ── 总是执行：记录到撤回缓存 ──
                recall_entry = {
                    "time": recall_time_short,
                    "timestamp": recall_time,
                    "orig_sender_name": original_entry.get("sender_name", "未知"),
                    "orig_sender_id": original_entry.get("sender_id", ""),
                    "orig_time": original_entry.get("time", ""),
                    "orig_content": original_entry.get("content", ""),
                    "orig_media_info": original_entry.get("media_info", {}),
                    "orig_quote": original_entry.get("quote"),
                    "recaller_name": recaller_name,
                    "recaller_id": recaller_id,
                    "group_code": group_code,
                    "msg_id": recalled_msg_id,
                    "msg_seq": recalled_msg_seq,
                }
                self.recall_cache.append(recall_entry)
                if len(self.recall_cache) > 200:
                    self.recall_cache = self.recall_cache[-200:]
                print(f"[撤回] 已记录撤回: {recaller_name} 撤回了消息")

                # ── 条件执行：仅在通知开关开启时发送通知 ──
                if not self.recall_notify_enabled:
                    continue

                orig_sender = original_entry.get("sender_name", "未知")
                orig_time = original_entry.get("time", "")
                orig_content = original_entry.get("content", "")

                # 时间戳转换
                if orig_time and orig_time.isdigit():
                    try:
                        ts = int(orig_time)
                        if ts > 1e12:
                            ts //= 1000
                        orig_time = datetime.fromtimestamp(ts).strftime("%H:%M:%S")
                    except (OSError, ValueError):
                        pass
                elif not orig_time:
                    orig_time = ""

                # 获取群名
                group_name = group_code
                if group_code in self.groups:
                    gn = self.groups[group_code].get("group_name", "")
                    if gn and gn != group_code:
                        group_name = f"{gn}({group_code})"

                notif = f"—— 撤回通知 ——\n群: {group_name}\n原发送者: {orig_sender}"
                if orig_time:
                    notif += f"\n时间: {orig_time}"

                # 尝试重发媒体（直接 protobuf 转发，不下载重上传）
                media_info = original_entry.get("media_info")
                media_sent = False

                if media_info:
                    mt = media_info.get("type", "")

                    if mt == "image":
                        urls = media_info.get("image_urls", [])
                        if urls:
                            try:
                                uuid_val = media_info.get("image_uuid", "")
                                width = media_info.get("image_width", 0)
                                height = media_info.get("image_height", 0)
                                size = media_info.get("image_size", 0)
                                msg = self._build_image_msg(
                                    [(urls[0], uuid_val, size, width, height)],
                                    group_code=notify_group,
                                )
                                await self.ws.send(msg)
                                await self.send_group_message(notif, target_group=notify_group)
                                media_sent = True
                                print(f"[撤回] 已重发图片通知到群 {notify_group}")
                            except Exception as e:
                                print(f"[撤回] 重发图片失败: {e}")

                    elif mt == "file":
                        file_url = media_info.get("file_url", "")
                        if file_url:
                            try:
                                msg = self._build_file_msg(
                                    file_url,
                                    file_id=media_info.get("file_uuid", ""),
                                    file_size=media_info.get("file_size", 0),
                                    file_name=media_info.get("file_name", ""),
                                    group_code=notify_group,
                                )
                                await self.ws.send(msg)
                                await self.send_group_message(notif, target_group=notify_group)
                                media_sent = True
                                print(f"[撤回] 已重发文件通知到群 {notify_group}")
                            except Exception as e:
                                print(f"[撤回] 重发文件失败: {e}")

                    elif mt == "sticker":
                        sticker_id = media_info.get("sticker_id", "")
                        package_id = media_info.get("package_id", "")
                        sticker_name = media_info.get("sticker_name", "")
                        if sticker_id and package_id:
                            try:
                                msg = self._build_raw_sticker_msg(
                                    sticker_name or "贴纸", sticker_id, package_id,
                                    group_code=notify_group,
                                )
                                await self.ws.send(msg)
                                await self.send_group_message(notif, target_group=notify_group)
                                media_sent = True
                                print(f"[撤回] 已重发贴纸通知到群 {notify_group}")
                            except Exception as e:
                                print(f"[撤回] 重发贴纸失败: {e}")

                if not media_sent and orig_content:
                    # 检测 \scalebox{数字} 参数是否 > 10，是则用代码块防止巨图刷屏
                    scalebox_match = re.search(r'\\scalebox\{(\d+(?:\.?\d+)?)\}', orig_content)
                    if scalebox_match and float(scalebox_match.group(1)) > 10:
                        max_backticks = 0
                        bcount = 0
                        for ch in orig_content:
                            if ch == '`':
                                bcount += 1
                                max_backticks = max(max_backticks, bcount)
                            else:
                                bcount = 0
                        fence = '`' * (max_backticks + 1) if max_backticks >= 3 else '```'
                        full_notif = f"{notif}\n原内容:\n{fence}\n{orig_content}\n（scale={scalebox_match.group(1)}>10，已用代码块包裹）\n{fence}"
                    else:
                        full_notif = f"{notif}\n原内容: {orig_content}"
                    await self.send_group_message(full_notif, target_group=notify_group)

                elif not media_sent and not orig_content:
                    await self.send_group_message(notif, target_group=notify_group)

    def _extract_message_content(self, push_json) -> Tuple[str, dict]:
        """从推送 JSON 中提取纯文本和媒体信息"""
        text_parts = []
        media_info: dict = {}
        image_urls = []
        image_uuid = ""
        image_width = 0
        image_height = 0
        image_size = 0

        for elem in push_json.get("msg_body", []):
            etype = elem.get("msg_type", "")
            content = elem.get("msg_content", {})
            if etype == "TIMTextElem":
                text_parts.append(content.get("text", ""))
            elif etype == "TIMCustomElem":
                try:
                    cd = json.loads(content.get("data", "{}"))
                    if cd.get("elem_type") == 1002:
                        text_parts.append(cd.get("text", ""))
                except Exception:
                    pass
            elif etype == "TIMFaceElem":
                face_str = content.get("data", "")
                try:
                    face = json.loads(face_str) if isinstance(face_str, str) else json.loads(face_str.decode())
                    name = face.get("name", "")
                    text_parts.append(f"[贴纸: {name}]" if name else "[贴纸]")
                    media_info = {"type": "sticker", "sticker_id": face.get("sticker_id",""),
                                  "sticker_name": name, "package_id": face.get("package_id","")}
                except Exception:
                    text_parts.append("[贴纸]")
                    media_info = {"type": "sticker"}
            elif etype == "TIMImageElem":
                img_array = content.get("image_info_array", [])
                last_info = {}
                last_url = None
                for info in img_array:
                    if isinstance(info, dict) and info.get("url"):
                        last_info = info
                        last_url = info["url"]
                if last_url:
                    image_urls.append(last_url)
                image_urls = list(dict.fromkeys(image_urls))

                text_parts.append("[图片]" if not media_info else "")
                image_uuid = content.get("uuid", "")
                image_width = last_info.get("width", 0)
                image_height = last_info.get("height", 0)
                image_size = last_info.get("size", 0)
                media_info = {"type": "image", "image_urls": image_urls,
                              "image_uuid": image_uuid,
                              "image_width": image_width,
                              "image_height": image_height,
                              "image_size": image_size}
            elif etype == "TIMFileElem":
                text_parts.append(f"[文件: {content.get('fileName','')}]")
                media_info = {"type": "file", "file_name": content.get("fileName",""),
                              "file_url": content.get("url",""),
                              "file_uuid": content.get("uuid",""),
                              "file_size": content.get("fileSize",0)}
        return " ".join(text_parts).strip(), media_info

    def _update_group_info(self, group_code, text):
        if not group_code:
            return
        short = text[:50] + "..." if len(text) > 50 else text
        if group_code not in self.groups:
            self.groups[group_code] = {"group_code": group_code, "group_name": group_code,
                                        "last_message": short,
                                        "last_message_time": datetime.now().strftime("%H:%M:%S"),
                                        "message_count": 0}
        g = self.groups[group_code]
        g["last_message"] = short
        g["last_message_time"] = datetime.now().strftime("%H:%M:%S")
        g["message_count"] = g.get("message_count", 0) + 1

    def _maybe_enqueue_proxy(self, cache_entry, push_json, text_content, media_info, from_account):
        """判断是否将消息加入代理转发队列"""
        if (self.auto_reply_text != "yb" or not cache_entry.get("group_code") or
                cache_entry["group_code"] == IMAGE_GROUP_CODE or from_account == self.bot_id or
                not IMAGE_GROUP_CODE):
            return
        if self.auto_reply_at_only:
            if not self._is_at_bot(push_json):
                return

        text = text_content.strip()
        media_type = (media_info or {}).get("type", "")
        if not text and not media_type:
            return

        raw_msg_id = cache_entry.get("msg_id", "")
        if raw_msg_id:
            if raw_msg_id in self._seen_proxy_msg_ids:
                print(f"[Auto-Proxy] ⚠️ 跳过重复消息 {raw_msg_id[:16]}...")
                return
            if len(self._seen_proxy_msg_ids) > 2000:
                self._seen_proxy_msg_ids = set(list(self._seen_proxy_msg_ids)[-1000:])
            self._seen_proxy_msg_ids.add(raw_msg_id)

        if not text and media_type:
            type_label = {"sticker": "[贴纸]", "image": "[图片]", "file": "[文件]"}
            text = type_label.get(media_type, "[消息]")

        # v3.5: 优先使用运行中的事件循环，回退到默认循环（避免 DeprecationWarning）
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.get_event_loop_policy().get_event_loop()

        item = {
            "future": loop.create_future(),
            "target_group": cache_entry["group_code"],
            "ref_msg_id": cache_entry.get("msg_id", ""),
            "ref_sender_name": cache_entry.get("sender_name", "未知用户"),
            "original_content": text,
            "media_info": media_info,
            "forwarded": False,
            "reply_received": False,
            "enqueue_time": time.time(),
        }
        self._proxy_queue.append(item)

        worker_alive = (self._proxy_worker_task is not None and
                        not self._proxy_worker_task.done() and
                        getattr(self, '_proxy_worker_running', False))
        if not worker_alive:
            self._proxy_worker_task = asyncio.create_task(self._proxy_worker_loop())
            print(f"[Auto-Proxy] 🔄 worker 已启动 (队列长度: {len(self._proxy_queue)})")

    def _is_at_bot(self, push_json) -> bool:
        for elem in push_json.get("msg_body", []):
            if elem.get("msg_type") != "TIMCustomElem":
                continue
            try:
                cd = json.loads(elem.get("msg_content", {}).get("data", "{}"))
                if cd.get("elem_type") == 1002 and cd.get("user_id") == self.bot_id:
                    return True
            except Exception:
                pass
        return False

    def _maybe_handle_yuanbao_image_reply(self, push_json, text_content):
        """检测元宝在 IMAGE_GROUP 中回复的图片消息"""
        if not self.forward_at_yuanbao:
            return
        if not self._proxy_queue:
            return
        if push_json.get("group_code") != IMAGE_GROUP_CODE:
            return
        if push_json.get("from_account") != YUANBAO_BOT_ID:
            return

        msg_body = push_json.get("msg_body", [])
        img_urls = []
        for elem in msg_body:
            if elem.get("msg_type") == "TIMImageElem":
                mc = elem.get("msg_content", {})
                img_array = mc.get("image_info_array", [])
                last_url = None
                for info in img_array:
                    if isinstance(info, dict) and info.get("url"):
                        last_url = info["url"]
                if last_url:
                    img_urls.append(last_url)

        text_urls = re.findall(r'!\[image\]\(([^)]+)\)', text_content)
        all_urls = list(dict.fromkeys(img_urls + text_urls))

        if not all_urls:
            return

        target_item = None
        for item in self._proxy_queue:
            if item.get("forwarded") and not item.get("reply_received"):
                target_item = item
                break

        if target_item is None:
            return

        print(f"[Auto-Proxy] 🖼️ 检测到元宝图片回复，共 {len(all_urls)} 张，开始解析直链...")

        resolved_urls = []
        for u in all_urls:
            resolved = self.resolve_image_url(u) if "resourceId" in u else u
            if resolved:
                resolved_urls.append(resolved)

        if not resolved_urls:
            print(f"[Auto-Proxy] ⚠️ 图片 URL 解析失败")
            return

        target_item["reply_received"] = True
        asyncio.create_task(self._download_and_send_images(resolved_urls, target_item))

    def _maybe_handle_ai_image_reply(self, push_json, text_content):
        """检测 /ai-image 流程的元宝图片回复（不依赖代理模式）"""
        if not self._ai_image_request:
            return
        if self._ai_image_request["future"].done():
            return
        if push_json.get("group_code") != IMAGE_GROUP_CODE:
            return
        if push_json.get("from_account") != YUANBAO_BOT_ID:
            return

        msg_body = push_json.get("msg_body", [])
        img_urls = []
        for elem in msg_body:
            if elem.get("msg_type") == "TIMImageElem":
                mc = elem.get("msg_content", {})
                img_array = mc.get("image_info_array", [])
                last_url = None
                for info in img_array:
                    if isinstance(info, dict) and info.get("url"):
                        last_url = info["url"]
                if last_url:
                    img_urls.append(last_url)
        text_urls = re.findall(r'!\[image\]\(([^)]+)\)', text_content)
        all_urls = list(dict.fromkeys(img_urls + text_urls))
        if not all_urls:
            return

        print(f"[AI-Image] 🖼️ 检测到元宝图片回复，{len(all_urls)} 张，开始解析直链...")

        resolved_urls = []
        for u in all_urls:
            resolved = self.resolve_image_url(u) if "resourceId" in u else u
            if resolved:
                resolved_urls.append(resolved)
        if not resolved_urls:
            print(f"[AI-Image] ⚠️ 图片 URL 解析全部失败")
            return

        target_group = self._ai_image_request["target_group"]
        future = self._ai_image_request["future"]
        self._ai_image_request = None  # 立即清除，防止重复处理

        asyncio.create_task(self._ai_image_download_and_send(resolved_urls, target_group, future))

    async def _ai_image_download_and_send(self, urls: list, target_group: str, future: asyncio.Future):
        """下载 AI 图片并发送到指定群聊"""
        import shutil

        temp_dir = tempfile.mkdtemp(prefix="ai_img_")
        downloaded = []
        try:
            for i, url in enumerate(urls):
                try:
                    resp = await asyncio.to_thread(requests.get, url, timeout=60)
                    if resp.status_code == 200:
                        fname = f"ai_{i}.png"
                        fpath = os.path.join(temp_dir, fname)
                        with open(fpath, 'wb') as f:
                            f.write(resp.content)
                        downloaded.append(fpath)
                        print(f"[AI-Image] 📥 已下载图片 {i+1}/{len(urls)}: {len(resp.content)} 字节")
                    else:
                        print(f"[AI-Image] ⚠️ 下载图片 {i+1} 失败: HTTP {resp.status_code}")
                except Exception as e:
                    print(f"[AI-Image] ⚠️ 下载图片 {i+1} 失败: {e}")

            if not downloaded:
                print(f"[AI-Image] ❌ 没有成功下载任何图片")
                if not future.done():
                    future.set_result(False)
                return

            orig_group = self.group_code
            self.group_code = target_group
            try:
                ok = await self.send_images_multi(downloaded)
                if ok:
                    print(f"[AI-Image] ✅ AI 图片已发送到目标群")
                    if not future.done():
                        future.set_result(True)
                else:
                    print(f"[AI-Image] ❌ 图片发送失败")
                    if not future.done():
                        future.set_result(False)
            finally:
                self.group_code = orig_group
        except Exception as e:
            print(f"[AI-Image] ❌ 处理异常: {e}")
            if not future.done():
                future.set_result(False)
        finally:
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception:
                pass

    async def _download_and_send_images(self, urls: list, target_item: dict):
        """下载图片并发送到原群"""
        import shutil

        temp_dir = tempfile.mkdtemp(prefix="proxy_img_")
        downloaded = []
        try:
            for i, url in enumerate(urls):
                try:
                    resp = await asyncio.to_thread(requests.get, url, timeout=60)
                    if resp.status_code == 200:
                        fname = f"yuanbao_{i}.png"
                        fpath = os.path.join(temp_dir, fname)
                        with open(fpath, 'wb') as f:
                            f.write(resp.content)
                        downloaded.append(fpath)
                        print(f"[Auto-Proxy] 📥 已下载图片 {i+1}/{len(urls)}: {len(resp.content)} 字节")
                    else:
                        print(f"[Auto-Proxy] ⚠️ 下载图片 {i+1} 失败: HTTP {resp.status_code}")
                except Exception as e:
                    print(f"[Auto-Proxy] ⚠️ 下载图片 {i+1} 失败: {e}")

            if not downloaded:
                print(f"[Auto-Proxy] ❌ 没有成功下载任何图片")
                return

            orig_group = self.group_code
            self.group_code = target_item["target_group"]
            try:
                ok = await self.send_images_multi(downloaded)
                if ok:
                    print(f"[Auto-Proxy] ✅ 元宝图片已转发到原群")
                    if not target_item["future"].done():
                        target_item["future"].set_result(True)
                else:
                    print(f"[Auto-Proxy] ❌ 图片发送到原群失败")
            finally:
                self.group_code = orig_group
        finally:
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception:
                pass

    def _maybe_handle_yuanbao_reply(self, push_json, text_content):
        """检测中转群里元宝的文本回复，转发回原群"""
        if not self.forward_at_yuanbao:
            return
        if (not self._proxy_queue or
                push_json.get("group_code") != IMAGE_GROUP_CODE or
                push_json.get("from_account") != YUANBAO_BOT_ID):
            return
        reply = text_content.strip()
        if not reply:
            return

        if re.search(r'!\[image\]\([^)]+\)', reply):
            return

        target_item = None
        for item in self._proxy_queue:
            if item.get("forwarded") and not item.get("reply_received"):
                target_item = item
                break

        if target_item is None:
            print(f"[Auto-Proxy] ⚠️ 收到元宝回复但无匹配待回复 item (队列{len(self._proxy_queue)}条)")
            return

        if target_item.get("reply_received"):
            return
        target_item["reply_received"] = True

        msg = self._build_reply_msg(reply, target_item.get("ref_msg_id", ""), target_group=target_item["target_group"])
        if msg:
            try:
                asyncio.create_task(self._send_and_complete(msg, target_item))
                print(f"[Auto-Proxy] 📥 收到元宝回复 → 转发到原群 | {reply[:60]}")
            except Exception as e:
                print(f"[Auto-Proxy] 回复转发失败: {e}")
                target_item["reply_received"] = False
        else:
            print(f"[Auto-Proxy] ⚠️ 构建回复消息失败")
            target_item["reply_received"] = False

    async def _send_and_complete(self, msg, current):
        """发送回复到原群并标记完成"""
        try:
            if not self.connected or not self.ws:
                print(f"[Auto-Proxy] ⚠️ 未连接，无法转发回复到原群")
                return
            await self.ws.send(msg)
            print(f"[Auto-Proxy] ✅ 已转发元宝回复到原群")
            if not current["future"].done():
                current["future"].set_result(True)
        except Exception as e:
            print(f"[Auto-Proxy] ❌ 转发回复到原群失败: {e}")

    def _handle_response(self, msg_id, conn_msg):
        """处理服务端响应"""
        if msg_id in self.pending_requests:
            future = self.pending_requests.pop(msg_id)
            try:
                cmd = conn_msg.get("head", {}).get("cmd", "")
                if cmd == BIZ_CMD_GET_MEMBERS:
                    data = conn_msg.get("data", b"")
                    future.set_result(self.codec.decode_get_group_member_list_rsp(data) if data
                                      else {"code": -1, "message": "响应数据为空"})
                elif cmd == BIZ_CMD_QUERY_GROUP_INFO:
                    data = conn_msg.get("data", b"")
                    future.set_result(self.codec.decode_query_group_info_rsp(data) if data
                                      else {"code": -1, "message": "响应数据为空"})
                elif cmd in (BIZ_CMD_SEND_GROUP, BIZ_CMD_SEND_C2C):
                    future.set_result({"code": 0, "message": "发送成功"})
                else:
                    future.set_result(conn_msg)
            except Exception as e:
                future.set_exception(e)

    # ── 图片/文件上传 ──
    def _get_upload_info(self, filename: str, file_id: str) -> Optional[dict]:
        if not self.bot_id or not self.token:
            return None
        url = f"https://{API_DOMAIN}/api/resource/genUploadInfo"
        headers = {
            "Content-Type": "application/json", "X-ID": self.bot_id, "X-Token": self.token,
            "X-Source": "web", "X-AppVersion": "2.0.1", "X-OperationSystem": "Linux", "X-Instance-Id": "99",
        }
        try:
            r = requests.post(url, headers=headers,
                             json={"fileName": filename, "fileId": file_id, "docFrom": "localDoc", "docOpenId": ""},
                             timeout=HTTP_TIMEOUT)
            result = r.json()
            return result.get("data", result) if result.get("code", 0) == 0 else None
        except Exception as e:
            print(f"[上传] 获取凭证失败: {e}")
            return None

    def _upload_to_cos(self, config: dict, data: bytes, filename: str, max_retries: int = 3) -> Optional[str]:
        """上传到腾讯云 COS（带重试机制）"""
        for attempt in range(1, max_retries + 1):
            try:
                from qcloud_cos import CosConfig, CosS3Client
                cc = CosConfig(Region=config["region"], SecretId=config["encryptTmpSecretId"],
                               SecretKey=config["encryptTmpSecretKey"], Token=config["encryptToken"])
                CosS3Client(cc).put_object(Bucket=config["bucketName"], Body=data,
                                            Key=config["location"], ContentType="application/octet-stream")
                return config.get("resourceUrl")
            except ImportError:
                return self._upload_to_cos_manual(config, data)
            except Exception as e:
                print(f"[上传] COS SDK 第{attempt}次失败: {e}")
                if attempt < max_retries:
                    time.sleep(1 * attempt)
                else:
                    return self._upload_to_cos_manual(config, data)

    def _upload_to_cos_manual(self, config: dict, data: bytes) -> Optional[str]:
        """手动构造 COS 签名上传"""
        import hmac as hm
        secret_id = config.get("encryptTmpSecretId", "")
        secret_key = config.get("encryptTmpSecretKey", "")
        token = config.get("encryptToken", "")
        kt = f"{config.get('startTime',0)};{config.get('expiredTime',0)}"
        sign_key = hm.new(secret_key.encode(), kt.encode(), hashlib.sha1).hexdigest()
        http_str = f"put\n{config['location']}\n\nhost={config['bucketName']}.cos.{config['region']}.myqcloud.com\n"
        string_to_sign = f"sha1\n{kt}\n{hashlib.sha1(http_str.encode()).hexdigest()}\n"
        sig = hm.new(sign_key.encode(), string_to_sign.encode(), hashlib.sha1).hexdigest()
        auth = f"q-sign-algorithm=sha1&q-ak={secret_id}&q-sign-time={kt}&q-key-time={kt}&q-header-list=host&q-url-param-list=&q-signature={sig}"
        if token:
            auth += f"&x-cos-security-token={token}"
        url = f"https://{config['bucketName']}.cos.{config['region']}.myqcloud.com{config['location']}"
        headers = {"Host": f"{config['bucketName']}.cos.{config['region']}.myqcloud.com",
                   "Authorization": auth, "Content-Type": "application/octet-stream"}
        if token:
            headers["x-cos-security-token"] = token
        try:
            r = requests.put(url, headers=headers, data=data, timeout=60)
            return config.get("resourceUrl", url) if r.status_code == 200 else None
        except Exception as e:
            print(f"[上传] 手动签名失败: {e}")
            return None

    # ── 消息构建辅助 ──
    def _build_raw_sticker_msg(self, name, sid, pid, group_code=None) -> bytes:
        gc = group_code or self.group_code or ""
        known = self.STICKERS.get(name, {})
        face = self.codec.encode_tim_face_elem(sid, pid, name,
                                               known.get("width",128), known.get("height",128),
                                               known.get("formats","png"))
        data = self.codec.encode_string(1, self._generate_msg_id())
        data += self.codec.encode_string(2, gc)
        data += self.codec.encode_string(3, self.bot_id or "")
        data += self.codec.encode_string(5, str(random.randint(1, 999999999)))
        data += self.codec.encode_message_field(6, face)
        head = HeadBuilder.request(BIZ_CMD_SEND_GROUP, self.seq_no, self._generate_msg_id(), BIZ_MODULE)
        self.seq_no += 1
        return self.codec.encode_conn_msg(head, data)

    def _build_image_msg(self, images: list, group_code=None) -> bytes:
        """构建多图群消息"""
        gc = group_code or self.group_code or ""
        data = self.codec.encode_string(1, self._generate_msg_id())
        data += self.codec.encode_string(2, gc)
        data += self.codec.encode_string(3, self.bot_id or "")
        data += self.codec.encode_string(5, str(random.randint(1, 999999999)))
        for img in images:
            data += self.codec.encode_message_field(6, self.codec.encode_tim_image_elem(*img))
        head = HeadBuilder.request(BIZ_CMD_SEND_GROUP, self.seq_no, self._generate_msg_id(), BIZ_MODULE)
        self.seq_no += 1
        return self.codec.encode_conn_msg(head, data)

    def _build_file_msg(self, url: str, file_id="", file_size=0, file_name="", group_code=None) -> bytes:
        gc = group_code or self.group_code or ""
        data = self.codec.encode_string(1, self._generate_msg_id())
        data += self.codec.encode_string(2, gc)
        data += self.codec.encode_string(3, self.bot_id or "")
        data += self.codec.encode_string(5, str(random.randint(1, 999999999)))
        data += self.codec.encode_message_field(6, self.codec.encode_tim_file_elem(url, file_id, file_size, file_name))
        head = HeadBuilder.request(BIZ_CMD_SEND_GROUP, self.seq_no, self._generate_msg_id(), BIZ_MODULE)
        self.seq_no += 1
        return self.codec.encode_conn_msg(head, data)

    def _build_reply_msg(self, text: str, ref_msg_id: str, at_user_id="", at_nickname="", target_group=None) -> Optional[bytes]:
        gc = target_group or self.group_code
        if at_user_id:
            display = at_nickname or self.user_db.get(at_user_id, '') or at_user_id
            at_data = json.dumps({"elem_type": 1002, "text": f"@{display}", "user_id": at_user_id})
            at_elem = self.codec.encode_string(1, "TIMCustomElem") + \
                      self.codec.encode_message_field(2, self.codec.encode_string(4, at_data))
            text_elem = self.codec.encode_string(1, "TIMTextElem") + \
                        self.codec.encode_message_field(2, self.codec.encode_string(1, text))
            data = self.codec.encode_string(1, self._generate_msg_id())
            data += self.codec.encode_string(2, gc)
            data += self.codec.encode_string(3, self.bot_id or "")
            data += self.codec.encode_string(5, str(random.randint(1, 999999999)))
            data += self.codec.encode_message_field(6, at_elem)
            data += self.codec.encode_message_field(6, text_elem)
            data += self.codec.encode_string(7, ref_msg_id)
        else:
            data = self.codec.encode_send_group_msg_req(self._generate_msg_id(), gc, self.bot_id or "", text, ref_msg_id)
        head = HeadBuilder.request(BIZ_CMD_SEND_GROUP, self.seq_no, self._generate_msg_id(), BIZ_MODULE)
        self.seq_no += 1
        return self.codec.encode_conn_msg(head, data)

    # ── 公共发送方法 ──
    async def send_group_message(self, text, at_user=None, at_nickname=None,
                                  ref_msg_id=None, target_group=None) -> bool:
        if not self.connected or not self.ws:
            return False
        gc = target_group or self.group_code
        try:
            if at_user:
                msg = self._build_at_message(text, at_user, at_nickname, gc, ref_msg_id)
            else:
                msg = self.codec.encode_conn_msg(
                    HeadBuilder.request(BIZ_CMD_SEND_GROUP, self.seq_no, self._generate_msg_id(), BIZ_MODULE),
                    self.codec.encode_send_group_msg_req(self._generate_msg_id(), gc, self.bot_id or "", text, ref_msg_id or "")
                )
                self.seq_no += 1
            await self.ws.send(msg)
            self._cache_sent_message(text, gc, "send_group")
            return True
        except Exception as e:
            print(f"[发送] 群消息失败: {e}")
            return False

    def _build_at_message(self, text, at_user, at_nickname, gc, ref_msg_id) -> bytes:
        # @所有人 走独立协议分支：text 必须是 "@所有人"，否则只会被识别为普通 @昵称
        if at_user == 'all':
            at_data = json.dumps({
                "elem_type": 1002,
                "text": "@所有人",
                "user_id": AT_ALL_SPECIAL_ID,
                "at_type": "all",  # 显式标记，元宝服务端据此触发全体推送
            })
        else:
            display = at_nickname or self.user_db.get(at_user, '') or at_user
            at_data = json.dumps({"elem_type": 1002, "text": f"@{display}", "user_id": at_user})
        at_elem = self.codec.encode_string(1, "TIMCustomElem") + \
                  self.codec.encode_message_field(2, self.codec.encode_string(4, at_data))
        text_elem = self.codec.encode_string(1, "TIMTextElem") + \
                    self.codec.encode_message_field(2, self.codec.encode_string(1, text))
        data = self.codec.encode_string(1, self._generate_msg_id())
        data += self.codec.encode_string(2, gc)
        data += self.codec.encode_string(3, self.bot_id or "")
        data += self.codec.encode_string(5, str(random.randint(1, 999999999)))
        data += self.codec.encode_message_field(6, at_elem)
        data += self.codec.encode_message_field(6, text_elem)
        if ref_msg_id:
            data += self.codec.encode_string(7, ref_msg_id)
        head = HeadBuilder.request(BIZ_CMD_SEND_GROUP, self.seq_no, self._generate_msg_id(), BIZ_MODULE)
        self.seq_no += 1
        msg = self.codec.encode_conn_msg(head, data)
        # V4.2 调试日志：记录 at-all 消息体大小，便于排查协议问题
        if at_user == 'all':
            print(f"[发送] @全体成员 text=@所有人 user_id={AT_ALL_SPECIAL_ID[:8]}... len={len(msg)}B")
        return msg

    def _cache_sent_message(self, text, gc, msg_type):
        self.msg_cache.append({
            "time": datetime.now().strftime("%H:%M:%S"),
            "sender_id": self.bot_id or "", "sender_name": "我",
            "group_code": gc or "", "content": text, "msg_type": msg_type, "msg_id": "",
        })
        if len(self.msg_cache) > 500:
            self.msg_cache = self.msg_cache[-500:]

    async def send_sticker(self, sticker_name, text="", at_user=None, at_nickname=None, target_group=None) -> bool:
        if not self.connected or not self.ws or sticker_name not in self.STICKERS:
            return False
        gc = target_group or self.group_code
        try:
            s = self.STICKERS[sticker_name]
            face = self.codec.encode_tim_face_elem(s["sticker_id"], s["package_id"], s["name"])
            data = self.codec.encode_string(1, self._generate_msg_id())
            data += self.codec.encode_string(2, gc)
            data += self.codec.encode_string(3, self.bot_id or "")
            data += self.codec.encode_string(5, str(random.randint(1, 999999999)))
            if at_user:
                display = at_nickname or self.user_db.get(at_user, '') or at_user
                at_data = json.dumps({"elem_type": 1002, "text": f"@{display}", "user_id": at_user})
                at_elem = self.codec.encode_string(1, "TIMCustomElem") + \
                          self.codec.encode_message_field(2, self.codec.encode_string(4, at_data))
                data += self.codec.encode_message_field(6, at_elem)
            data += self.codec.encode_message_field(6, face)
            if text:
                data += self.codec.encode_message_field(6,
                    self.codec.encode_string(1, "TIMTextElem") +
                    self.codec.encode_message_field(2, self.codec.encode_string(1, text)))
            head = HeadBuilder.request(BIZ_CMD_SEND_GROUP, self.seq_no, self._generate_msg_id(), BIZ_MODULE)
            self.seq_no += 1
            await self.ws.send(self.codec.encode_conn_msg(head, data))
            self._cache_sent_message(f"[贴纸:{sticker_name}] {text}", gc, "send_sticker")
            return True
        except Exception as e:
            print(f"[发送] 贴纸失败: {e}")
            return False

    async def send_c2c_message(self, to_account: str, text: str) -> bool:
        if not self.connected or not self.ws:
            return False
        try:
            data = self.codec.encode_send_c2c_msg_req(self._generate_msg_id(), to_account, self.bot_id or "", text)
            head = HeadBuilder.request(BIZ_CMD_SEND_C2C, self.seq_no, self._generate_msg_id(), BIZ_MODULE)
            self.seq_no += 1
            await self.ws.send(self.codec.encode_conn_msg(head, data))
            self._cache_sent_message(text, "", "send_c2c")
            return True
        except Exception as e:
            print(f"[发送] 私聊失败: {e}")
            return False

    async def send_image(self, image_path: str) -> bool:
        """发送本地图片到当前群（单张）"""
        return await self.send_images_multi([image_path])

    async def send_images_multi(self, image_paths: list) -> bool:
        """发送多张图片到当前群（一次消息包含多图）"""
        if not self.connected or not self.ws:
            return False

        import io
        try:
            from PIL import Image
            has_pil = True
        except ImportError:
            has_pil = False

        images = []
        for path in image_paths:
            if not os.path.exists(path):
                print(f"[图片] 文件不存在: {path}")
                continue
            try:
                with open(path, 'rb') as f:
                    data = f.read()
            except Exception as e:
                print(f"[图片] 读取失败 {path}: {e}")
                continue

            max_bytes = 20 * 1024 * 1024
            if len(data) > max_bytes:
                print(f"[图片] 过大（跳过）: {path} ({len(data)/1024/1024:.1f}MB)")
                continue

            filename = os.path.basename(path)
            file_id = uuid.uuid4().hex
            config = self._get_upload_info(filename, file_id)
            if not config:
                continue

            url = self._upload_to_cos(config, data, filename)
            if not url:
                continue

            width, height = 0, 0
            if has_pil:
                try:
                    img = Image.open(io.BytesIO(data))
                    width, height = img.size
                except Exception:
                    pass

            images.append((url, file_id, len(data), width, height))

        if not images:
            print("[图片] 没有成功处理任何图片")
            return False

        try:
            msg = self._build_image_msg(images)
            await self.ws.send(msg)
            print(f"[图片] 已发送 {len(images)} 张图片")
            for path in image_paths[:len(images)]:
                self._cache_sent_message(f"[图片] {os.path.basename(path)}", self.group_code or "", "send_image")
            return True
        except Exception as e:
            print(f"[图片] 发送失败: {e}")
            return False

    async def send_file(self, file_path: str) -> bool:
        """发送文件消息"""
        if not self.connected or not self.ws:
            return False
        if not os.path.exists(file_path):
            print(f"[文件] 文件不存在: {file_path}")
            return False
        try:
            with open(file_path, 'rb') as f:
                data = f.read()
        except Exception as e:
            print(f"[文件] 读取失败: {e}")
            return False
        if len(data) > 20 * 1024 * 1024:
            print(f"[文件] 文件过大: {len(data)/1024/1024:.1f}MB")
            return False
        fname = os.path.basename(file_path)
        fid = uuid.uuid4().hex
        config = self._get_upload_info(fname, fid)
        if not config:
            return False
        url = self._upload_to_cos(config, data, fname)
        if not url:
            return False
        try:
            msg = self._build_file_msg(url, fid, len(data), fname)
            await self.ws.send(msg)
            self._cache_sent_message(f"[文件] {fname}", self.group_code or "", "send_file")
            return True
        except Exception as e:
            print(f"[文件] 发送失败: {e}")
            return False

    async def send_multi_at_message(self, text: str, at_users: list) -> bool:
        if not self.connected or not self.ws:
            return False
        try:
            data = self.codec.encode_string(1, self._generate_msg_id())
            data += self.codec.encode_string(2, self.group_code)
            data += self.codec.encode_string(3, self.bot_id or "")
            data += self.codec.encode_string(5, str(random.randint(1, 999999999)))
            for uid, nick in at_users:
                display = nick or uid
                at_data = json.dumps({"elem_type": 1002, "text": f"@{display}", "user_id": uid})
                at_elem = self.codec.encode_string(1, "TIMCustomElem") + \
                          self.codec.encode_message_field(2, self.codec.encode_string(4, at_data))
                data += self.codec.encode_message_field(6, at_elem)
            data += self.codec.encode_message_field(6,
                self.codec.encode_string(1, "TIMTextElem") +
                self.codec.encode_message_field(2, self.codec.encode_string(1, text)))
            head = HeadBuilder.request(BIZ_CMD_SEND_GROUP, self.seq_no, self._generate_msg_id(), BIZ_MODULE)
            self.seq_no += 1
            await self.ws.send(self.codec.encode_conn_msg(head, data))
            self._cache_sent_message(f"[批量艾特 {len(at_users)}人] {text}", self.group_code or "", "send_group")
            return True
        except Exception as e:
            print(f"[发送] 批量艾特失败: {e}")
            return False

    # ── 群成员查询 ──
    async def get_group_members(self) -> Optional[dict]:
        if not self.connected or not self.ws:
            return None
        msg_id = self._generate_msg_id()
        biz_data = self.codec.encode_get_group_member_list_req(self.group_code or "")
        head = HeadBuilder.request(BIZ_CMD_GET_MEMBERS, self.seq_no, msg_id, BIZ_MODULE)
        self.seq_no += 1
        # v3.5: 使用 get_event_loop_policy() 避免弃用警告
        loop = asyncio.get_event_loop_policy().get_event_loop()
        future = loop.create_future()
        self.pending_requests[msg_id] = future
        try:
            await self.ws.send(self.codec.encode_conn_msg(head, biz_data))
            return await asyncio.wait_for(future, timeout=30)
        except asyncio.TimeoutError:
            self.pending_requests.pop(msg_id, None)
            return None
        except Exception as e:
            print(f"[成员] 查询失败: {e}")
            return None

    async def get_group_info(self, group_code: str = None) -> Optional[dict]:
        """查询群信息（含群名称、群主 user_id）

        不传 group_code 时使用 self.group_code（当前选中群聊）。
        """
        if not self.connected or not self.ws:
            return None
        msg_id = self._generate_msg_id()
        target = group_code or self.group_code or ""
        biz_data = self.codec.encode_string(1, target)
        head = HeadBuilder.request(BIZ_CMD_QUERY_GROUP_INFO, self.seq_no, msg_id, BIZ_MODULE)
        self.seq_no += 1
        loop = asyncio.get_event_loop_policy().get_event_loop()
        future = loop.create_future()
        self.pending_requests[msg_id] = future
        try:
            await self.ws.send(self.codec.encode_conn_msg(head, biz_data))
            return await asyncio.wait_for(future, timeout=30)
        except asyncio.TimeoutError:
            self.pending_requests.pop(msg_id, None)
            return None
        except Exception as e:
            print(f"[群信息] 查询失败: {e}")
            return None

    # ── 刷屏工具 ──
    async def spam_with_at(self, text, count, at_user=None, at_nickname=None, interval=0.1):
        success = fail = 0
        for i in range(count):
            if await self.send_group_message(text, at_user, at_nickname):
                success += 1
            else:
                fail += 1
            if i < count - 1:
                await asyncio.sleep(interval)
        return success, fail

    async def spam_sticker(self, name, count, text="", interval=0.1):
        success = fail = 0
        for i in range(count):
            if await self.send_sticker(name, text):
                success += 1
            else:
                fail += 1
            if i < count - 1:
                await asyncio.sleep(interval)
        return success, fail

    async def spam_c2c(self, to, text, count, interval=0.1):
        success = fail = 0
        for i in range(count):
            if await self.send_c2c_message(to, text):
                success += 1
            else:
                fail += 1
            if i < count - 1:
                await asyncio.sleep(interval)
        return success, fail


# ═══════════════════════════════════════════
#  Head 构建器
# ═══════════════════════════════════════════
class HeadBuilder:
    @staticmethod
    def request(cmd: str, seq: int, msg_id: str, module: str) -> bytes:
        return SimpleProtobufCodec.encode_head(CMD_TYPE_REQUEST, cmd, seq, msg_id, module)


# ═══════════════════════════════════════════
#  Flask Web 应用
# ═══════════════════════════════════════════
app = Flask(__name__)

# ── 全局设置（必须在 sender / msg_logger 之前定义）──
settings = {
    'group_code': app_config.get('DEFAULT_GROUP_CODE', ''),
    'interval': 0.1,
    'heartbeat_interval': 10,
    'auto_reply_enabled': False,
    'group_reply_text': app_config.get('AUTO_REPLY_GROUP_TEXT', '@我干啥'),
    'c2c_reply_text': app_config.get('AUTO_REPLY_C2C_TEXT', '我是Bot'),
    'forward_mode_enabled': False,
    'forward_at_only': False,
    'forward_at_yuanbao': True,
    'msg_log_enabled': app_config.get('MSG_LOG_ENABLED', True),  # 本地消息记录开关，默认开启
    'recall_monitor_enabled': app_config.get('RECALL_MONITOR_ENABLED', True),  # 撤回监听开关（默认开启）
    'recall_notify_enabled': app_config.get('RECALL_NOTIFY_ENABLED', False),  # 撤回通知开关
    'recall_notify_target': app_config.get('RECALL_NOTIFY_TARGET', 'original'),  # 通知目标：original/relay
    'recall_color': app_config.get('RECALL_COLOR', '#ff3b30'),  # 撤回消息颜色（红橙黄绿青蓝紫），默认红
}

settings_lock = threading.Lock()

# ── 全局状态 ──
sender = EnhancedSpamSender()
sender.recall_monitor_enabled = settings.get('recall_monitor_enabled', True)
sender.recall_notify_enabled = settings.get('recall_notify_enabled', False)
sender.recall_notify_target = settings.get('recall_notify_target', 'original')


# ═══════════════════════════════════════════
#  插件生态（v4.4 新增）
#  — 扫描 plugins/ 目录，加载 plugin.py + plugin.json
#  — 调用 register(ctx)，ctx 暴露发送/事件/页面/卡片/配置 API
#  — 前端通过 /api/plugins 系列接口驱动统一格式 UI
# ═══════════════════════════════════════════
import subprocess
import importlib.util
import shutil

PLUGINS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'plugins')
os.makedirs(PLUGINS_DIR, exist_ok=True)


class PluginContext:
    """传递给插件 register(ctx) 的上下文对象，封装 Bot 全部可用能力"""

    def __init__(self, manager, plugin_name):
        self._m = manager
        self.name = plugin_name
        self.message_handlers = 0
        self.pages = []
        self.cards = []
        self.routes = 0

    @staticmethod
    def _loop():
        try:
            return asyncio.get_event_loop()
        except RuntimeError:
            return None

    # ── 发送类 API（异步执行，不阻塞调用方）──
    def send_group(self, text, at_user=None, at_nickname=None, group_code=None):
        lp = self._loop()
        if lp and sender.connected:
            lp.create_task(sender.send_group_message(
                text, at_user=at_user, at_nickname=at_nickname, target_group=group_code))

    def send_at_all(self, text, group_code=None):
        lp = self._loop()
        if lp and sender.connected:
            lp.create_task(sender.send_group_message(
                text, at_user='all', at_nickname='全体成员', target_group=group_code))

    def send_sticker(self, name, text="", at_user=None, at_nickname=None, target_group=None):
        lp = self._loop()
        if lp and sender.connected:
            lp.create_task(sender.send_sticker(
                name, text=text, at_user=at_user, at_nickname=at_nickname, target_group=target_group))

    def send_image(self, path):
        lp = self._loop()
        if lp and sender.connected:
            lp.create_task(sender.send_image(path))

    def send_file(self, path):
        lp = self._loop()
        if lp and sender.connected:
            lp.create_task(sender.send_file(path))

    def send_c2c(self, to_account, text):
        lp = self._loop()
        if lp and sender.connected:
            lp.create_task(sender.send_c2c_message(to_account, text))

    # ── 事件钩子 ──
    def on_message(self, cb):
        self.message_handlers += 1
        self._m.on_message_callbacks.append((self.name, cb))

    def on_connect(self, cb):
        self._m.on_connect_callbacks.append((self.name, cb))

    def on_disconnect(self, cb):
        self._m.on_disconnect_callbacks.append((self.name, cb))

    # ── 页面 / 卡片（元数据驱动，前端统一渲染）──
    def register_blueprint(self, bp):
        try:
            app.register_blueprint(bp)
            self.routes += 1 + len(getattr(bp, 'deferred_functions', []))
        except AssertionError:
            # 蓝图已注册（reload 场景），忽略重复注册
            pass
        except Exception as e:
            print(f"[插件] 注册蓝图失败 {self.name}: {e}")

    def register_page(self, title, icon="🧩", weight=0):
        pid = f"plugin-{self.name}-{len(self.pages)}"
        self.pages.append({"id": pid, "title": title, "icon": icon,
                           "weight": weight, "plugin": self.name})
        return pid

    def register_card(self, page, title, icon="📋", weight=0, description="",
                      rows=None, actions=None, fields=None, refresh=0):
        self.cards.append({
            "page": page, "title": title, "icon": icon, "weight": weight,
            "description": description, "rows": rows or [], "actions": actions or [],
            "fields": fields or [], "refresh": refresh,
        })

    # ── 配置持久化（每插件独立 config.json）──
    def get_config(self):
        path = os.path.join(self._m.plugins_dir, self.name, "config.json")
        try:
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception:
            pass
        return {}

    def save_config(self, data):
        path = os.path.join(self._m.plugins_dir, self.name, "config.json")
        try:
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
            print(f"[插件] 保存配置失败 {self.name}: {e}")
            return False


class PluginManager:
    """插件加载器与运行时注册表"""

    def __init__(self):
        self.plugins_dir = PLUGINS_DIR
        self.plugins = {}                       # name -> {meta, module, enabled}
        self.on_message_callbacks = []          # [(name, cb)]
        self.on_connect_callbacks = []
        self.on_disconnect_callbacks = []
        self._state_file = os.path.join(self.plugins_dir, '_state.json')
        self._state = {}
        self._load_state()

    def _load_state(self):
        try:
            if os.path.exists(self._state_file):
                with open(self._state_file, 'r', encoding='utf-8') as f:
                    self._state = json.load(f)
        except Exception:
            self._state = {}

    def _save_state(self):
        try:
            with open(self._state_file, 'w', encoding='utf-8') as f:
                json.dump(self._state, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    def is_enabled(self, name):
        return not self._state.get('disabled', {}).get(name, False)

    def set_enabled(self, name, enabled):
        d = self._state.setdefault('disabled', {})
        if enabled:
            d.pop(name, None)
        else:
            d[name] = True
        self._save_state()

    def list_plugins(self):
        return [info['meta'] for info in self.plugins.values()]

    def startup_load(self):
        try:
            for name in sorted(os.listdir(self.plugins_dir)):
                path = os.path.join(self.plugins_dir, name)
                if not os.path.isdir(path) or name.startswith('_'):
                    continue
                if not os.path.exists(os.path.join(path, 'plugin.py')):
                    continue
                self._load_plugin(name, path, silent=True)
        except Exception as e:
            print(f"[插件] 启动加载失败: {e}")

    def _clear_plugin_hooks(self, name):
        self.on_message_callbacks = [c for c in self.on_message_callbacks if c[0] != name]
        self.on_connect_callbacks = [c for c in self.on_connect_callbacks if c[0] != name]
        self.on_disconnect_callbacks = [c for c in self.on_disconnect_callbacks if c[0] != name]

    def _load_plugin(self, name, path, silent=False):
        meta = {'name': name, 'version': '0.0.0', 'author': '', 'description': '',
                'active': True, 'error': ''}
        try:
            json_path = os.path.join(path, 'plugin.json')
            if os.path.exists(json_path):
                with open(json_path, 'r', encoding='utf-8') as f:
                    j = json.load(f)
                for k in ('name', 'version', 'author', 'description'):
                    if k in j:
                        meta[k] = j[k]
            enabled = self.is_enabled(name)
            ctx = PluginContext(self, name)
            py_path = os.path.join(path, 'plugin.py')
            spec = importlib.util.spec_from_file_location(f"_plugin_{name}", py_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            if not hasattr(module, 'register'):
                meta['error'] = '缺少 register(ctx) 函数'
                meta['active'] = False
            else:
                self._clear_plugin_hooks(name)
                module.register(ctx)
                meta['message_handlers'] = ctx.message_handlers
                meta['routes'] = ctx.routes
                meta['pages'] = ctx.pages
                meta['cards'] = ctx.cards
                meta['active'] = enabled
            self.plugins[name] = {'meta': meta, 'module': module, 'enabled': enabled}
            return {'ok': True, 'plugin': name}
        except Exception as e:
            import traceback
            traceback.print_exc()
            meta.update({'active': False, 'error': str(e)})
            self.plugins[name] = {'meta': meta, 'module': None, 'enabled': False}
            return {'ok': False, 'error': str(e)} if not silent else {'ok': True, 'plugin': name}

    def reload(self, name=None):
        if name:
            if name not in self.plugins:
                return {'ok': False, 'error': '插件不存在'}
            path = os.path.join(self.plugins_dir, name)
            self._clear_plugin_hooks(name)
            return self._load_plugin(name, path)
        self.on_message_callbacks = []
        self.on_connect_callbacks = []
        self.on_disconnect_callbacks = []
        for nm in list(self.plugins.keys()):
            self._load_plugin(nm, os.path.join(self.plugins_dir, nm), silent=True)
        return {'ok': True, 'plugins': list(self.plugins.keys())}

    def toggle(self, name, enabled):
        if name not in self.plugins:
            return {'ok': False, 'error': '插件不存在'}
        self.set_enabled(name, enabled)
        self.reload(name)
        return {'ok': True, 'plugin': name, 'enabled': enabled}

    def install(self, url):
        name = url.rstrip('/').split('/')[-1]
        if name.endswith('.git'):
            name = name[:-4]
        if not name or name.startswith('_'):
            return {'ok': False, 'error': '无效的仓库地址'}
        dest = os.path.join(self.plugins_dir, name)
        if os.path.exists(dest):
            return {'ok': False, 'error': f'插件目录已存在: {name}'}
        try:
            subprocess.run(['git', 'clone', '--depth', '1', url, dest],
                           check=True, capture_output=True, text=True, timeout=120)
        except Exception as e:
            shutil.rmtree(dest, ignore_errors=True)
            return {'ok': False, 'error': f'克隆失败: {e}'}
        if not os.path.exists(os.path.join(dest, 'plugin.py')):
            shutil.rmtree(dest, ignore_errors=True)
            return {'ok': False, 'error': '仓库缺少 plugin.py'}
        return self._load_plugin(name, dest)

    def dispatch_message(self, msg):
        """在消息接收循环中调用，分发给所有启用插件的 on_message 回调"""
        for name, cb in list(self.on_message_callbacks):
            info = self.plugins.get(name)
            if not info or not info.get('enabled', True):
                continue
            try:
                res = cb(msg)
                if asyncio.iscoroutine(res):
                    asyncio.ensure_future(res)
            except Exception as e:
                print(f"[插件] on_message 异常 ({name}): {e}")

    def fire_event(self, event):
        cbs = self.on_connect_callbacks if event == 'connect' else self.on_disconnect_callbacks
        for name, cb in list(cbs):
            try:
                res = cb()
                if asyncio.iscoroutine(res):
                    asyncio.ensure_future(res)
            except Exception as e:
                print(f"[插件] {event} 事件异常 ({name}): {e}")


plugin_manager = PluginManager()
sender.plugin_manager = plugin_manager
plugin_manager.startup_load()


# 消息本地文件记录器（不限制数量，实时写入 logs/ 目录）
# 开关状态从 config.json 的 MSG_LOG_ENABLED 读取，默认 True（开启）
msg_logger = MessageLogger(enabled=settings.get('msg_log_enabled', True))

_proxy_config_version = 0
_config_lock = threading.Lock()

# Asyncio 事件循环管理
_loop = None
_loop_thread = None
_loop_ready = threading.Event()


def _run_loop():
    global _loop
    _loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_loop)
    _loop_ready.set()
    _loop.run_forever()

def _ensure_loop():
    global _loop, _loop_thread
    if _loop is None or not _loop.is_running():
        _loop_ready.clear()
        _loop = asyncio.new_event_loop()
        _loop_thread = threading.Thread(target=_run_loop, daemon=True)
        _loop_thread.start()
        if not _loop_ready.wait(timeout=5):
            raise RuntimeError("事件循环未能在 5 秒内启动")

def async_call(coro, timeout=30):
    _ensure_loop()
    return asyncio.run_coroutine_threadsafe(coro, _loop).result(timeout=timeout)

def async_call_no_wait(coro):
    _ensure_loop()
    asyncio.run_coroutine_threadsafe(coro, _loop)


# ── 配置持久化 ──
def save_config():
    """将运行时配置写回 config.json（线程安全）"""
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.json')
    with _config_lock:
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
        except FileNotFoundError:
            config = {}
        config.update({
            'AUTO_REPLY_RULES': sender.AUTO_REPLY_RULES,
            'DEFAULT_REPLY': sender.DEFAULT_REPLY,
            'AUTO_REPLY_GROUP_TEXT': sender.AUTO_REPLY_GROUP_TEXT,
            'AUTO_REPLY_C2C_TEXT': sender.AUTO_REPLY_C2C_TEXT,
            'DEFAULT_GROUP_CODE': settings['group_code'],
            'HEARTBEAT_INTERVAL': sender.heartbeat_interval,
            'FORWARD_MODE_ENABLED': settings.get('forward_mode_enabled', False),
            'FORWARD_AT_ONLY': settings.get('forward_at_only', False),
            'FORWARD_AT_YUANBAO': sender.forward_at_yuanbao,
            'MSG_LOG_ENABLED': settings.get('msg_log_enabled', True),
            'RECALL_MONITOR_ENABLED': getattr(sender, 'recall_monitor_enabled', True),
            'RECALL_NOTIFY_ENABLED': getattr(sender, 'recall_notify_enabled', False),
            'RECALL_NOTIFY_TARGET': getattr(sender, 'recall_notify_target', 'original'),
            'RECALL_COLOR': settings.get('recall_color', '#ff3b30'),
        })
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, ensure_ascii=False, indent=2)


# ── 错误处理 ──
@app.errorhandler(404)
def not_found(e):
    return jsonify({'ok': False, 'message': '接口不存在'}), 404

@app.errorhandler(405)
def method_not_allowed(e):
    return jsonify({'ok': False, 'message': '请求方法不允许'}), 405

@app.errorhandler(Exception)
def handle_exception(e):
    code = getattr(e, 'code', None)
    if code and 400 <= code < 600:
        return jsonify({'ok': False, 'message': getattr(e, 'description', str(e))}), code
    print(f"[错误] {e}")
    import traceback
    traceback.print_exc()
    return jsonify({'ok': False, 'message': f'服务器内部错误: {e}'}), 500


# ── 健康检查 ──
@app.route('/api/health')
def api_health():
    return jsonify({'ok': True, 'connected': sender.connected, 'time': datetime.now().isoformat()})


# ── 连接管理 ──
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/connect', methods=['POST'])
def api_connect():
    try:
        sender.group_code = settings['group_code']
        sender._should_reconnect = True  # 主动连接 → 允许自动重连
        if async_call(sender.connect()):
            return jsonify({'ok': True, 'message': '连接成功'})
        return jsonify({'ok': False, 'message': '连接失败，请检查配置'}), 400
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e)}), 500

@app.route('/api/disconnect', methods=['POST'])
def api_disconnect():
    try:
        async_call(sender.disconnect(manual=True))
        return jsonify({'ok': True, 'message': '已断开连接'})
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e)}), 500

@app.route('/api/status', methods=['GET'])
def api_status():
    logger_stats = msg_logger.stats() if msg_logger else {}
    return jsonify({
        'connected': sender.connected,
        'group_code': sender.group_code or settings['group_code'],
        'user_count': len(sender.user_db),
        'message_count': len(sender.msg_cache),
        'bot_id': sender.bot_id or '',
        'auto_reply_enabled': sender.auto_reply_enabled,
        'heartbeat_interval': sender.heartbeat_interval,
        'groups': list(sender.groups.values()),
        'forward_mode_enabled': sender.auto_reply_text == "yb",
        'forward_at_only': sender.auto_reply_at_only,
        'forward_at_yuanbao': sender.forward_at_yuanbao,
        'image_group_code': IMAGE_GROUP_CODE,
        'yuanbao_id': YUANBAO_BOT_ID,
        'forward_queue_length': len(sender._proxy_queue),
        'worker_running': sender._proxy_worker_task is not None and not sender._proxy_worker_task.done(),
        'logger_written': logger_stats.get('today_written', 0),
        'logger_pending': logger_stats.get('pending', 0),
        'logger_total_size': logger_stats.get('total_size', 0),
        'msg_log_enabled': msg_logger.enabled if msg_logger else False,
        'recall_monitor_enabled': getattr(sender, 'recall_monitor_enabled', True),
        'recall_notify_enabled': getattr(sender, 'recall_notify_enabled', False),
        'recall_notify_target': getattr(sender, 'recall_notify_target', 'original'),
        'recall_cache_count': len(getattr(sender, 'recall_cache', [])),
    })


# ── 消息 ──
@app.route('/api/messages', methods=['GET'])
def api_messages():
    limit = request.args.get('limit', 50, type=int)
    cache = sender.msg_cache
    return jsonify({'messages': cache[-limit:] if cache else [], 'total': len(cache)})


# ── 撤回消息查看 ──
@app.route('/api/recall-messages', methods=['GET'])
def api_recall_messages():
    limit = request.args.get('limit', 50, type=int)
    cache = sender.recall_cache
    return jsonify({'messages': cache[-limit:] if cache else [], 'total': len(cache)})


# ── 发送图片 ──
@app.route('/api/send-image', methods=['POST'])
def api_send_image():
    if not sender.connected:
        return jsonify({'ok': False, 'message': '未连接'}), 400
    try:
        if request.content_type and 'multipart' in request.content_type:
            if 'file' not in request.files:
                return jsonify({'ok': False, 'message': '未选择图片文件'}), 400
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix='_' + (request.files['file'].filename or ''),
                                              dir=os.path.dirname(os.path.abspath(__file__)))
            tmp.close()
            request.files['file'].save(tmp.name)
            # ← 修复：读取 @ 目标字段（multipart 分支此前完全忽略）
            at_u = (request.form.get('at_user') or '').strip()
            at_n = (request.form.get('at_nickname') or '').strip()
            try:
                ok = async_call(sender.send_image(tmp.name))
            finally:
                try: os.unlink(tmp.name)
                except Exception: pass
            # 发送图片后再追加一条 @ 文字消息（带用户填写的 @ 目标）
            if ok and at_u:
                if at_u == 'all':
                    sender.send_at_all()
                else:
                    sender.send_group('', at_user=at_u, at_nickname=at_n or at_u)
            return (jsonify({'ok': True, 'message': '图片发送成功'}) if ok
                    else jsonify({'ok': False, 'message': '图片发送失败'}), 400)
        else:
            data = request.get_json(force=True)
            path = data.get('path', '').strip()
            if not path or not os.path.exists(path):
                return jsonify({'ok': False, 'message': '文件路径无效'}), 400
            return (jsonify({'ok': True, 'message': '图片发送成功'}) if async_call(sender.send_image(path))
                    else jsonify({'ok': False, 'message': '图片发送失败'}), 400)
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e)}), 500


# ── 发送文件 ──
@app.route('/api/send-file', methods=['POST'])
def api_send_file():
    if not sender.connected:
        return jsonify({'ok': False, 'message': '未连接'}), 400
    try:
        if request.content_type and 'multipart' in request.content_type:
            if 'file' not in request.files:
                return jsonify({'ok': False, 'message': '未选择文件'}), 400
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix='_' + (request.files['file'].filename or ''),
                                              dir=os.path.dirname(os.path.abspath(__file__)))
            tmp.close()
            request.files['file'].save(tmp.name)
            # ← 修复：读取 @ 目标字段（multipart 分支此前完全忽略）
            at_u = (request.form.get('at_user') or '').strip()
            at_n = (request.form.get('at_nickname') or '').strip()
            try:
                ok = async_call(sender.send_file(tmp.name))
            finally:
                try: os.unlink(tmp.name)
                except Exception: pass
            # 发送文件后再追加一条 @ 文字消息（带用户填写的 @ 目标）
            if ok and at_u:
                if at_u == 'all':
                    sender.send_at_all()
                else:
                    sender.send_group('', at_user=at_u, at_nickname=at_n or at_u)
            return (jsonify({'ok': True, 'message': '文件发送成功'}) if ok
                    else jsonify({'ok': False, 'message': '文件发送失败'}), 400)
        else:
            data = request.get_json(force=True)
            path = data.get('path', '').strip()
            if not path or not os.path.exists(path):
                return jsonify({'ok': False, 'message': '文件路径无效'}), 400
            return (jsonify({'ok': True, 'message': '文件发送成功'}) if async_call(sender.send_file(path))
                    else jsonify({'ok': False, 'message': '文件发送失败'}), 400)
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e)}), 500


# ── 发送消息 ──
@app.route('/api/send', methods=['POST'])
def api_send():
    if not sender.connected:
        return jsonify({'ok': False, 'message': '未连接'}), 400
    data = request.get_json(force=True)
    mode = data.get('mode', 'normal')
    text = data.get('text', '')
    target = data.get('target', '')
    count = int(data.get('count', 1))
    interval = float(data.get('interval', settings['interval']))
    ref_msg_id = data.get('ref_msg_id', '')

    def _parse_at(ts, fallback_user="", fallback_nick=""):
        uid = (fallback_user or '').strip()
        nick = (fallback_nick or '').strip()
        raw = (ts or '').strip()
        if raw:
            parts = raw.split(':', 1)
            uid = parts[0].strip()
            nick = parts[1].strip() if len(parts) > 1 else nick
        return uid, nick

    try:
        if mode == 'normal':
            ok = async_call(sender.send_group_message(text, ref_msg_id=ref_msg_id))
            return jsonify({'ok': ok, 'message': '发送成功' if ok else '发送失败'})
        elif mode == 'at':
            if ':' not in target and target and target not in sender.user_db:
                matches = [uid for uid, nick in sender.user_db.items() if nick == target]
                if len(matches) == 1:
                    target = matches[0]
                elif len(matches) > 1:
                    if target == YUANBAO_NICKNAME:
                        target = YUANBAO_BOT_ID
                    else:
                        return jsonify({'ok': False, 'message': f'昵称「{target}」重复，请用ID艾特'}), 400
            uid, nick = _parse_at(target, data.get('at_user',''), data.get('at_nickname',''))
            if not uid: return jsonify({'ok': False, 'message': '请指定目标用户'}), 400
            ok = async_call(sender.send_group_message(text, uid, nick, ref_msg_id))
            return jsonify({'ok': ok, 'message': '发送成功' if ok else '发送失败'})
        elif mode == 'spam':
            s, f = async_call(sender.spam_with_at(text, count, interval=interval))
            return jsonify({'ok': f == 0, 'message': f'刷屏完成: 成功 {s}, 失败 {f}', 'success': s, 'fail': f})
        elif mode == 'atspam':
            if ':' not in target and target and target not in sender.user_db:
                matches = [uid for uid, nick in sender.user_db.items() if nick == target]
                if len(matches) == 1:
                    target = matches[0]
                elif len(matches) > 1:
                    if target == YUANBAO_NICKNAME:
                        target = YUANBAO_BOT_ID
                    else:
                        return jsonify({'ok': False, 'message': f'昵称「{target}」重复，请用ID艾特'}), 400
            uid, nick = _parse_at(target, data.get('at_user',''), data.get('at_nickname',''))
            if not uid: return jsonify({'ok': False, 'message': '请指定目标用户'}), 400
            s, f = async_call(sender.spam_with_at(text, count, uid, nick, interval))
            return jsonify({'ok': f == 0, 'message': f'艾特刷屏完成: 成功 {s}, 失败 {f}', 'success': s, 'fail': f})
        elif mode == 'multi-at':
            if not target: return jsonify({'ok': False, 'message': '请指定目标用户'}), 400
            at_users = []
            for uid in [u.strip() for u in target.split(',') if u.strip()]:
                if ':' in uid:
                    u, n = uid.split(':', 1)
                    at_users.append((u.strip(), n.strip()))
                else:
                    at_users.append((uid, sender.user_db.get(uid, uid)))
            ok = async_call(sender.send_multi_at_message(text, at_users))
            return jsonify({'ok': ok, 'message': '发送成功' if ok else '发送失败'})
        elif mode == 'dm':
            if not target: return jsonify({'ok': False, 'message': '请指定目标用户'}), 400
            ok = async_call(sender.send_c2c_message(target, text))
            return jsonify({'ok': ok, 'message': '发送成功' if ok else '发送失败'})
        elif mode == 'dmspam':
            if not target: return jsonify({'ok': False, 'message': '请指定目标用户'}), 400
            s, f = async_call(sender.spam_c2c(target, text, count, interval))
            return jsonify({'ok': f == 0, 'message': f'私聊刷屏完成: 成功 {s}, 失败 {f}', 'success': s, 'fail': f})
        else:
            return jsonify({'ok': False, 'message': f'未知模式: {mode}'}), 400
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e)}), 500


# ── 发送贴纸 ──
@app.route('/api/send-sticker', methods=['POST'])
def api_send_sticker():
    if not sender.connected:
        return jsonify({'ok': False, 'message': '未连接'}), 400
    data = request.get_json(force=True)
    name = data.get('name', '')
    if not name:
        return jsonify({'ok': False, 'message': '请选择贴纸'}), 400
    try:
        count = int(data.get('count', 1))
        if count > 1:
            s, f = async_call(sender.spam_sticker(name, count, data.get('text',''),
                                                   float(data.get('interval', settings['interval']))))
            return jsonify({'ok': f == 0, 'message': f'贴纸刷屏完成: 成功 {s}, 失败 {f}', 'success': s, 'fail': f})
        ok = async_call(sender.send_sticker(name, data.get('text',''),
                                             data.get('at_user','').strip(), data.get('at_nickname','').strip()))
        return jsonify({'ok': ok, 'message': '发送成功' if ok else '发送失败'})
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e)}), 500


# ── 引用回复 ──
@app.route('/api/send-reply', methods=['POST'])
def api_send_reply():
    if not sender.connected:
        return jsonify({'ok': False, 'message': '未连接'}), 400
    data = request.get_json(force=True)
    idx = int(data.get('index', -1))
    if idx < 0 or idx >= len(sender.msg_cache):
        return jsonify({'ok': False, 'message': '无效的消息序号'}), 400
    target_msg = sender.msg_cache[idx]
    ref = target_msg.get('msg_id', '')
    # 如果引用的消息来自其他群，则发到对应的群
    target_gc = target_msg.get('group_code', '') or None
    text = data.get('text', '')
    at_u = data.get('at_user', '').strip()
    at_n = data.get('at_nickname', '').strip()
    count = int(data.get('count', 1))
    interval = float(data.get('interval', settings['interval']))
    try:
        if count > 1:
            # v3.5: 在 Flask 工作线程中直接用 time.sleep，避免跨线程 asyncio.sleep 开销
            success = fail = 0
            for i in range(count):
                ok = async_call(sender.send_group_message(text, at_u or None, at_n or None, ref, target_group=target_gc))
                success += ok
                fail += not ok
                if i < count - 1:
                    time.sleep(interval)
            return jsonify({'ok': fail == 0, 'message': f'引用刷屏: {success}成功 {fail}失败', 'success': success, 'fail': fail})
        ok = async_call(sender.send_group_message(text, at_u or None, at_n or None, ref, target_group=target_gc))
        return jsonify({'ok': ok, 'message': '回复成功' if ok else '回复失败'})
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e)}), 500


# ── @全体成员 ──
@app.route('/api/send/at-all', methods=['POST'])
def api_send_at_all():
    if not sender.connected:
        return jsonify({
            'ok': False,
            'message': '未连接 WebSocket，请先连接',
            'code': 'NOT_CONNECTED',
        }), 400
    if not sender.group_code:
        return jsonify({
            'ok': False,
            'message': '未设置目标群号（DEFAULT_GROUP_CODE）',
            'code': 'NO_GROUP',
        }), 400
    data = request.get_json(force=True) or {}
    text = (data.get('text') or '').strip()
    if not text:
        return jsonify({
            'ok': False,
            'message': '请输入要 @全体成员 的消息内容',
            'code': 'EMPTY_TEXT',
        }), 400
    try:
        ok = async_call(sender.send_group_message(text, at_user='all', at_nickname='全体成员'))
        if ok:
            return jsonify({
                'ok': True,
                'message': '已发送 @全体成员（使用 AT_ALL_SPECIAL_ID 协议）',
                'special_id': AT_ALL_SPECIAL_ID,
                'preview': f'@所有人 {text}',
            })
        return jsonify({
            'ok': False,
            'message': '发送失败：WebSocket 已断开或消息被服务端拒绝（请检查机器人是否有 @全体 权限）',
            'code': 'SEND_FAILED',
        }), 500
    except Exception as e:
        return jsonify({
            'ok': False,
            'message': f'发送异常: {e}',
            'code': 'EXCEPTION',
        }), 500


@app.route('/api/diag/at-all', methods=['GET'])
def api_diag_at_all():
    """@全体成员 协议诊断 — 检查连接/群号/特殊 ID/权限线索"""
    return jsonify({
        'ok': True,
        'connected': getattr(sender, 'connected', False),
        'group_code': getattr(sender, 'group_code', '') or '',
        'bot_id': getattr(sender, 'bot_id', '') or '',
        'at_all_special_id': AT_ALL_SPECIAL_ID,
        'protocol': {
            'elem_type': 1002,
            'text': '@所有人',
            'user_id': AT_ALL_SPECIAL_ID,
            'at_type': 'all',
        },
        'checklist': [
            '1. WebSocket 已连接（connected=true）',
            '2. 设置了 DEFAULT_GROUP_CODE',
            '3. 机器人在目标群是管理员（必须）',
            '4. 群未禁用 @全体成员',
            '5. APP_KEY 拥有 @全体 权限（部分版本需要）',
        ],
        'frontend_calls': [
            '专用接口: POST /api/send/at-all  body={"text": "..."}',
            '通用接口: POST /api/send  body={"text":"...","mode":"at","target_id":"all"}',
        ],
    })


@app.route('/api/send/ai-image', methods=['POST'])
def api_send_ai_image():
    if not sender.connected:
        return jsonify({'ok': False, 'message': '未连接'}), 400
    if not IMAGE_GROUP_CODE:
        return jsonify({'ok': False, 'message': '配置中缺少 IMAGE_GROUP_CODE'}), 400
    data = request.get_json(force=True)
    prompt = data.get('prompt', '').strip()
    if not prompt:
        return jsonify({'ok': False, 'message': '请输入提示词'}), 400
    if sender._ai_image_request is not None and not sender._ai_image_request["future"].done():
        return jsonify({'ok': False, 'message': '上一张图片正在生成中，请稍候'}), 400

    try:
        _ensure_loop()
        future = _loop.create_future()
        target_group = sender.group_code
        if not target_group:
            return jsonify({'ok': False, 'message': '未选择群聊'}), 400
        sender._ai_image_request = {"future": future, "target_group": target_group}

        system_msg = (f"System:请生成一张图片，不要发多余文字说明，"
                      f"直接发送图片，图片比例 1024x1024\n{prompt}")
        print(f"[AI-Image] 正在发送图片生成请求: {prompt}")

        ok = async_call(sender.send_group_message(
            system_msg,
            at_user=YUANBAO_BOT_ID,
            at_nickname=YUANBAO_NICKNAME,
            target_group=IMAGE_GROUP_CODE
        ))

        if not ok:
            sender._ai_image_request = None
            return jsonify({'ok': False, 'message': '发送图片生成请求失败'}), 500

        print(f"[AI-Image] 请求已发送，等待元宝回复...")

        try:
            result = async_call(asyncio.wait_for(future, timeout=120), timeout=125)
            if result:
                print(f"[AI-Image] 完成")
                return jsonify({'ok': True, 'message': 'AI 图片已生成并发送到群聊'})
            else:
                print(f"[AI-Image] 处理失败（未检测到图片回复）")
                return jsonify({'ok': False, 'message': '元宝未回复图片'}), 500
        except asyncio.TimeoutError:
            print(f"[AI-Image] 超时: 元宝未在 120 秒内回复图片")
            if sender._ai_image_request:
                if not sender._ai_image_request["future"].done():
                    sender._ai_image_request["future"].cancel()
                sender._ai_image_request = None
            return jsonify({'ok': False, 'message': '等待元宝回复超时（120秒）'}), 500
    except Exception as e:
        if sender._ai_image_request:
            if not sender._ai_image_request["future"].done():
                sender._ai_image_request["future"].cancel()
            sender._ai_image_request = None
        return jsonify({'ok': False, 'message': str(e)}), 500


# ── 代理模式 API ──
@app.route('/api/forward-mode/enable', methods=['POST'])
def api_forward_mode_enable():
    if not sender.connected:
        return jsonify({'ok': False, 'message': '未连接'}), 400
    if not IMAGE_GROUP_CODE:
        return jsonify({'ok': False, 'message': '配置中缺少 IMAGE_GROUP_CODE'}), 400
    data = request.get_json(force=True)
    at_only = data.get('at_only', False)
    fwd_at_yb = data.get('forward_at_yuanbao', True)

    sender.auto_reply_text = "yb"
    sender.auto_reply_at_only = at_only
    sender.forward_at_yuanbao = fwd_at_yb
    sender._proxy_queue.clear()
    with settings_lock:
        settings.update({'forward_mode_enabled': True, 'forward_at_only': at_only,
                         'forward_at_yuanbao': fwd_at_yb})
    global _proxy_config_version
    _proxy_config_version += 1
    save_config()

    return jsonify({'ok': True, 'message': '代理模式已启用（实时转发 + 等待元宝回复）',
                    'at_only': at_only, 'forward_at_yuanbao': fwd_at_yb})

@app.route('/api/forward-mode/disable', methods=['POST'])
def api_forward_mode_disable():
    sender.auto_reply_text = None
    sender.auto_reply_at_only = False
    for item in sender._proxy_queue:
        if not item["future"].done():
            item["future"].cancel()
    sender._proxy_queue.clear()
    sender._proxy_worker_running = False
    if sender._proxy_worker_task and not sender._proxy_worker_task.done():
        sender._proxy_worker_task.cancel()
    sender._proxy_worker_task = None
    with settings_lock:
        settings['forward_mode_enabled'] = False
    global _proxy_config_version
    _proxy_config_version += 1
    save_config()
    return jsonify({'ok': True, 'message': '代理模式已禁用'})

@app.route('/api/forward-mode/config', methods=['GET'])
def api_forward_mode_config():
    return jsonify({'ok': True, 'enabled': sender.auto_reply_text == "yb",
                    'at_only': sender.auto_reply_at_only,
                    'forward_at_yuanbao': sender.forward_at_yuanbao,
                    'image_group_code': IMAGE_GROUP_CODE, 'yuanbao_id': YUANBAO_BOT_ID,
                    'yuanbao_nickname': YUANBAO_NICKNAME,
                    'queue_length': len(sender._proxy_queue),
                    'worker_running': sender._proxy_worker_task is not None and not sender._proxy_worker_task.done()})

@app.route('/api/forward-mode/clear-queue', methods=['POST'])
def api_forward_mode_clear_queue():
    for item in sender._proxy_queue:
        if not item["future"].done():
            item["future"].cancel()
    sender._proxy_queue.clear()
    sender._proxy_worker_running = False
    sender._proxy_worker_task = None
    return jsonify({'ok': True, 'message': '代理队列已清空'})

@app.route('/api/forward-mode/toggle-at-yuanbao', methods=['POST'])
def api_forward_mode_toggle_at_yuanbao():
    data = request.get_json(force=True)
    enabled = data.get('enabled', True)
    sender.forward_at_yuanbao = enabled
    with settings_lock:
        settings['forward_at_yuanbao'] = enabled
    global _proxy_config_version
    _proxy_config_version += 1
    save_config()
    return jsonify({'ok': True, 'message': f'@元宝已{"开启" if enabled else "关闭"}', 'forward_at_yuanbao': enabled})


# ── 自动回复 API ──
@app.route('/api/auto-reply/enable', methods=['POST'])
def api_enable_auto_reply():
    sender.auto_reply_enabled = True
    return jsonify({'ok': True, 'message': '自动回复已启用'})

@app.route('/api/auto-reply/disable', methods=['POST'])
def api_disable_auto_reply():
    sender.auto_reply_enabled = False
    return jsonify({'ok': True, 'message': '自动回复已禁用'})

@app.route('/api/auto-reply/status', methods=['GET'])
def api_get_auto_reply_status():
    return jsonify({'enabled': sender.auto_reply_enabled, 'config': {
        'auto_reply_rules': sender.AUTO_REPLY_RULES, 'default_reply': sender.DEFAULT_REPLY,
        'auto_reply_group_text': sender.AUTO_REPLY_GROUP_TEXT, 'auto_reply_c2c_text': sender.AUTO_REPLY_C2C_TEXT
    }})

@app.route('/api/auto-reply/test', methods=['POST'])
def api_test_auto_reply():
    data = request.get_json(force=True)
    text = data.get('text', '').strip()
    if not text:
        return jsonify({'ok': False, 'message': '请输入测试文本'}), 400
    reply = sender.get_auto_reply(text, data.get('is_group', True), data.get('group_code', ''))
    return jsonify({'ok': True, 'reply': reply,
                    'matched': reply not in [sender.AUTO_REPLY_GROUP_TEXT, sender.AUTO_REPLY_C2C_TEXT, sender.DEFAULT_REPLY]})

@app.route('/api/auto-reply/rules', methods=['GET'])
def api_get_auto_reply_rules():
    return jsonify({'ok': True, 'rules': sender.AUTO_REPLY_RULES, 'count': len(sender.AUTO_REPLY_RULES)})

@app.route('/api/auto-reply/rules', methods=['POST'])
def api_add_auto_reply_rule():
    data = request.get_json(force=True)
    for f in ('match_type', 'pattern', 'reply_text'):
        if f not in data or not data[f]:
            return jsonify({'ok': False, 'message': f'缺少字段: {f}'}), 400
    rule = {'match_type': data['match_type'], 'pattern': data['pattern'],
            'reply_text': data['reply_text'], 'group_only': data.get('group_only', False),
            'enabled': data.get('enabled', True), 'priority': data.get('priority', len(sender.AUTO_REPLY_RULES) + 1)}
    if 'patterns' in data:
        rule['patterns'] = data['patterns']
    sender.AUTO_REPLY_RULES.append(rule)
    save_config()
    return jsonify({'ok': True, 'message': '规则添加成功', 'rule': rule})

@app.route('/api/auto-reply/rules/<int:index>', methods=['PUT'])
def api_update_auto_reply_rule(index):
    if index < 0 or index >= len(sender.AUTO_REPLY_RULES):
        return jsonify({'ok': False, 'message': '索引无效'}), 404
    rule = sender.AUTO_REPLY_RULES[index]
    for k, v in request.get_json(force=True).items():
        if k in ('match_type', 'pattern', 'reply_text', 'patterns'):
            rule[k] = v
        elif k in ('group_only', 'enabled'):
            rule[k] = bool(v)
        elif k == 'priority':
            rule[k] = int(v)
    save_config()
    return jsonify({'ok': True, 'message': '规则更新成功', 'rule': rule})

@app.route('/api/auto-reply/rules/<int:index>', methods=['DELETE'])
def api_delete_auto_reply_rule(index):
    if index < 0 or index >= len(sender.AUTO_REPLY_RULES):
        return jsonify({'ok': False, 'message': '索引无效'}), 404
    deleted = sender.AUTO_REPLY_RULES.pop(index)
    save_config()
    return jsonify({'ok': True, 'message': '规则删除成功', 'rule': deleted})

@app.route('/api/auto-reply/rules/reorder', methods=['POST'])
def api_reorder_auto_reply_rules():
    data = request.get_json(force=True)
    new_order = data.get('order', [])
    if len(new_order) != len(sender.AUTO_REPLY_RULES):
        return jsonify({'ok': False, 'message': '排序数组长度不匹配'}), 400
    try:
        sender.AUTO_REPLY_RULES = [sender.AUTO_REPLY_RULES[i] for i in new_order]
        save_config()
        return jsonify({'ok': True, 'message': '规则顺序已更新'})
    except IndexError:
        return jsonify({'ok': False, 'message': '无效索引'}), 400


# ── 插件生态（v4.4）──
@app.route('/api/plugins', methods=['GET'])
def api_plugins():
    try:
        return jsonify({'ok': True, 'plugins': plugin_manager.list_plugins()})
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e)}), 500


@app.route('/api/plugins/install', methods=['POST'])
def api_plugins_install():
    try:
        data = request.get_json(force=True) or {}
        url = (data.get('url') or '').strip()
        if not url:
            return jsonify({'ok': False, 'error': '请输入 Git 仓库地址'}), 400
        r = plugin_manager.install(url)
        if r.get('ok'):
            return jsonify(r)
        return jsonify(r), 400
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


@app.route('/api/plugins/toggle', methods=['POST'])
def api_plugins_toggle():
    try:
        data = request.get_json(force=True) or {}
        name = data.get('name')
        enabled = bool(data.get('enabled'))
        if not name:
            return jsonify({'ok': False, 'error': '缺少插件名称'}), 400
        r = plugin_manager.toggle(name, enabled)
        if r.get('ok'):
            return jsonify(r)
        return jsonify(r), 400
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


@app.route('/api/plugins/reload', methods=['POST'])
def api_plugins_reload():
    try:
        data = request.get_json(force=True) or {}
        r = plugin_manager.reload(data.get('name'))
        return jsonify(r)
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


# ── 群聊管理 ──
@app.route('/api/groups', methods=['GET'])
def api_get_groups():
    return jsonify({'ok': True, 'groups': list(sender.groups.values()),
                    'count': len(sender.groups), 'current_group': sender.group_code})

@app.route('/api/groups/switch', methods=['POST'])
def api_switch_group():
    gc = request.get_json(force=True).get('group_code', '').strip()
    if not gc:
        return jsonify({'ok': False, 'message': '请输入群号'}), 400
    with settings_lock:
        sender.group_code = settings['group_code'] = gc
    return jsonify({'ok': True, 'message': f'已切换到群聊 {gc}'})

@app.route('/api/group/name', methods=['GET'])
def api_group_name():
    gc = request.args.get('group_code', '').strip()
    if not gc:
        return jsonify({'ok': False, 'message': '缺少 group_code'}), 400
    try:
        gi = async_call(sender.get_group_info(gc))
        if gi and gi.get('code') == 0:
            ginfo = gi.get('group_info', {})
            return jsonify({
                'ok': True, 'group_code': gc,
                'group_name': ginfo.get('group_name', gc),
                'group_owner_user_id': ginfo.get('group_owner_user_id', ''),
                'group_owner_nickname': ginfo.get('group_owner_nickname', ''),
            })
        return jsonify({'ok': False, 'message': '查询失败'}), 400
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e)}), 500


# ── 心跳管理 ──
@app.route('/api/heartbeat/interval', methods=['POST'])
def api_set_heartbeat_interval():
    try:
        iv = float(request.get_json(force=True).get('interval', 1.0))
        if not 0 <= iv <= 60:
            return jsonify({'ok': False, 'message': '间隔必须在 0-60 秒之间'}), 400
        with settings_lock:
            sender.heartbeat_interval = settings['heartbeat_interval'] = iv
        return jsonify({'ok': True, 'message': f'心跳间隔已设为 {iv} 秒'})
    except (ValueError, TypeError):
        return jsonify({'ok': False, 'message': '无效的间隔值'}), 400

@app.route('/api/heartbeat', methods=['GET'])
def api_heartbeat():
    return jsonify({'ok': True, 'timestamp': datetime.now().isoformat(), 'connected': sender.connected,
                    'heartbeat_interval': sender.heartbeat_interval,
                    'heartbeat_running': sender.heartbeat_task is not None and not sender.heartbeat_task.done(),
                    'receive_loop_running': sender.receive_task is not None and not sender.receive_task.done()})


# ── 成员管理 ──
@app.route('/api/members', methods=['GET'])
def api_members():
    if not sender.connected:
        return jsonify({'ok': False, 'message': '未连接', 'members': []}), 400
    try:
        member_result = async_call(sender.get_group_members())
        group_owner_user_id = ""
        if member_result and member_result.get('code') == 0:
            for m in member_result.get('member_list', []):
                uid = m.get('user_id', '')
                if uid:
                    sender.user_db[uid] = m.get('nick_name', '')
            try:
                gi = async_call(sender.get_group_info())
                if gi and gi.get('code') == 0:
                    group_owner_user_id = gi.get('group_info', {}).get('group_owner_user_id', '')
            except Exception:
                pass
            return jsonify({'ok': True, 'members': member_result['member_list'],
                            'count': len(member_result['member_list']),
                            'group_owner_user_id': group_owner_user_id})
        return jsonify({'ok': False, 'message': member_result.get('message', '获取失败'), 'members': []}), 400
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e), 'members': []}), 500

@app.route('/api/users', methods=['GET'])
def api_users():
        return jsonify({'ok': True, 'users': [{'user_id': k, 'nickname': v} for k, v in sender.user_db.items()],
                        'count': len(sender.user_db)})

@app.route('/api/users', methods=['POST'])
def api_add_user():
    data = request.get_json(force=True)
    uid = data.get('user_id', '').strip()
    if not uid:
        return jsonify({'ok': False, 'message': '用户ID不能为空'}), 400
    sender.user_db[uid] = data.get('nickname', '') or uid
    return jsonify({'ok': True, 'message': '用户添加成功'})

@app.route('/api/users/<user_id>', methods=['DELETE'])
def api_delete_user(user_id):
    if user_id in sender.user_db:
        del sender.user_db[user_id]
        return jsonify({'ok': True, 'message': '用户删除成功'})
    return jsonify({'ok': False, 'message': '用户不存在'}), 404


# ── 贴纸列表 ──
@app.route('/api/stickers', methods=['GET'])
def api_stickers():
    q = request.args.get('q', '').lower()
    stickers = [{'key': k, 'name': v.get('name', k), 'sticker_id': v.get('sticker_id', ''), 'package_id': v.get('package_id', '')}
                for k, v in sender.STICKERS.items()
                if q in k.lower() or q in v.get('name', '').lower()]
    return jsonify({'stickers': stickers, 'count': len(stickers)})


# ── 设置 ──
@app.route('/api/settings', methods=['GET'])
def api_get_settings():
    with settings_lock:
        return jsonify(settings)

@app.route('/api/settings', methods=['POST'])
def api_update_settings():
    global _proxy_config_version
    data = request.get_json(force=True)
    _proxy_dirty = False

    with settings_lock:
        if 'group_code' in data:
            settings['group_code'] = str(data['group_code'])
            sender.group_code = settings['group_code']
        if 'interval' in data:
            try: settings['interval'] = float(data['interval'])
            except (ValueError, TypeError): pass
        if 'heartbeat_interval' in data:
            try:
                settings['heartbeat_interval'] = sender.heartbeat_interval = float(data['heartbeat_interval'])
            except (ValueError, TypeError): pass
        if 'auto_reply_enabled' in data:
            settings['auto_reply_enabled'] = sender.auto_reply_enabled = bool(data['auto_reply_enabled'])

        if 'group_reply_text' in data:
            settings['group_reply_text'] = sender.AUTO_REPLY_GROUP_TEXT = str(data['group_reply_text'])
        if 'c2c_reply_text' in data:
            settings['c2c_reply_text'] = sender.AUTO_REPLY_C2C_TEXT = str(data['c2c_reply_text'])

        if 'forward_mode_enabled' in data:
            enabled = bool(data['forward_mode_enabled'])
            settings['forward_mode_enabled'] = enabled
            if enabled:
                sender.auto_reply_text = "yb"
                sender.auto_reply_at_only = data.get('forward_at_only', False)
            else:
                sender.auto_reply_text = None
                sender.auto_reply_at_only = False
                sender._proxy_queue.clear()
            _proxy_dirty = True
        for key in ('forward_at_only', 'forward_at_yuanbao'):
            if key in data:
                val = bool(data[key])
                settings[key] = val
                setattr(sender, key, val)
                _proxy_dirty = True
        if 'msg_log_enabled' in data:
            enabled = bool(data['msg_log_enabled'])
            settings['msg_log_enabled'] = enabled
        if 'recall_monitor_enabled' in data:
            enabled = bool(data['recall_monitor_enabled'])
            settings['recall_monitor_enabled'] = sender.recall_monitor_enabled = enabled
        if 'recall_notify_enabled' in data:
            enabled = bool(data['recall_notify_enabled'])
            settings['recall_notify_enabled'] = sender.recall_notify_enabled = enabled
        if 'recall_notify_target' in data:
            target = str(data['recall_notify_target'])
            if target in ('original', 'relay'):
                settings['recall_notify_target'] = sender.recall_notify_target = target
        if 'recall_color' in data:
            color = str(data['recall_color'])
            # 仅接受合法的 #RGB / #RRGGBB 颜色值
            if color.startswith('#') and len(color) in (4, 7) and all(c in '0123456789abcdefABCDEF#' for c in color):
                settings['recall_color'] = color

    if _proxy_dirty:
        _proxy_config_version += 1
    if 'auto_reply_rules' in data:
        sender.AUTO_REPLY_RULES = data['auto_reply_rules']
    if 'default_reply' in data:
        sender.DEFAULT_REPLY = data['default_reply']
    if 'msg_log_enabled' in data:
        if msg_logger is not None:
            if enabled:
                msg_logger.enable()
            else:
                msg_logger.disable()
    save_config()
    return jsonify({'ok': True, 'settings': settings})


# ── config.json 可视化编辑 API ──
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.json')

# 修改后必须重启才能生效的字段
RESTART_ONLY_KEYS = ('APP_KEY', 'APP_SECRET', 'API_DOMAIN', 'WS_URL', 'PORT',
                     'IMAGE_GROUP_CODE', 'YUANBAO_ID')

@app.route('/api/config', methods=['GET'])
def api_get_config():
    """读取 config.json 完整内容（前端可视化渲染）"""
    try:
        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
            config = json.load(f)
        return jsonify({'ok': True, 'config': config, 'path': CONFIG_PATH})
    except Exception as e:
        return jsonify({'ok': False, 'message': f'读取 config.json 失败: {e}'}), 500

@app.route('/api/config', methods=['POST'])
def api_save_config():
    """保存 config.json：写入磁盘 + 热应用可运行时修改的字段"""
    data = request.get_json(force=True)
    new_config = data.get('config')
    if not isinstance(new_config, dict):
        return jsonify({'ok': False, 'message': '配置必须是 JSON 对象'}), 400
    try:
        # 校验可序列化
        json.dumps(new_config, ensure_ascii=False)
        # 原子写入
        tmp_path = CONFIG_PATH + '.tmp'
        with open(tmp_path, 'w', encoding='utf-8') as f:
            json.dump(new_config, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, CONFIG_PATH)

        # ── 热应用运行时可修改的字段 ──
        if 'AUTO_REPLY_RULES' in new_config and isinstance(new_config['AUTO_REPLY_RULES'], list):
            sender.AUTO_REPLY_RULES = new_config['AUTO_REPLY_RULES']
        if 'DEFAULT_REPLY' in new_config:
            sender.DEFAULT_REPLY = str(new_config['DEFAULT_REPLY'])
        if 'AUTO_REPLY_GROUP_TEXT' in new_config:
            sender.AUTO_REPLY_GROUP_TEXT = str(new_config['AUTO_REPLY_GROUP_TEXT'])
        if 'AUTO_REPLY_C2C_TEXT' in new_config:
            sender.AUTO_REPLY_C2C_TEXT = str(new_config['AUTO_REPLY_C2C_TEXT'])
        if 'DEFAULT_GROUP_CODE' in new_config:
            with settings_lock:
                settings['group_code'] = sender.group_code = str(new_config['DEFAULT_GROUP_CODE'])
        if 'HEARTBEAT_INTERVAL' in new_config:
            try:
                iv = float(new_config['HEARTBEAT_INTERVAL'])
                if 0 <= iv <= 60:
                    with settings_lock:
                        settings['heartbeat_interval'] = sender.heartbeat_interval = iv
            except (ValueError, TypeError):
                pass
        if 'FORWARD_AT_YUANBAO' in new_config:
            sender.forward_at_yuanbao = bool(new_config['FORWARD_AT_YUANBAO'])
        if 'MSG_LOG_ENABLED' in new_config and msg_logger is not None:
            enabled = bool(new_config['MSG_LOG_ENABLED'])
            if enabled:
                msg_logger.enable()
            else:
                msg_logger.disable()
            with settings_lock:
                settings['msg_log_enabled'] = enabled
        if 'RECALL_MONITOR_ENABLED' in new_config:
            enabled = bool(new_config['RECALL_MONITOR_ENABLED'])
            with settings_lock:
                settings['recall_monitor_enabled'] = sender.recall_monitor_enabled = enabled
        if 'RECALL_NOTIFY_ENABLED' in new_config:
            enabled = bool(new_config['RECALL_NOTIFY_ENABLED'])
            with settings_lock:
                settings['recall_notify_enabled'] = sender.recall_notify_enabled = enabled
        if 'RECALL_NOTIFY_TARGET' in new_config:
            target = str(new_config['RECALL_NOTIFY_TARGET'])
            if target in ('original', 'relay'):
                with settings_lock:
                    settings['recall_notify_target'] = sender.recall_notify_target = target
        if 'RECALL_COLOR' in new_config:
            color = str(new_config['RECALL_COLOR'])
            if color.startswith('#') and len(color) in (4, 7) and all(c in '0123456789abcdefABCDEF#' for c in color):
                with settings_lock:
                    settings['recall_color'] = color

        restart_keys = [k for k in RESTART_ONLY_KEYS if k in new_config]
        msg = 'config.json 已保存并生效'
        if restart_keys:
            msg += f'；需重启后生效: {", ".join(restart_keys)}'
        return jsonify({'ok': True, 'message': msg, 'restart_required': bool(restart_keys),
                        'path': CONFIG_PATH, 'config': new_config})
    except Exception as e:
        return jsonify({'ok': False, 'message': f'保存失败: {e}'}), 500


# ── 代理队列详情 ──
@app.route('/api/forward-mode/queue', methods=['GET'])
def api_forward_queue_detail():
    items = [{'index': i, 'sender_name': item.get("ref_sender_name",""),
              'content': item.get("original_content","")[:60],
              'media_type': (item.get("media_info") or {}).get("type",""),
              'target_group': item.get("target_group",""),
              'done': item["future"].done() if item.get("future") else False}
             for i, item in enumerate(sender._proxy_queue)]
    return jsonify({'ok': True, 'enabled': sender.auto_reply_text == "yb",
                    'at_only': sender.auto_reply_at_only,
                    'forward_at_yuanbao': sender.forward_at_yuanbao,
                    'image_group_code': IMAGE_GROUP_CODE, 'yuanbao_id': YUANBAO_BOT_ID,
                    'queue_length': len(sender._proxy_queue),
                    'worker_running': sender._proxy_worker_task is not None and not sender._proxy_worker_task.done(),
                    'items': items})


# ── 消息本地记录 API ──
@app.route('/api/msg-log/stats', methods=['GET'])
def api_msg_log_stats():
    if msg_logger is None:
        return jsonify({'ok': False, 'message': '记录器未初始化'}), 500
    stats = msg_logger.stats()
    stats['enabled'] = msg_logger.enabled
    return jsonify({'ok': True, **stats})

@app.route('/api/msg-log/enable', methods=['POST'])
def api_msg_log_enable():
    """开启本地消息记录"""
    if msg_logger is None:
        return jsonify({'ok': False, 'message': '记录器未初始化'}), 500
    msg_logger.enable()
    save_config()
    return jsonify({'ok': True, 'message': '本地消息记录已开启', 'enabled': msg_logger.enabled})

@app.route('/api/msg-log/disable', methods=['POST'])
def api_msg_log_disable():
    """关闭本地消息记录"""
    if msg_logger is None:
        return jsonify({'ok': False, 'message': '记录器未初始化'}), 500
    msg_logger.disable()
    save_config()
    return jsonify({'ok': True, 'message': '本地消息记录已关闭', 'enabled': msg_logger.enabled})

@app.route('/api/msg-log/toggle', methods=['POST'])
def api_msg_log_toggle():
    """切换本地消息记录开关"""
    if msg_logger is None:
        return jsonify({'ok': False, 'message': '记录器未初始化'}), 500
    data = request.get_json(force=True) if request.is_json else {}
    if 'enabled' in data:
        enabled = bool(data['enabled'])
    else:
        enabled = not msg_logger.enabled
    if enabled:
        msg_logger.enable()
    else:
        msg_logger.disable()
    save_config()
    return jsonify({'ok': True, 'message': f'本地消息记录已{"开启" if msg_logger.enabled else "关闭"}',
                    'enabled': msg_logger.enabled})

@app.route('/api/msg-log/files', methods=['GET'])
def api_msg_log_files():
    if msg_logger is None:
        return jsonify({'ok': False, 'message': '记录器未初始化'}), 500
    return jsonify({'ok': True, 'files': msg_logger.list_files()})

@app.route('/api/msg-log/recent', methods=['GET'])
def api_msg_log_recent():
    if msg_logger is None:
        return jsonify({'ok': False, 'message': '记录器未初始化'}), 500
    count = request.args.get('count', 100, type=int)
    fmt = request.args.get('format', 'text')
    if fmt not in ('text', 'jsonl'):
        fmt = 'text'
    lines = msg_logger.read_recent(count=count, fmt=fmt)
    return jsonify({'ok': True, 'format': fmt, 'count': len(lines), 'lines': lines})

@app.route('/api/msg-log/download', methods=['GET'])
def api_msg_log_download():
    if msg_logger is None:
        return jsonify({'ok': False, 'message': '记录器未初始化'}), 500
    fmt = request.args.get('format', 'txt')
    fname = request.args.get('file', '').strip()
    base = msg_logger.base_dir
    if fname:
        if '/' in fname or '\\' in fname or '..' in fname:
            return jsonify({'ok': False, 'message': '非法文件名'}), 400
        fp = os.path.join(base, fname)
        if not os.path.exists(fp):
            return jsonify({'ok': False, 'message': '文件不存在'}), 404
    else:
        ext = '.txt' if fmt == 'txt' else '.log'
        today = datetime.now().strftime("%Y%m%d")
        fp = os.path.join(base, f"messages_{today}{ext}")
        if not os.path.exists(fp):
            return jsonify({'ok': False, 'message': '今日尚无记录'}), 404
    return send_file(fp, as_attachment=True, download_name=os.path.basename(fp))

@app.route('/api/msg-log/clear-today', methods=['POST'])
def api_msg_log_clear_today():
    if msg_logger is None:
        return jsonify({'ok': False, 'message': '记录器未初始化'}), 500
    base = msg_logger.base_dir
    today = datetime.now().strftime("%Y%m%d")
    cleared = 0
    for ext in ('.log', '.txt'):
        fp = os.path.join(base, f"messages_{today}{ext}")
        if os.path.exists(fp):
            try:
                os.remove(fp)
                cleared += 1
            except Exception as e:
                return jsonify({'ok': False, 'message': f'删除失败: {e}'}), 500
    msg_logger._total_written = 0
    return jsonify({'ok': True, 'message': f'今日日志已清空（{cleared} 个文件）'})


# ═══════════════════════════════════════════
#  SSE 事件流（v3.5: 非阻塞实现）
# ═══════════════════════════════════════════
class SSEBroker:
    """非阻塞 SSE 广播器

    - 主线程将事件放入线程安全的队列
    - 每个 SSE 连接独立消费队列，不阻塞 Flask 工作线程
    - 使用 1 个后台轮询线程替代 time.sleep
    """

    def __init__(self):
        self._subscribers: list = []
        self._lock = threading.Lock()
        self._running = True
        self._thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._thread.start()

    def subscribe(self):
        """为新的 SSE 客户端创建一个队列"""
        q = deque()
        with self._lock:
            self._subscribers.append(q)
        return q

    def unsubscribe(self, q):
        with self._lock:
            if q in self._subscribers:
                self._subscribers.remove(q)

    def publish(self, event: str):
        """向所有订阅者广播一条事件"""
        with self._lock:
            for q in self._subscribers:
                q.append(event)

    def _poll_loop(self):
        """后台线程：定期采集状态变化并广播（替代 SSE 生成器中的 time.sleep）"""
        last_len = 0
        last_queue_sig = ""
        last_logger_sig = ""
        loop_count = 0

        while self._running:
            try:
                # 消息增量检测
                cur_len = len(sender.msg_cache)
                if cur_len > last_len:
                    for msg in sender.msg_cache[last_len:cur_len]:
                        self.publish(f"data: {json.dumps(msg, ensure_ascii=False)}\n\n")
                    last_len = cur_len
                elif cur_len < last_len:
                    last_len = cur_len

                # 代理队列状态
                queue_items = [{'sender': it.get("ref_sender_name",""),
                                'content': (it.get("original_content","") or "")[:60],
                                'media_type': (it.get("media_info") or {}).get("type",""),
                                'done': it["future"].done() if it.get("future") else False,
                                'forwarded': it.get("forwarded", False),
                                'reply_received': it.get("reply_received", False)}
                               for it in sender._proxy_queue]
                qlen = len(sender._proxy_queue)
                worker = getattr(sender, '_proxy_worker_running', False)
                sig = json.dumps({'qlen': qlen, 'worker': worker, 'ver': _proxy_config_version,
                                   'at_yb': sender.forward_at_yuanbao,
                                   'items': queue_items}, ensure_ascii=False)
                if sig != last_queue_sig:
                    last_queue_sig = sig
                    current = None
                    for it in queue_items:
                        if it.get("forwarded") and not it.get("reply_received"):
                            current = it
                            break
                    evt = json.dumps({'type': 'proxy_status', 'enabled': sender.auto_reply_text == "yb",
                                      'at_only': sender.auto_reply_at_only,
                                      'forward_at_yuanbao': sender.forward_at_yuanbao,
                                      'image_group_code': IMAGE_GROUP_CODE, 'yuanbao_id': YUANBAO_BOT_ID,
                                      'queue_length': qlen, 'worker_running': worker, 'current': current,
                                      'items': queue_items}, ensure_ascii=False)
                    self.publish(f"data: {evt}\n\n")

                # 每 ~5 秒广播 logger 统计
                loop_count += 1
                if loop_count % 10 == 0 and msg_logger is not None:
                    stats = msg_logger.stats()
                    stats['enabled'] = msg_logger.enabled
                    lg_sig = json.dumps(stats, ensure_ascii=False)
                    if lg_sig != last_logger_sig:
                        last_logger_sig = lg_sig
                        self.publish(f"data: {json.dumps({'type': 'logger_stats', **stats}, ensure_ascii=False)}\n\n")

                time.sleep(1.0)
            except Exception as e:
                print(f"[SSE] 轮询异常: {e}")
                time.sleep(1)

    def shutdown(self):
        self._running = False


# 全局 SSE 广播器实例
sse_broker = SSEBroker()


@app.route('/api/events')
def api_events():
    """SSE 事件流端点（v3.5: 从 broker 队列消费，不阻塞 Flask 线程）"""
    def generate():
        q = sse_broker.subscribe()
        try:
            yield ": connected\n\n"
            while True:
                if q:
                    # 批量取出所有待发事件
                    events = []
                    while q:
                        events.append(q.popleft())
                    for evt in events:
                        yield evt
                else:
                    # 无事件时发送心跳保活
                    yield ": heartbeat\n\n"
                    time.sleep(0.5)
        except GeneratorExit:
            sse_broker.unsubscribe(q)

    return Response(stream_with_context(generate()), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'Connection': 'keep-alive', 'X-Accel-Buffering': 'no'})


# ── 启动入口 ──
if __name__ == '__main__':
    import socket

    template_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
    os.makedirs(template_dir, exist_ok=True)

    def get_local_ip():
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(('8.8.8.8', 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return '127.0.0.1'

    local_ip = get_local_ip()
    print("=" * 60)
    print("  元宝 Bot Web 控制台 - v5.2")
    print("=" * 60)
    print(f"  本地:  http://127.0.0.1:{PORT}")
    print(f"  网络:  http://{local_ip}:{PORT}")
    print(f"  日志:  {msg_logger.base_dir}/")
    print("=" * 60)

    import atexit
    atexit.register(lambda: msg_logger.shutdown() if msg_logger else None)
    atexit.register(lambda: sse_broker.shutdown())

    app.run(host='0.0.0.0', port=PORT, debug=False, threaded=True)
