import asyncio
import time
from datetime import datetime
from typing import Any

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.message_components import Image
from astrbot.api.star import Context, Star, register


@register("qun_helper", "YourName", "入群欢迎与定时消息插件", "1.0.0")
class QunHelperPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self._scheduler_task: asyncio.Task | None = None
        self._running = True

        # 用于主动消息发送，缓存群号对应的 unified_msg_origin
        self._group_umo_cache: dict[str, str] = {}

        # 防止同一入群事件重复触发，短时间去重
        self._recent_welcome_keys: dict[str, float] = {}
        self._welcome_dedupe_seconds = 30
        self._welcome_cache_ttl_seconds = 300

        # 防止定时消息在同一天内重复发送
        self._last_schedule_fire_key: str | None = None

    async def initialize(self):
        self._scheduler_task = asyncio.create_task(self._scheduled_sender_loop())

    async def terminate(self):
        self._running = False
        if self._scheduler_task and not self._scheduler_task.done():
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def cache_group_umo(self, event: AstrMessageEvent):
        """缓存群会话 UMO，供定时主动发送使用。"""
        group_id = event.get_group_id()
        if not group_id:
            return
        self._group_umo_cache[str(group_id)] = event.unified_msg_origin

    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    @filter.event_message_type(filter.EventMessageType.ALL)
    async def on_group_member_increase(self, event: AstrMessageEvent):
        """仅处理 OneBot v11 的群成员入群通知。"""
        if not self._get_bool("enable_welcome", True):
            return

        raw = getattr(event.message_obj, "raw_message", None)
        post_type = self._raw_get(raw, "post_type")
        notice_type = self._raw_get(raw, "notice_type")

        if post_type != "notice" or notice_type != "group_increase":
            return

        group_id = str(self._raw_get(raw, "group_id") or "").strip()
        user_id = str(self._raw_get(raw, "user_id") or "").strip()
        sub_type = str(self._raw_get(raw, "sub_type") or "").strip()

        if not group_id or not user_id:
            return

        welcome_whitelist = set(self._get_str_list("welcome_group_whitelist"))
        if group_id not in welcome_whitelist:
            return

        if self._is_duplicate_welcome(group_id, user_id, sub_type):
            return

        msg = self._build_welcome_message(group_id=group_id, user_id=user_id)
        image_source = self._get_text("welcome_image", "")
        try:
            await self._send_group_notice(
                event.unified_msg_origin,
                text=msg,
                image_source=image_source,
            )
        except Exception as exc:
            logger.error(
                "发送入群欢迎失败 group_id=%s user_id=%s err=%s",
                group_id,
                user_id,
                exc,
            )

    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    @filter.event_message_type(filter.EventMessageType.ALL)
    async def on_group_member_decrease(self, event: AstrMessageEvent):
        """仅处理 OneBot v11 的群成员退群通知。"""
        if not self._get_bool("enable_leave_notice", True):
            return

        raw = getattr(event.message_obj, "raw_message", None)
        post_type = self._raw_get(raw, "post_type")
        notice_type = self._raw_get(raw, "notice_type")

        if post_type != "notice" or notice_type != "group_decrease":
            return

        group_id = str(self._raw_get(raw, "group_id") or "").strip()
        user_id = str(self._raw_get(raw, "user_id") or "").strip()
        operator_id = str(self._raw_get(raw, "operator_id") or "").strip()
        sub_type = str(self._raw_get(raw, "sub_type") or "").strip()

        if not group_id or not user_id:
            return

        notice_whitelist = set(self._get_str_list("welcome_group_whitelist"))
        if group_id not in notice_whitelist:
            return

        if self._is_duplicate_welcome(group_id, user_id, f"decrease:{sub_type}"):
            return

        msg = self._build_leave_message(
            group_id=group_id,
            user_id=user_id,
            operator_id=operator_id,
            sub_type=sub_type,
        )
        image_source = self._get_text("leave_image", "")
        try:
            await self._send_group_notice(
                event.unified_msg_origin,
                text=msg,
                image_source=image_source,
            )
        except Exception as exc:
            logger.error(
                "发送退群通知失败 group_id=%s user_id=%s err=%s",
                group_id,
                user_id,
                exc,
            )

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("开启欢迎")
    async def enable_welcome_command(self, event: AstrMessageEvent):
        self.config["enable_welcome"] = True
        await self._persist_config()
        yield event.plain_result("已开启入群欢迎。")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("关闭欢迎")
    async def disable_welcome_command(self, event: AstrMessageEvent):
        self.config["enable_welcome"] = False
        await self._persist_config()
        yield event.plain_result("已关闭入群欢迎。")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("查看当前白名单")
    async def show_welcome_whitelist_command(self, event: AstrMessageEvent):
        whitelist = self._get_str_list("welcome_group_whitelist")
        if not whitelist:
            yield event.plain_result("当前入群欢迎白名单为空。")
            return

        lines = ["当前入群欢迎白名单："]
        for idx, group_id in enumerate(whitelist, start=1):
            lines.append(f"{idx}. {group_id}")
        yield event.plain_result("\n".join(lines))

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("设置欢迎语")
    async def set_welcome_template_command(self, event: AstrMessageEvent):
        template = self._extract_command_payload(event, "设置欢迎语")
        if not template:
            yield event.plain_result(
                "用法：/设置欢迎语 欢迎新成员 {user_id} 加入群 {group_id}"
            )
            return

        self.config["welcome_template"] = template
        await self._persist_config()
        yield event.plain_result("已更新欢迎语模板。")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("设置入群通知")
    async def set_join_notice_command(self, event: AstrMessageEvent):
        """支持在群内通过文字 + 图片设置入群通知。"""
        text = self._extract_command_payload(event, "设置入群通知")
        image_source = self._extract_first_image_source(event)

        if not text and not image_source:
            yield event.plain_result(
                "用法：/设置入群通知 文本（可附带 1 张图片）"
            )
            return

        updated: list[str] = []
        if text:
            self.config["welcome_template"] = text
            updated.append("文字")
        if image_source:
            self.config["welcome_image"] = image_source
            updated.append("图片")

        await self._persist_config()
        yield event.plain_result("已更新入群通知：" + "、".join(updated))

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("设置退群通知")
    async def set_leave_notice_command(self, event: AstrMessageEvent):
        """支持在群内通过文字 + 图片设置退群通知。"""
        text = self._extract_command_payload(event, "设置退群通知")
        image_source = self._extract_first_image_source(event)

        if not text and not image_source:
            yield event.plain_result(
                "用法：/设置退群通知 文本（可附带 1 张图片）"
            )
            return

        updated: list[str] = []
        if text:
            self.config["leave_template"] = text
            updated.append("文字")
        if image_source:
            self.config["leave_image"] = image_source
            updated.append("图片")

        await self._persist_config()
        yield event.plain_result("已更新退群通知：" + "、".join(updated))

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("添加白名单")
    async def add_welcome_whitelist_command(self, event: AstrMessageEvent):
        group_id = self._extract_group_id_arg(event, "添加白名单")
        if not group_id:
            yield event.plain_result("用法：/添加白名单 群号")
            return

        whitelist = self._get_str_list("welcome_group_whitelist")
        if group_id in whitelist:
            yield event.plain_result(f"群 {group_id} 已在白名单中。")
            return

        whitelist.append(group_id)
        self.config["welcome_group_whitelist"] = whitelist
        await self._persist_config()
        yield event.plain_result(f"已添加白名单群号：{group_id}")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("移除白名单")
    async def remove_welcome_whitelist_command(self, event: AstrMessageEvent):
        group_id = self._extract_group_id_arg(event, "移除白名单")
        if not group_id:
            yield event.plain_result("用法：/移除白名单 群号")
            return

        whitelist = self._get_str_list("welcome_group_whitelist")
        if group_id not in whitelist:
            yield event.plain_result(f"群 {group_id} 不在白名单中。")
            return

        new_whitelist = [gid for gid in whitelist if gid != group_id]
        self.config["welcome_group_whitelist"] = new_whitelist
        await self._persist_config()
        yield event.plain_result(f"已移除白名单群号：{group_id}")

    async def _scheduled_sender_loop(self):
        while self._running:
            await asyncio.sleep(15)

            if not self._get_bool("enable_schedule", False):
                continue

            should_send, fire_key = self._should_fire_daily_schedule()
            if not should_send:
                continue

            self._last_schedule_fire_key = fire_key

            groups = self._get_str_list("schedule_group_whitelist")
            msg = self._get_text("schedule_message", "群通知：请文明发言，遵守群规。")
            if not groups or not msg:
                continue

            for group_id in groups:
                umo = self._group_umo_cache.get(group_id)
                if not umo:
                    logger.warning(
                        "定时消息跳过，未找到群会话缓存 group_id=%s。请先让该群产生一条消息。",
                        group_id,
                    )
                    continue

                try:
                    await self.context.send_message(umo, MessageChain().message(msg))
                except Exception as exc:
                    logger.error(
                        "发送定时消息失败 group_id=%s err=%s",
                        group_id,
                        exc,
                    )

    def _is_duplicate_welcome(self, group_id: str, user_id: str, sub_type: str) -> bool:
        now = time.time()
        self._cleanup_welcome_cache(now)
        key = f"{group_id}:{user_id}:{sub_type}"

        ts = self._recent_welcome_keys.get(key)
        if ts is not None and now - ts < self._welcome_dedupe_seconds:
            return True

        self._recent_welcome_keys[key] = now
        return False

    def _cleanup_welcome_cache(self, now: float) -> None:
        expired = [
            key
            for key, ts in self._recent_welcome_keys.items()
            if now - ts > self._welcome_cache_ttl_seconds
        ]
        for key in expired:
            self._recent_welcome_keys.pop(key, None)

    def _build_welcome_message(self, group_id: str, user_id: str) -> str:
        template = self._get_text(
            "welcome_template",
            "欢迎新成员 {user_id} 加入本群，祝你聊天愉快。",
        )
        try:
            return template.format(group_id=group_id, user_id=user_id)
        except Exception:
            logger.warning("welcome_template 格式错误，已回退到默认欢迎语。")
            return f"欢迎新成员 {user_id} 加入本群，祝你聊天愉快。"

    def _build_leave_message(
        self,
        group_id: str,
        user_id: str,
        operator_id: str,
        sub_type: str,
    ) -> str:
        template = self._get_text(
            "leave_template",
            "成员 {user_id} 已离开本群。",
        )
        try:
            return template.format(
                group_id=group_id,
                user_id=user_id,
                operator_id=operator_id,
                sub_type=sub_type,
            )
        except Exception:
            logger.warning("leave_template 格式错误，已回退到默认退群通知。")
            return f"成员 {user_id} 已离开本群。"

    async def _send_group_notice(
        self,
        unified_msg_origin: str,
        text: str,
        image_source: str,
    ) -> None:
        chain = self._build_notice_chain(text=text, image_source=image_source)
        if not chain.chain:
            return
        await self.context.send_message(unified_msg_origin, chain)

    def _build_notice_chain(self, text: str, image_source: str) -> MessageChain:
        chain = MessageChain()
        if text:
            chain.message(text)

        image_source = str(image_source or "").strip()
        if image_source:
            if self._is_http_url(image_source):
                chain.url_image(image_source)
            else:
                chain.file_image(image_source)

        return chain

    @staticmethod
    def _is_http_url(value: str) -> bool:
        lower = value.lower()
        return lower.startswith("http://") or lower.startswith("https://")

    @staticmethod
    def _extract_first_image_source(event: AstrMessageEvent) -> str:
        for comp in event.get_messages():
            if not isinstance(comp, Image):
                continue

            url = str(getattr(comp, "url", "") or "").strip()
            if url:
                return url

            file_path = str(getattr(comp, "file", "") or "").strip()
            if file_path:
                return file_path

        return ""

    @staticmethod
    def _raw_get(raw: Any, key: str) -> Any:
        if raw is None:
            return None

        if isinstance(raw, dict):
            return raw.get(key)

        getter = getattr(raw, "get", None)
        if callable(getter):
            try:
                return getter(key)
            except Exception:
                pass

        return getattr(raw, key, None)

    def _get_bool(self, key: str, default: bool) -> bool:
        return bool(self.config.get(key, default))

    def _get_text(self, key: str, default: str) -> str:
        value = self.config.get(key, default)
        if value is None:
            return default
        return str(value).strip()

    def _get_str_list(self, key: str) -> list[str]:
        value = self.config.get(key, [])
        if not isinstance(value, list):
            return []

        result: list[str] = []
        for item in value:
            text = str(item).strip()
            if text:
                result.append(text)
        return result

    def _should_fire_daily_schedule(self) -> tuple[bool, str]:
        schedule_time = self._get_text("schedule_daily_time", "09:00")
        parsed = self._parse_daily_time(schedule_time)
        if parsed is None:
            parsed = (9, 0)

        now = datetime.now()
        hour, minute = parsed
        fire_key = f"{now.date().isoformat()}-{hour:02d}:{minute:02d}"

        if self._last_schedule_fire_key == fire_key:
            return False, fire_key

        if now.hour == hour and now.minute == minute:
            return True, fire_key

        return False, fire_key

    @staticmethod
    def _parse_daily_time(raw: str) -> tuple[int, int] | None:
        text = str(raw or "").strip()
        if not text:
            return None

        parts = text.split(":")
        if len(parts) != 2:
            return None

        hour_str, minute_str = parts
        if not hour_str.isdigit() or not minute_str.isdigit():
            return None

        hour = int(hour_str)
        minute = int(minute_str)
        if hour < 0 or hour > 23 or minute < 0 or minute > 59:
            return None

        return hour, minute

    @staticmethod
    def _extract_command_payload(event: AstrMessageEvent, command_name: str) -> str:
        text = str(event.message_str or "").strip()
        if not text:
            return ""

        if text.startswith("/") or text.startswith("／"):
            text = text[1:].strip()

        if text.startswith(command_name):
            return text[len(command_name) :].strip()

        return ""

    @classmethod
    def _extract_group_id_arg(cls, event: AstrMessageEvent, command_name: str) -> str:
        payload = cls._extract_command_payload(event, command_name)
        if not payload:
            return ""

        group_id = payload.split()[0].strip()
        if not group_id.isdigit():
            return ""

        return group_id

    async def _persist_config(self) -> None:
        save_fn = getattr(self.config, "save_config", None)
        if not callable(save_fn):
            return

        try:
            result = save_fn()
            if asyncio.iscoroutine(result):
                await result
        except Exception as exc:
            logger.error("保存插件配置失败 err=%s", exc)
