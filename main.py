import asyncio
import time
from typing import Any

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
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
        try:
            await self.context.send_message(
                event.unified_msg_origin,
                MessageChain().message(msg),
            )
        except Exception as exc:
            logger.error(
                "发送入群欢迎失败 group_id=%s user_id=%s err=%s",
                group_id,
                user_id,
                exc,
            )

    async def _scheduled_sender_loop(self):
        while self._running:
            interval = self._get_schedule_interval()
            await asyncio.sleep(interval)

            if not self._get_bool("enable_schedule", False):
                continue

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

    def _get_schedule_interval(self) -> int:
        raw_interval = self.config.get("schedule_interval_seconds", 3600)
        try:
            interval = int(raw_interval)
        except Exception:
            interval = 3600

        # 最小 10 秒，避免误配置导致高频刷屏
        return max(interval, 10)
