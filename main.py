import asyncio
import time
from collections import defaultdict
from datetime import datetime, timedelta
from html import escape
from typing import Any

from aiohttp import web
from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.message_components import At, Image, Plain
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
        self._last_silent_reminder_fire_key: str | None = None

        # 消息排行榜统计
        self._stats_lock = asyncio.Lock()
        self._msg_total_by_user: dict[str, int] = defaultdict(int)
        self._msg_by_group_user: dict[str, dict[str, int]] = defaultdict(
            lambda: defaultdict(int)
        )
        self._msg_activity_by_group_user: dict[str, dict[str, int]] = defaultdict(
            lambda: defaultdict(int)
        )
        self._user_name_cache: dict[str, str] = {}
        self._daily_speakers_by_group_day: dict[str, set[str]] = defaultdict(set)
        self._daily_speaker_keep_days = max(
            1,
            self._get_int("speaker_stats_keep_days", 30),
        )

        # 排行榜 Web 服务
        self._rank_app_runner: web.AppRunner | None = None
        self._rank_site: web.TCPSite | None = None

        # 延时踢人任务
        self._pending_kick_tasks: set[asyncio.Task] = set()

    async def initialize(self):
        self._scheduler_task = asyncio.create_task(self._scheduled_sender_loop())
        if self._get_bool("enable_rank_server", True):
            await self._start_rank_server()

    async def terminate(self):
        self._running = False
        await self._stop_rank_server()

        for task in list(self._pending_kick_tasks):
            if task and not task.done():
                task.cancel()
        self._pending_kick_tasks.clear()

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
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def collect_message_stats(self, event: AstrMessageEvent):
        """统计群消息，用于排行榜展示。"""
        raw = getattr(event.message_obj, "raw_message", None)
        if self._raw_get(raw, "post_type") != "message":
            return

        group_id = str(event.get_group_id() or "").strip()
        user_id = str(event.get_sender_id() or "").strip()
        if not group_id or not user_id:
            return

        user_name = str(event.get_sender_name() or "").strip() or user_id
        day_key = self._today_key()
        async with self._stats_lock:
            # 独立记录群内发言活跃计数，供“观察期发言免踢”逻辑使用。
            self._msg_activity_by_group_user[group_id][user_id] += 1
            self._user_name_cache[user_id] = user_name
            self._daily_speakers_by_group_day[f"{group_id}:{day_key}"].add(user_id)
            self._cleanup_daily_speaker_cache_locked()

        rank_whitelist = set(self._get_str_list("rank_group_whitelist"))
        if rank_whitelist and group_id not in rank_whitelist:
            return

        async with self._stats_lock:
            self._msg_total_by_user[user_id] += 1
            self._msg_by_group_user[group_id][user_id] += 1

    @filter.command("群发言排行")
    async def group_rank_command(self, event: AstrMessageEvent):
        """在群内查看当前群发言排行。"""
        group_id = str(event.get_group_id() or "").strip()
        if not group_id:
            yield event.plain_result("请在群聊中使用：/群发言排行 [人数]")
            return

        rank_whitelist = set(self._get_str_list("rank_group_whitelist"))
        if rank_whitelist and group_id not in rank_whitelist:
            yield event.plain_result("当前群未开启发言排行。")
            return

        payload = self._extract_command_payload(event, "群发言排行")
        limit = self._extract_first_int(payload)
        if limit is None:
            limit = 10
        limit = max(1, min(limit, 50))

        async with self._stats_lock:
            group_data = dict(self._msg_by_group_user.get(group_id, {}))
            names = dict(self._user_name_cache)

        if not group_data:
            yield event.plain_result("当前群还没有可用的发言统计。")
            return

        rows = sorted(group_data.items(), key=lambda item: item[1], reverse=True)
        lines = [f"本群发言排行 TOP {limit}"]
        for idx, (user_id, count) in enumerate(rows[:limit], start=1):
            name = names.get(user_id, user_id)
            lines.append(f"{idx}. {name}({user_id}) - {count}")

        yield event.plain_result("\n".join(lines))

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

        notify_time = self._format_now_str()
        msg = self._build_welcome_message(
            group_id=group_id,
            user_id=user_id,
            notify_time=notify_time,
        )
        image_source = self._get_text("welcome_image", "")
        try:
            await self._send_group_notice(
                event.unified_msg_origin,
                text=msg,
                image_source=image_source,
                at_user_id=user_id,
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

        notify_time = self._format_now_str()
        msg = self._build_leave_message(
            group_id=group_id,
            user_id=user_id,
            operator_id=operator_id,
            sub_type=sub_type,
            notify_time=notify_time,
        )
        image_source = self._get_text("leave_image", "")
        try:
            await self._send_group_notice(
                event.unified_msg_origin,
                text=msg,
                image_source=image_source,
                at_user_id=user_id,
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

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("设置未发言提醒时间")
    async def set_silent_reminder_time_command(self, event: AstrMessageEvent):
        payload = self._extract_command_payload(event, "设置未发言提醒时间")
        parsed = self._parse_daily_time(payload)
        if parsed is None:
            yield event.plain_result("用法：/设置未发言提醒时间 HH:MM（24小时制）")
            return

        hour, minute = parsed
        time_text = f"{hour:02d}:{minute:02d}"
        self.config["silent_reminder_daily_time"] = time_text
        self.config["enable_silent_reminder_schedule"] = True

        group_id = str(event.get_group_id() or "").strip()
        added_group = False
        if group_id:
            groups = self._get_str_list("silent_reminder_group_whitelist")
            if group_id not in groups:
                groups.append(group_id)
                self.config["silent_reminder_group_whitelist"] = groups
                added_group = True

        await self._persist_config()
        tip = "并已将当前群加入定时提醒白名单。" if added_group else ""
        yield event.plain_result(
            f"已设置未发言定时提醒时间为 {time_text}，并已开启定时提醒功能。{tip}"
        )

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("艾特管理员")
    async def mention_admins_command(self, event: AstrMessageEvent):
        group_id = str(event.get_group_id() or "").strip()
        if not group_id:
            yield event.plain_result("请在群聊中使用：/艾特管理员 [附加内容]")
            return

        members = await self._get_group_member_list(event, group_id)
        if members is None:
            yield event.plain_result("获取群成员列表失败，请检查机器人权限。")
            return

        self_id = str(event.get_self_id() or "").strip()
        admin_ids: list[str] = []
        for member in members:
            if not isinstance(member, dict):
                continue
            user_id = str(member.get("user_id") or "").strip()
            if not user_id or user_id == self_id:
                continue
            role = str(member.get("role") or "member").lower()
            if role in {"owner", "admin"}:
                admin_ids.append(user_id)

        admin_ids = list(dict.fromkeys(admin_ids))
        if not admin_ids:
            yield event.plain_result("当前群未找到可艾特的管理员。")
            return

        payload = self._extract_command_payload(event, "艾特管理员")
        tail_text = payload if payload else "请管理员关注处理。"

        chain = MessageChain()
        for uid in admin_ids:
            chain.chain.append(At(qq=uid))
            chain.chain.append(Plain(text=" "))
        chain.chain.append(Plain(text=tail_text))

        try:
            await self.context.send_message(event.unified_msg_origin, chain)
            yield event.plain_result(f"已艾特 {len(admin_ids)} 位管理员。")
        except Exception as exc:
            logger.error("艾特管理员发送失败 group_id=%s err=%s", group_id, exc)
            yield event.plain_result("发送失败，请检查机器人发言权限。")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("全体成员")
    async def mention_all_members_command(self, event: AstrMessageEvent):
        group_id = str(event.get_group_id() or "").strip()
        if not group_id:
            yield event.plain_result("请在群聊中使用：/全体成员 内容")
            return

        payload = self._extract_command_payload(event, "全体成员")
        tail_text = payload.strip()
        if not tail_text:
            yield event.plain_result("用法：/全体成员 内容")
            return

        members = await self._get_group_member_list(event, group_id)
        if members is None:
            yield event.plain_result("获取群成员列表失败，请检查机器人权限。")
            return

        self_id = str(event.get_self_id() or "").strip()
        mention_ids: list[str] = []
        for member in members:
            if not isinstance(member, dict):
                continue
            user_id = str(member.get("user_id") or "").strip()
            if not user_id or user_id == self_id:
                continue
            mention_ids.append(user_id)

        mention_ids = list(dict.fromkeys(mention_ids))
        if not mention_ids:
            yield event.plain_result("当前群未找到可艾特成员。")
            return

        batch_size = max(1, self._get_int("all_members_mention_batch_size", 20))
        sent_cnt = 0
        batch_cnt = 0
        for uid_chunk in self._chunk_list(mention_ids, batch_size):
            chain = MessageChain()
            for uid in uid_chunk:
                chain.chain.append(At(qq=uid))
                chain.chain.append(Plain(text=" "))

            if tail_text and batch_cnt == 0:
                chain.chain.append(Plain(text=tail_text))

            try:
                await self.context.send_message(event.unified_msg_origin, chain)
                sent_cnt += len(uid_chunk)
                batch_cnt += 1
            except Exception as exc:
                logger.error("全体成员分批艾特发送失败 group_id=%s err=%s", group_id, exc)

        yield event.plain_result(
            f"已分批艾特 {sent_cnt} 位成员，共发送 {batch_cnt} 条消息。"
        )

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("设置全体艾特数量")
    async def set_all_members_mention_batch_size_command(self, event: AstrMessageEvent):
        payload = self._extract_command_payload(event, "设置全体艾特数量")
        size = self._extract_first_int(payload)
        if size is None or size <= 0:
            yield event.plain_result("用法：/设置全体艾特数量 数字（例如 /设置全体艾特数量 20）")
            return

        # 单条艾特过多会触发风控，这里给出安全上限。
        size = min(size, 50)
        self.config["all_members_mention_batch_size"] = size
        await self._persist_config()
        yield event.plain_result(f"已设置 /全体成员 每条艾特人数为 {size}。")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("踢未发言")
    async def kick_inactive_members_command(self, event: AstrMessageEvent):
        group_id = str(event.get_group_id() or "").strip()
        if not group_id:
            yield event.plain_result("请在群聊中使用：/踢未发言 天数")
            return

        payload = self._extract_command_payload(event, "踢未发言")
        days = self._extract_first_int(payload)
        if days is None or days <= 0:
            yield event.plain_result("用法：/踢未发言 天数（例如 /踢未发言 3）")
            return

        keep_days = max(1, self._get_int("speaker_stats_keep_days", self._daily_speaker_keep_days))
        if days > keep_days:
            yield event.plain_result(
                f"当前仅保留最近 {keep_days} 天发言记录，请先调大 speaker_stats_keep_days。"
            )
            return

        members = await self._get_group_member_list(event, group_id)
        if members is None:
            yield event.plain_result("获取群成员列表失败，请检查机器人权限。")
            return

        active_ids = await self._collect_recent_speakers(group_id, days)
        self_id = str(event.get_self_id() or "").strip()
        kick_user_whitelist = set(self._get_str_list("kick_user_whitelist"))

        target_ids: list[str] = []
        skipped_admin_cnt = 0
        skipped_whitelist_cnt = 0
        for member in members:
            if not isinstance(member, dict):
                continue

            user_id = str(member.get("user_id") or "").strip()
            if not user_id or user_id == self_id:
                continue
            if user_id in kick_user_whitelist:
                skipped_whitelist_cnt += 1
                continue

            role = str(member.get("role") or "member").lower()
            if role in {"owner", "admin"}:
                skipped_admin_cnt += 1
                continue

            if user_id in active_ids:
                continue

            target_ids.append(user_id)

        if not target_ids:
            yield event.plain_result(
                f"已检查完成：最近 {days} 天内所有可处理成员均有发言。"
                f"（跳过管理员/群主 {skipped_admin_cnt} 人，白名单 {skipped_whitelist_cnt} 人）"
            )
            return

        delay_minutes = max(1, self._get_int("kick_inactive_delay_minutes", 10))
        delay_seconds = delay_minutes * 60

        warn_text = (
            f"以下成员最近 {days} 天未发言，"
            f"将于 {delay_minutes} 分钟后执行移出；"
            f"若观察期内发言，将免于踢出。"
        )
        warn_chain = MessageChain()
        for uid in target_ids:
            warn_chain.chain.append(At(qq=uid))
            warn_chain.chain.append(Plain(text=" "))
        warn_chain.chain.append(Plain(text=warn_text))

        async with self._stats_lock:
            activity_baseline = {
                uid: self._msg_activity_by_group_user.get(group_id, {}).get(uid, 0)
                for uid in target_ids
            }

        try:
            await self.context.send_message(event.unified_msg_origin, warn_chain)
        except Exception as exc:
            logger.error("发送踢未发言预警失败 group_id=%s err=%s", group_id, exc)

        task = asyncio.create_task(
            self._execute_delayed_inactive_kick(
                unified_msg_origin=event.unified_msg_origin,
                group_id=group_id,
                days=days,
                target_ids=target_ids,
                activity_baseline=activity_baseline,
                delay_seconds=delay_seconds,
            )
        )
        self._track_kick_task(task)

        yield event.plain_result(
            f"已提醒 {len(target_ids)} 人，{delay_minutes} 分钟后执行踢出。"
            f"（范围: 最近 {days} 天，跳过管理员/群主 {skipped_admin_cnt} 人，"
            f"白名单 {skipped_whitelist_cnt} 人）"
        )

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("踢低等级")
    async def kick_low_level_command(self, event: AstrMessageEvent):
        """指令踢出低于指定群等级的成员（10分钟后执行）。"""
        group_id = str(event.get_group_id() or "").strip()
        if not group_id:
            yield event.plain_result("请在群聊中使用：/踢低等级 等级")
            return

        payload = self._extract_command_payload(event, "踢低等级")
        threshold = self._extract_first_int(payload)
        if threshold is None:
            threshold = self._get_int("kick_min_level", 1)

        if threshold <= 0:
            yield event.plain_result("等级阈值必须大于 0。")
            return

        members = await self._get_group_member_list(event, group_id)
        if members is None:
            yield event.plain_result("获取群成员列表失败，请检查机器人权限。")
            return

        self_id = str(event.get_self_id() or "").strip()
        kick_user_whitelist = set(self._get_str_list("kick_user_whitelist"))
        target_ids: list[str] = []
        skipped_admin_cnt = 0
        skipped_whitelist_cnt = 0

        for member in members:
            if not isinstance(member, dict):
                continue

            user_id = str(member.get("user_id") or "").strip()
            if not user_id or user_id == self_id:
                continue

            if user_id in kick_user_whitelist:
                skipped_whitelist_cnt += 1
                continue

            role = str(member.get("role") or "member").lower()
            if role in {"owner", "admin"}:
                skipped_admin_cnt += 1
                continue

            level = self._parse_member_level(member.get("level"))
            if level is None or level >= threshold:
                continue

            target_ids.append(user_id)

        if not target_ids:
            yield event.plain_result(
                f"已检查完成：没有低于 {threshold} 级的可踢成员。"
            )
            return

        delay_seconds = 10 * 60
        delay_minutes = delay_seconds // 60

        warn_text = (
            f"以下成员群等级低于 {threshold}，"
            f"将于 {delay_minutes} 分钟后执行移出；"
            f"若观察期内发言，将免于踢出。"
        )
        warn_chain = MessageChain()
        for uid in target_ids:
            warn_chain.chain.append(At(qq=uid))
            warn_chain.chain.append(Plain(text=" "))
        warn_chain.chain.append(Plain(text=warn_text))

        async with self._stats_lock:
            activity_baseline = {
                uid: self._msg_activity_by_group_user.get(group_id, {}).get(uid, 0)
                for uid in target_ids
            }

        try:
            await self.context.send_message(event.unified_msg_origin, warn_chain)
        except Exception as exc:
            logger.error("发送踢人预警失败 group_id=%s err=%s", group_id, exc)

        task = asyncio.create_task(
            self._execute_delayed_kick(
                unified_msg_origin=event.unified_msg_origin,
                group_id=group_id,
                threshold=threshold,
                target_ids=target_ids,
                activity_baseline=activity_baseline,
                delay_seconds=delay_seconds,
            )
        )
        self._track_kick_task(task)

        yield event.plain_result(
            f"已提醒 {len(target_ids)} 人，{delay_minutes} 分钟后执行踢出。"
            f"（跳过管理员/群主 {skipped_admin_cnt} 人，"
            f"白名单 {skipped_whitelist_cnt} 人）"
        )

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("立即执行")
    async def kick_low_level_now_command(self, event: AstrMessageEvent):
        """按低等级规则立即执行踢人，不等待冷却。"""
        group_id = str(event.get_group_id() or "").strip()
        if not group_id:
            yield event.plain_result("请在群聊中使用：/立即执行 等级")
            return

        payload = self._extract_command_payload(event, "立即执行")
        threshold = self._extract_first_int(payload)
        if threshold is None:
            threshold = self._get_int("kick_min_level", 1)

        if threshold <= 0:
            yield event.plain_result("等级阈值必须大于 0。")
            return

        members = await self._get_group_member_list(event, group_id)
        if members is None:
            yield event.plain_result("获取群成员列表失败，请检查机器人权限。")
            return

        self_id = str(event.get_self_id() or "").strip()
        kick_user_whitelist = set(self._get_str_list("kick_user_whitelist"))
        kicked_ids: list[str] = []
        failed_ids: list[str] = []
        skipped_admin_cnt = 0
        skipped_whitelist_cnt = 0

        for member in members:
            if not isinstance(member, dict):
                continue

            user_id = str(member.get("user_id") or "").strip()
            if not user_id or user_id == self_id:
                continue

            if user_id in kick_user_whitelist:
                skipped_whitelist_cnt += 1
                continue

            role = str(member.get("role") or "member").lower()
            if role in {"owner", "admin"}:
                skipped_admin_cnt += 1
                continue

            level = self._parse_member_level(member.get("level"))
            if level is None or level >= threshold:
                continue

            ok = await self._kick_group_member(event, group_id, user_id)
            if ok:
                kicked_ids.append(user_id)
            else:
                failed_ids.append(user_id)

        if not kicked_ids and not failed_ids:
            yield event.plain_result(
                f"已检查完成：没有低于 {threshold} 级的可踢成员。"
                f"（跳过管理员/群主 {skipped_admin_cnt} 人，"
                f"白名单 {skipped_whitelist_cnt} 人）"
            )
            return

        lines = [
            f"立即执行完成（阈值: {threshold}）",
            f"成功踢出: {len(kicked_ids)} 人",
            f"失败: {len(failed_ids)} 人",
            f"跳过管理员/群主: {skipped_admin_cnt} 人",
            f"跳过白名单: {skipped_whitelist_cnt} 人",
        ]
        if kicked_ids:
            lines.append("成功ID: " + ", ".join(kicked_ids[:20]))
        if failed_ids:
            lines.append("失败ID: " + ", ".join(failed_ids[:20]))
        yield event.plain_result("\n".join(lines))

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("提醒未发言")
    async def remind_silent_members_command(self, event: AstrMessageEvent):
        """艾特今天未发言成员，并附带自定义话术。"""
        group_id = str(event.get_group_id() or "").strip()
        if not group_id:
            yield event.plain_result("请在群聊中使用：/提醒未发言 自定义话术")
            return

        custom_text = self._extract_command_payload(event, "提醒未发言")
        remind_text = custom_text or self._get_text(
            "silent_reminder_text",
            "今天还没发言，记得来冒个泡哦~",
        )
        self_id = str(event.get_self_id() or "").strip()
        sender_id = str(event.get_sender_id() or "").strip()
        result = await self._send_silent_reminder_for_group(
            event=event,
            unified_msg_origin=event.unified_msg_origin,
            group_id=group_id,
            remind_text=remind_text,
            sender_id=sender_id,
            self_id=self_id,
        )

        if result is None:
            yield event.plain_result("获取群成员列表失败，请检查机器人权限。")
            return

        silent_total, reminded_cnt = result
        if silent_total <= 0:
            yield event.plain_result("今天全员都发言了，无需提醒。")
            return

        yield event.plain_result(
            f"提醒完成：未发言 {silent_total} 人，已发送提醒 {reminded_cnt} 人。"
        )

    async def _scheduled_sender_loop(self):
        while self._running:
            await asyncio.sleep(15)

            await self._process_schedule_message()
            await self._process_schedule_silent_reminder()

    async def _process_schedule_message(self) -> None:
        if not self._get_bool("enable_schedule", False):
            return

        should_send, fire_key = self._should_fire_daily_schedule()
        if not should_send:
            return

        self._last_schedule_fire_key = fire_key
        groups = self._get_str_list("schedule_group_whitelist")
        msg = self._get_text("schedule_message", "群通知：请文明发言，遵守群规。")
        if not groups or not msg:
            return

        at_all = self._get_bool("schedule_at_all", True)
        for group_id in groups:
            umo = self._group_umo_cache.get(group_id)
            if not umo:
                logger.warning(
                    "定时消息跳过，未找到群会话缓存 group_id=%s。请先让该群产生一条消息。",
                    group_id,
                )
                continue

            try:
                if at_all:
                    chain = MessageChain()
                    chain.chain.append(At(qq="all"))
                    chain.chain.append(Plain(text=f" {msg}"))
                    await self.context.send_message(umo, chain)
                else:
                    await self.context.send_message(umo, MessageChain().message(msg))
            except Exception as exc:
                logger.error(
                    "发送定时消息失败 group_id=%s err=%s",
                    group_id,
                    exc,
                )

    async def _process_schedule_silent_reminder(self) -> None:
        if not self._get_bool("enable_silent_reminder_schedule", False):
            return

        should_send, fire_key = self._should_fire_silent_reminder_schedule()
        if not should_send:
            return

        self._last_silent_reminder_fire_key = fire_key
        groups = self._get_str_list("silent_reminder_group_whitelist")
        if not groups:
            return

        remind_text = self._get_text(
            "silent_reminder_text",
            "今天还没发言，记得来冒个泡哦~",
        )
        for group_id in groups:
            umo = self._group_umo_cache.get(group_id)
            if not umo:
                logger.warning(
                    "定时未发言提醒跳过，未找到群会话缓存 group_id=%s。请先让该群产生一条消息。",
                    group_id,
                )
                continue

            result = await self._send_silent_reminder_for_group(
                event=None,
                unified_msg_origin=umo,
                group_id=group_id,
                remind_text=remind_text,
            )
            if result is None:
                logger.error("定时未发言提醒失败，无法获取成员列表 group_id=%s", group_id)
                continue

            silent_total, reminded_cnt = result
            logger.info(
                "定时未发言提醒完成 group_id=%s silent=%s reminded=%s",
                group_id,
                silent_total,
                reminded_cnt,
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

    def _build_welcome_message(
        self,
        group_id: str,
        user_id: str,
        notify_time: str,
    ) -> str:
        template = self._get_text(
            "welcome_template",
            "欢迎新成员 {user_id} 加入本群，当前时间：{time}。",
        )
        try:
            message = template.format(
                group_id=group_id,
                user_id=user_id,
                time=notify_time,
            )
            return self._ensure_time_visible(message, notify_time)
        except Exception:
            logger.warning("welcome_template 格式错误，已回退到默认欢迎语。")
            return f"欢迎新成员 {user_id} 加入本群。\n当前时间：{notify_time}。"

    def _build_leave_message(
        self,
        group_id: str,
        user_id: str,
        operator_id: str,
        sub_type: str,
        notify_time: str,
    ) -> str:
        template = self._get_text(
            "leave_template",
            "成员 {user_id} 已离开本群，当前时间：{time}。",
        )
        try:
            message = template.format(
                group_id=group_id,
                user_id=user_id,
                operator_id=operator_id,
                sub_type=sub_type,
                time=notify_time,
            )
            return self._ensure_time_visible(message, notify_time)
        except Exception:
            logger.warning("leave_template 格式错误，已回退到默认退群通知。")
            return f"成员 {user_id} 已离开本群。\n当前时间：{notify_time}。"

    async def _send_group_notice(
        self,
        unified_msg_origin: str,
        text: str,
        image_source: str,
        at_user_id: str = "",
    ) -> None:
        chain = self._build_notice_chain(
            text=text,
            image_source=image_source,
            at_user_id=at_user_id,
        )
        if not chain.chain:
            return
        await self.context.send_message(unified_msg_origin, chain)

    def _build_notice_chain(
        self,
        text: str,
        image_source: str,
        at_user_id: str = "",
    ) -> MessageChain:
        chain = MessageChain()

        at_user_id = str(at_user_id or "").strip()
        if at_user_id:
            chain.chain.append(At(qq=at_user_id))

        if text:
            plain_text = text if not at_user_id else f" {text}"
            chain.chain.append(Plain(text=plain_text))

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
    def _ensure_time_visible(message: str, notify_time: str) -> str:
        text = QunHelperPlugin._normalize_notice_text(str(message or ""))
        if not text:
            return f"当前时间：{notify_time}"

        # 兼容历史模板：如果模板未包含时间占位符，统一在末尾补充时间
        if notify_time not in text:
            return f"{text}\n当前时间：{notify_time}"

        return text

    @staticmethod
    def _normalize_notice_text(raw_text: str) -> str:
        text = str(raw_text or "")
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        # 兼容用户在配置中输入字面量 \n / \r\n
        text = text.replace("\\r\\n", "\n").replace("\\n", "\n").replace("\\r", "\n")

        lines = [line.rstrip() for line in text.split("\n")]
        while lines and not lines[0].strip():
            lines.pop(0)
        while lines and not lines[-1].strip():
            lines.pop()
        return "\n".join(lines)

    @staticmethod
    def _format_now_str() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    async def _get_member_level(
        self,
        event: AstrMessageEvent | None,
        group_id: str,
        user_id: str,
    ) -> int | None:
        result = await self._call_aiocqhttp_action(
            event,
            "get_group_member_info",
            group_id=int(group_id) if group_id.isdigit() else group_id,
            user_id=int(user_id) if user_id.isdigit() else user_id,
            no_cache=True,
        )
        data = self._extract_action_data(result)
        if not isinstance(data, dict):
            return None

        return self._parse_member_level(data.get("level"))

    async def _kick_group_member(
        self,
        event: AstrMessageEvent | None,
        group_id: str,
        user_id: str,
    ) -> bool:
        result = await self._call_aiocqhttp_action(
            event,
            "set_group_kick",
            group_id=int(group_id) if group_id.isdigit() else group_id,
            user_id=int(user_id) if user_id.isdigit() else user_id,
            reject_add_request=False,
        )

        if result is None:
            logger.error("踢人失败，调用 set_group_kick 无返回。group_id=%s user_id=%s", group_id, user_id)
            return False

        if isinstance(result, dict):
            status = str(result.get("status") or "").lower()
            if status and status != "ok":
                logger.error("踢人失败 group_id=%s user_id=%s ret=%s", group_id, user_id, result)
                return False

        return True

    async def _call_aiocqhttp_action(
        self,
        event: AstrMessageEvent | None,
        action: str,
        **kwargs,
    ) -> Any:
        bot = getattr(event, "bot", None) if event else None

        try:
            if bot and callable(getattr(bot, "call_action", None)):
                return await bot.call_action(action, **kwargs)

            api = getattr(bot, "api", None) if bot else None
            if api and callable(getattr(api, "call_action", None)):
                return await api.call_action(action, **kwargs)

            platform = self.context.get_platform(filter.PlatformAdapterType.AIOCQHTTP)
            get_client = getattr(platform, "get_client", None)
            client = get_client() if callable(get_client) else None
            if client and callable(getattr(client, "call_action", None)):
                return await client.call_action(action, **kwargs)

            client_api = getattr(client, "api", None) if client else None
            if client_api and callable(getattr(client_api, "call_action", None)):
                return await client_api.call_action(action, **kwargs)
        except Exception as exc:
            logger.error("调用 OneBot API 失败 action=%s err=%s", action, exc)

        return None

    @staticmethod
    def _extract_action_data(result: Any) -> Any:
        if isinstance(result, dict) and "data" in result:
            return result.get("data")
        return result

    @staticmethod
    def _parse_member_level(value: Any) -> int | None:
        if isinstance(value, int):
            return value

        text = str(value or "").strip()
        if not text:
            return None

        if text.isdigit():
            return int(text)

        digits = "".join(ch for ch in text if ch.isdigit())
        if not digits:
            return None
        return int(digits)

    @staticmethod
    def _extract_first_int(text: str) -> int | None:
        content = str(text or "").strip()
        if not content:
            return None

        first = content.split()[0]
        if first.isdigit():
            return int(first)
        return None

    def _get_int(self, key: str, default: int) -> int:
        value = self.config.get(key, default)
        try:
            return int(value)
        except Exception:
            return default

    async def _get_group_member_list(
        self,
        event: AstrMessageEvent | None,
        group_id: str,
    ) -> list[dict[str, Any]] | None:
        result = await self._call_aiocqhttp_action(
            event,
            "get_group_member_list",
            group_id=int(group_id) if group_id.isdigit() else group_id,
        )
        data = self._extract_action_data(result)
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        return None

    @staticmethod
    def _today_key() -> str:
        return datetime.now().date().isoformat()

    def _cleanup_daily_speaker_cache_locked(self) -> None:
        self._daily_speaker_keep_days = max(
            1,
            self._get_int("speaker_stats_keep_days", self._daily_speaker_keep_days),
        )
        today = datetime.now().date()
        expired_keys: list[str] = []
        for key in self._daily_speakers_by_group_day.keys():
            try:
                _, date_text = key.rsplit(":", 1)
                day = datetime.strptime(date_text, "%Y-%m-%d").date()
                if (today - day).days > self._daily_speaker_keep_days:
                    expired_keys.append(key)
            except Exception:
                expired_keys.append(key)

        for key in expired_keys:
            self._daily_speakers_by_group_day.pop(key, None)

    @staticmethod
    def _chunk_list(items: list[str], size: int):
        for i in range(0, len(items), size):
            yield items[i : i + size]

    async def _send_silent_reminder_for_group(
        self,
        event: AstrMessageEvent | None,
        unified_msg_origin: str,
        group_id: str,
        remind_text: str,
        sender_id: str = "",
        self_id: str = "",
    ) -> tuple[int, int] | None:
        members = await self._get_group_member_list(event, group_id)
        if members is None:
            return None

        today_key = self._today_key()
        async with self._stats_lock:
            spoken = set(self._daily_speakers_by_group_day.get(f"{group_id}:{today_key}", set()))
            if sender_id:
                spoken.add(sender_id)

        silent_ids: list[str] = []
        for member in members:
            if not isinstance(member, dict):
                continue
            user_id = str(member.get("user_id") or "").strip()
            if not user_id:
                continue
            if self_id and user_id == self_id:
                continue
            if user_id in spoken:
                continue
            silent_ids.append(user_id)

        if not silent_ids:
            return 0, 0

        batch_size = max(1, self._get_int("silent_reminder_batch_size", 20))
        reminded_cnt = 0
        for uid_chunk in self._chunk_list(silent_ids, batch_size):
            chain = MessageChain()
            for uid in uid_chunk:
                chain.chain.append(At(qq=uid))
                chain.chain.append(Plain(text=" "))
            chain.chain.append(Plain(text=remind_text))

            try:
                await self.context.send_message(unified_msg_origin, chain)
                reminded_cnt += len(uid_chunk)
            except Exception as exc:
                logger.error("提醒未发言发送失败 group_id=%s err=%s", group_id, exc)

        return len(silent_ids), reminded_cnt

    async def _collect_recent_speakers(self, group_id: str, days: int) -> set[str]:
        if days <= 0:
            return set()

        keys = [
            f"{group_id}:{(datetime.now().date() - timedelta(days=offset)).isoformat()}"
            for offset in range(days)
        ]
        async with self._stats_lock:
            result: set[str] = set()
            for key in keys:
                result.update(self._daily_speakers_by_group_day.get(key, set()))
            return result

    def _track_kick_task(self, task: asyncio.Task) -> None:
        self._pending_kick_tasks.add(task)

        def _cleanup(done_task: asyncio.Task) -> None:
            self._pending_kick_tasks.discard(done_task)
            if done_task.cancelled():
                return
            try:
                exc = done_task.exception()
                if exc:
                    logger.error("延时踢人任务异常 err=%s", exc)
            except Exception:
                pass

        task.add_done_callback(_cleanup)

    async def _execute_delayed_kick(
        self,
        unified_msg_origin: str,
        group_id: str,
        threshold: int,
        target_ids: list[str],
        activity_baseline: dict[str, int],
        delay_seconds: int,
    ) -> None:
        try:
            await asyncio.sleep(delay_seconds)
        except asyncio.CancelledError:
            return

        kicked_ids: list[str] = []
        failed_ids: list[str] = []
        recovered_cnt = 0
        missing_cnt = 0
        spoke_skip_cnt = 0
        whitelist_skip_cnt = 0

        async with self._stats_lock:
            activity_now = dict(self._msg_activity_by_group_user.get(group_id, {}))

        kick_user_whitelist = set(self._get_str_list("kick_user_whitelist"))

        dedup_ids = list(dict.fromkeys(target_ids))
        for user_id in dedup_ids:
            if user_id in kick_user_whitelist:
                whitelist_skip_cnt += 1
                continue

            baseline = int(activity_baseline.get(user_id, 0))
            current = int(activity_now.get(user_id, 0))
            if current > baseline:
                spoke_skip_cnt += 1
                continue

            level = await self._get_member_level(None, group_id, user_id)
            if level is None:
                missing_cnt += 1
                continue

            if level >= threshold:
                recovered_cnt += 1
                continue

            ok = await self._kick_group_member(None, group_id, user_id)
            if ok:
                kicked_ids.append(user_id)
            else:
                failed_ids.append(user_id)

        lines = [
            f"踢低等级执行完成（阈值: {threshold}）",
            f"成功踢出: {len(kicked_ids)} 人",
            f"失败: {len(failed_ids)} 人",
            f"白名单免踢: {whitelist_skip_cnt} 人",
            f"观察期发言免踢: {spoke_skip_cnt} 人",
            f"已达标无需踢出: {recovered_cnt} 人",
            f"未找到成员信息: {missing_cnt} 人",
        ]
        if kicked_ids:
            lines.append("成功ID: " + ", ".join(kicked_ids[:20]))
        if failed_ids:
            lines.append("失败ID: " + ", ".join(failed_ids[:20]))

        try:
            await self.context.send_message(
                unified_msg_origin,
                MessageChain().message("\n".join(lines)),
            )
        except Exception as exc:
            logger.error("发送踢人执行结果失败 group_id=%s err=%s", group_id, exc)

    async def _execute_delayed_inactive_kick(
        self,
        unified_msg_origin: str,
        group_id: str,
        days: int,
        target_ids: list[str],
        activity_baseline: dict[str, int],
        delay_seconds: int,
    ) -> None:
        try:
            await asyncio.sleep(delay_seconds)
        except asyncio.CancelledError:
            return

        kicked_ids: list[str] = []
        failed_ids: list[str] = []
        spoke_skip_cnt = 0
        whitelist_skip_cnt = 0

        async with self._stats_lock:
            activity_now = dict(self._msg_activity_by_group_user.get(group_id, {}))

        kick_user_whitelist = set(self._get_str_list("kick_user_whitelist"))
        for user_id in list(dict.fromkeys(target_ids)):
            if user_id in kick_user_whitelist:
                whitelist_skip_cnt += 1
                continue

            baseline = int(activity_baseline.get(user_id, 0))
            current = int(activity_now.get(user_id, 0))
            if current > baseline:
                spoke_skip_cnt += 1
                continue

            ok = await self._kick_group_member(None, group_id, user_id)
            if ok:
                kicked_ids.append(user_id)
            else:
                failed_ids.append(user_id)

        lines = [
            f"踢未发言执行完成（范围: 最近 {days} 天）",
            f"成功踢出: {len(kicked_ids)} 人",
            f"失败: {len(failed_ids)} 人",
            f"白名单免踢: {whitelist_skip_cnt} 人",
            f"观察期发言免踢: {spoke_skip_cnt} 人",
        ]
        if kicked_ids:
            lines.append("成功ID: " + ", ".join(kicked_ids[:20]))
        if failed_ids:
            lines.append("失败ID: " + ", ".join(failed_ids[:20]))

        try:
            await self.context.send_message(
                unified_msg_origin,
                MessageChain().message("\n".join(lines)),
            )
        except Exception as exc:
            logger.error("发送踢未发言执行结果失败 group_id=%s err=%s", group_id, exc)

    async def _start_rank_server(self) -> None:
        if self._rank_app_runner is not None:
            return

        port = self._get_int("rank_server_port", 16666)
        if port <= 0:
            port = 16666

        app = web.Application()
        app.router.add_get("/", self._handle_rank_index)
        app.router.add_get("/api/rankings", self._handle_rank_api)

        runner = web.AppRunner(app)
        try:
            await runner.setup()
            site = web.TCPSite(runner, "0.0.0.0", port)
            await site.start()
            self._rank_app_runner = runner
            self._rank_site = site
            logger.info("消息排行榜服务已启动: http://0.0.0.0:%s", port)
        except Exception as exc:
            logger.error("启动消息排行榜服务失败 port=%s err=%s", port, exc)
            try:
                await runner.cleanup()
            except Exception:
                pass

    async def _stop_rank_server(self) -> None:
        if self._rank_app_runner is None:
            return

        try:
            await self._rank_app_runner.cleanup()
        except Exception as exc:
            logger.error("关闭消息排行榜服务失败 err=%s", exc)
        finally:
            self._rank_site = None
            self._rank_app_runner = None

    async def _handle_rank_api(self, _request: web.Request) -> web.Response:
        snapshot = await self._build_rank_snapshot()
        return web.json_response(snapshot)

    async def _handle_rank_index(self, _request: web.Request) -> web.Response:
        snapshot = await self._build_rank_snapshot()
        html = self._render_rank_html(snapshot)
        return web.Response(text=html, content_type="text/html", charset="utf-8")

    async def _build_rank_snapshot(self) -> dict[str, Any]:
        async with self._stats_lock:
            by_group = {gid: dict(data) for gid, data in self._msg_by_group_user.items()}
            names = dict(self._user_name_cache)

        rank_whitelist = set(self._get_str_list("rank_group_whitelist"))
        if rank_whitelist:
            by_group = {
                gid: data for gid, data in by_group.items() if gid in rank_whitelist
            }

        total: dict[str, int] = defaultdict(int)
        for group_data in by_group.values():
            for user_id, cnt in group_data.items():
                total[user_id] += cnt

        total_rows = sorted(total.items(), key=lambda item: item[1], reverse=True)
        total_top = [
            {
                "rank": idx,
                "user_id": user_id,
                "user_name": names.get(user_id, user_id),
                "count": count,
            }
            for idx, (user_id, count) in enumerate(total_rows[:100], start=1)
        ]

        group_top: dict[str, list[dict[str, Any]]] = {}
        for group_id, group_data in by_group.items():
            rows = sorted(group_data.items(), key=lambda item: item[1], reverse=True)
            group_top[group_id] = [
                {
                    "rank": idx,
                    "user_id": user_id,
                    "user_name": names.get(user_id, user_id),
                    "count": count,
                }
                for idx, (user_id, count) in enumerate(rows[:50], start=1)
            ]

        return {
            "generated_at": self._format_now_str(),
            "total_top": total_top,
            "group_top": group_top,
        }

    def _render_rank_html(self, snapshot: dict[str, Any]) -> str:
        generated_at = escape(str(snapshot.get("generated_at", "")))
        total_top = snapshot.get("total_top", [])
        group_top = snapshot.get("group_top", {})

        total_rows_html = "".join(
            (
                "<tr>"
                f"<td>{row.get('rank')}</td>"
                f"<td>{escape(str(row.get('user_name', '')))}</td>"
                f"<td>{escape(str(row.get('user_id', '')))}</td>"
                f"<td>{row.get('count')}</td>"
                "</tr>"
            )
            for row in total_top
        )
        if not total_rows_html:
            total_rows_html = "<tr><td colspan='4'>暂无数据</td></tr>"

        group_sections: list[str] = []
        for group_id in sorted(group_top.keys()):
            rows = group_top[group_id]
            rows_html = "".join(
                (
                    "<tr>"
                    f"<td>{row.get('rank')}</td>"
                    f"<td>{escape(str(row.get('user_name', '')))}</td>"
                    f"<td>{escape(str(row.get('user_id', '')))}</td>"
                    f"<td>{row.get('count')}</td>"
                    "</tr>"
                )
                for row in rows
            )
            if not rows_html:
                rows_html = "<tr><td colspan='4'>暂无数据</td></tr>"

            group_sections.append(
                "<section>"
                f"<h2>群 {escape(group_id)} 排行榜</h2>"
                "<table>"
                "<thead><tr><th>排名</th><th>昵称</th><th>用户ID</th><th>消息数</th></tr></thead>"
                f"<tbody>{rows_html}</tbody>"
                "</table>"
                "</section>"
            )

        group_html = "".join(group_sections) if group_sections else "<p>暂无群排行数据</p>"

        return (
            "<!doctype html>"
            "<html lang='zh-CN'><head><meta charset='utf-8'>"
            "<meta name='viewport' content='width=device-width, initial-scale=1'>"
            "<title>消息排行榜</title>"
            "<style>body{font-family:Segoe UI,Arial,sans-serif;margin:24px;background:#f6f8fb;color:#1f2328;}"
            "h1,h2{margin:0 0 12px 0;}"
            "p{margin:8px 0 16px 0;color:#57606a;}"
            "table{width:100%;border-collapse:collapse;background:#fff;margin-bottom:20px;border-radius:8px;overflow:hidden;}"
            "th,td{padding:10px 12px;border-bottom:1px solid #d0d7de;text-align:left;}"
            "th{background:#f3f4f6;font-weight:600;}"
            "tr:last-child td{border-bottom:none;}"
            "section{margin-top:20px;}"
            "</style></head><body>"
            "<h1>消息排行榜</h1>"
            f"<p>更新时间：{generated_at} ｜ API: /api/rankings</p>"
            "<section><h2>总消息榜 TOP 100</h2>"
            "<table><thead><tr><th>排名</th><th>昵称</th><th>用户ID</th><th>消息数</th></tr></thead>"
            f"<tbody>{total_rows_html}</tbody></table></section>"
            f"{group_html}"
            "</body></html>"
        )

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

    def _should_fire_silent_reminder_schedule(self) -> tuple[bool, str]:
        schedule_time = self._get_text("silent_reminder_daily_time", "21:00")
        parsed = self._parse_daily_time(schedule_time)
        if parsed is None:
            parsed = (21, 0)

        now = datetime.now()
        hour, minute = parsed
        fire_key = f"{now.date().isoformat()}-{hour:02d}:{minute:02d}"

        if self._last_silent_reminder_fire_key == fire_key:
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
