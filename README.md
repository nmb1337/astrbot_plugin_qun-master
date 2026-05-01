# 群欢迎与定时消息插件

这是一个 AstrBot 插件，提供两个功能：

1. 新成员入群欢迎（仅 OneBot v11 / aiocqhttp）。
2. 群内定时发送固定消息。

## 功能特性

- 支持独立开关：可以单独开启或关闭入群欢迎。
- 支持群号白名单：只在指定群里触发欢迎或定时消息。
- 防刷屏：仅在真实入群事件触发欢迎，并做短时间去重。
- 不调用 LLM：欢迎语和定时消息均为固定模板，不消耗模型 token。
- 错误保护：发送失败会捕获异常并记录日志，不会导致插件崩溃。

## 配置项

插件配置由 `_conf_schema.json` 定义，可在 AstrBot WebUI 中配置：

- `enable_welcome`：是否启用入群欢迎。
- `welcome_group_whitelist`：允许触发欢迎的群号白名单。
- `welcome_template`：欢迎语模板，支持 `{user_id}` 和 `{group_id}`。
- `enable_schedule`：是否启用定时发送。
- `schedule_group_whitelist`：允许接收定时消息的群号白名单。
- `schedule_interval_seconds`：定时发送间隔（秒）。
- `schedule_message`：定时发送的固定文本。

## 注意事项

- 定时消息依赖群会话缓存。插件需要先收到该群至少一条消息，才能拿到可用的会话标识并主动发送。
- 入群欢迎事件判断基于 OneBot 的 `notice_type=group_increase`。
- 如果机器人在群内被禁言或权限不足，发送会失败并写日志。
