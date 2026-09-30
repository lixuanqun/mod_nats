# mod_nats 本地全链路与压测记录

## 2026-09-30 v0.4.0：所有权租约全链路

日期：2026-09-30。环境：`examples/docker-lab` compose（nats + fs + 消费端），fs 镜像由本仓库 Dockerfile.fs 从 `aicc-fs:local-verify` 构建，模块即本仓库源码。测试配置：`node-uuid=test-node-01`、`allow-native-api=true`（selftest 用 originate 建通道）、`owner-lease-ttl=4`、`metrics-interval=1`、`channel-params=hangup_cause,test_var`。呼叫路径 `loopback/9196 &park`（镜像 dialplan 的 echo 应答）。selftest 经 `MODNATS_URL` / `MODNATS_DEST` 环境变量指向 lab 网络。

**60/60 通过**，其中租约专项：

| 用例 | 结果 |
|------|------|
| Accept(D) 起租 | 200 |
| `XNode.Touch` / `fs.channel.touch` | 200，result 带 `lease_ttl=4` |
| owner 静默 4s 后 `Event.OwnerLost` | 在 `event.ownerlost` 公共主题与原 owner 信箱都收到 |
| 到期后原 owner `GetState` | 400 `channel not accepted`（绑定已释放） |
| 备用控制器重新 Accept | 200，取得控制权 |
| 备用控制器 Touch / Hangup | 均 200 |
| `nats status` | 新增 `owner lease ttl=Ns` 行 |
| 3 × `reload mod_nats`（ttl=4 生效中） | 连接每次回到 UP，无崩溃 |

同一轮里 Dial 的 202/job_uuid/`Event.Result` 链路、bridge、CDR、信箱路由全部通过。本轮同时修正了 selftest 相对 v0.3 语义的漂移：客户端默认注入 `ctrl_uuid`（与 xctrl SDK 一致）、未知 uuid 在所有权门禁下报 400 而非 404、RINGING/MEDIA 降级为提示信息。

## 2026-09-29：v0.3.1 全链路与压测

## 环境

- FreeSWITCH 1.11.2（`aicc-fs:local-verify`，Debian 12，glibc 2.36）
- `mod_nats.so` 与 `mod_loopback.so` 在该镜像内、对着镜像自带的头文件编译
- libnats：nats.c 3.11.0，未启用 TLS
- NATS：JetStream 打开。正式这一轮把服务端 `max_payload` 调到 4MB，否则默认 1MB 会在请求到达模块之前被客户端拒绝
- 节点：`node-uuid=fs-lab-1`，前缀 `nats.fs.`，8 个 worker，`req-qsize=8192`，`pub-qsize=16384`
- 独立消费进程只订阅 `nats.fs.>`
- 呼叫路径：`loopback/park`（同一进程内的两条腿）

## 功能测试

21/21 通过。

| 用例 | 结果 |
|------|------|
| `XNode.JStatus`，数字 id 原样返回 | 200，`id` 是 JSON 数字 |
| 未知方法 | 501 |
| 缺少 `uuid` | 400 |
| Accept 不存在的通道 | 404 |
| NativeAPI 默认关闭 | 403 `native api disabled` |
| 超过 1MiB 的请求 | 400 `payload too large` |
| 非法 JSON | 400 `invalid JSON payload` |
| Dial 含 `execute_on_*` | 400 |
| 未 Accept 就 Hangup | 400 `channel not accepted` |
| Accept | 200 |
| 其他控制器再次 Accept | 419 |
| Observe | 200，不抢控制权 |
| `GetState.answer_state` | `ACTIVE`，不是 `CS_*` |
| SetVar / GetVar | `nats_lab=ok` |
| Play | 200 |
| NativeApp | 200，表示已排队 |
| Hangup 已接管通道 | 200 |
| Dial A / Dial B | 均 202，随后 `Event.Result` code=200，uuid 与预留值一致 |
| Bridge 两条已接管通道 | 200 |
| 消费端收到 `Event.CDR` | 本轮 CDR 增加 54，与 JetStream 流中的 54 条一致 |

默认 `max_payload=1MB` 时，超限请求在客户端报 `maximum payload exceeded`，到不了模块。调到 4MB 后，模块自己返回 400。

## 压测

正式一轮：2026-09-29 16:05–16:06（UTC+8）。

| 项目 | 结果 |
|------|------|
| JStatus | 32 路并发，15 秒，144,075 次全部成功，约 9,602 次/秒 |
| JStatus 延迟 | p50 3.1 ms，p95 5.0 ms，p99 6.4 ms，最大 41 ms |
| Dial | 4 路并发，24/24 成功，约 108 通/秒 |
| Dial 到 `Event.Result` | p50 33 ms，p99 49 ms |
| 结束后 `nats status` | `pub errors=0`，`fallbacks=0`，pub/js/reply/event/req 队列都是 0 |

`dropped=1` 是那条超限请求，模块按设计计数丢掉。更早一轮在机器更空时，JStatus 约 13,772 次/秒，p99 4.0 ms，同样 0 错误。108 通/秒是进程内 loopback，不能当成外部 SIP 的容量。

消费端在这一轮看到的 JStatus 请求增量为 144,077，与成功响应 144,075 相差 2，落在消费端每秒落盘的窗口里。`Event.Result` 增加 26（功能拨号 2 次加压测 24 次）。JetStream metrics 流在约 18 秒内收到 4 条，与 5 秒心跳一致。

替换 NATS 容器后连接先变为 DOWN，JetStream 队列一度积到大约 22 条，随后在没有新增发布错误的情况下排空。测试结束后会话用 `hupall` 清回 0。
