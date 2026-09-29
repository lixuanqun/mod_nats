# mod_nats 模块设计文档

NATS 消息总线集成模块——为 FreeSWITCH 提供基于 NATS 的呼叫控制面（XCC 协议兼容）、事件流与 CDR 发布。目标是替代 ESL 成为外部系统集成的正门，实现**媒体与控制分层**。

## 1. 架构

```
┌───────────────────────────────┐            ┌────────────────────────────┐
│  FreeSWITCH (媒体节点)         │            │  外部控制器 (XCtrl)         │
│                               │   NATS     │  Java / Go / ...           │
│  RTP / 录音 / ASR/TTS(媒体面) │◄──────────►│  只收发控制消息,不碰媒体     │
│  mod_nats   (控制面)          │  JSON-RPC  │                            │
└───────────────────────────────┘            └────────────────────────────┘
        │                                               
        └── mod_event_socket 保持不动,仅作 fs_cli 运维通道
```

模块分层：

| 文件 | 职责 |
|------|------|
| `mod_nats.c` | 加载/卸载、配置解析、请求工作线程、`nats` CLI 命令 |
| `nats_conn.c` | cnats 连接管理（自动重连）、订阅分发、发布队列 |
| `nats_proto.c` | JSON-RPC 2.0 信封、方法路由、响应组装 |
| `nats_methods.c` | XNode.* / fs.* 方法、Accept 与 observer 绑定、异步 Dial |
| `nats_events.c` | Event.Channel / Event.CDR / Event.Result 事件发布、observer 信箱 |
| `schema/catalog.json` | 线上 JSON Schema 目录（方法入参与事件） |

线程模型：cnats 回调线程只做拷贝入队（永不阻塞）；N 个请求工作线程执行 FS 调用；单发布线程排空发布队列，**队列满则丢弃并计数**（事件流 at-most-once，关键数据走 CDR/JetStream）。

## 2. Subject 布局

前缀可配置（`subject-prefix`），默认 `nats.fs.`。

| Subject | 方向 | 用途 |
|---------|------|------|
| `{prefix}node.{node_uuid}` | 控制器 → FS | JSON-RPC 请求入口 |
| `{prefix}ctrl.{ctrl_uuid}` | FS → 控制器 | 控制者信箱，以及 `fs.channel.observe` 观察者信箱：通道事件、Event.Result |
| `{prefix}event.channel.{STATE}` | FS → 广播 | 通道状态事件。`event-routing=mailbox` 且通道已被 Accept 时不发 |
| `{prefix}event.{event_name}` | FS → 广播 | 原生事件转发（默认关闭） |
| `{prefix}event.cdr` | FS → 广播 | CDR（建议服务端配 JetStream stream 持久化） |

**SDK 兼容性**：官方 xctrl Go SDK（`xswitch-cn/xctrl`）将 `cn.xswitch.` 前缀硬编码在 `ctrl/ctrl.go` 中，无配置接口。两种用法：
- 把本模块 `subject-prefix` 配成 `cn.xswitch.` → 官方 Go/Java SDK 即插即用；
- 坚持自有前缀 `nats.fs.` → fork SDK 改前缀（Apache 2.0/MIT 允许），或自研客户端。

## 3. 协议

XCC 风格 JSON-RPC 2.0（信封规范见 https://docs.xswitch.cn/xcc-api/design/ ，消息结构见 xswitch-cn/proto 的 xctrl.proto）：

```json
// 请求（控制器 → {prefix}node.{node_uuid}）
{"jsonrpc":"2.0","id":"call-1","method":"XNode.Answer",
 "params":{"ctrl_uuid":"my-ctrl","uuid":"efed526f-..."}}

// 响应（FS → 请求的 reply inbox）
{"jsonrpc":"2.0","id":"call-1",
 "result":{"code":200,"message":"OK","node_uuid":"..."}}

// 事件通知（无 id）
{"jsonrpc":"2.0","method":"Event.Channel",
 "params":{"node_uuid":"...","uuid":"...","state":"ANSWERED","caller_id_number":"1000",...}}
```

result.code 语义：200 成功 / 202 已受理（结果走 Event.Result）/ 400 拒绝 / 404 通道不存在 / 419 已被其他控制器接管 / 500 内部错误 / 501 未实现。

### 已实现方法（v0.1）

| 方法 | 说明 |
|------|------|
| XNode.Accept | 控制器接管通道（首个成功者获得控制权，后续返回 419） |
| fs.channel.observe / unobserve | 观察者：不获得控制权。`event-routing` 为 `mailbox` 或 `both` 时，事件进入该 ctrl 信箱。每通道最多 32 个 |
| XNode.Answer / Hangup | 应答 / 挂机。通道已有 owner 时，`ctrl_uuid` 必须是 owner，否则 419 |
| XNode.Play / Stop / Broadcast | 放音 / 停止放音 / 广播媒体 |
| XNode.Bridge / ChannelBridge | 桥接两条通道 |
| XNode.SetVar / GetVar / GetState / GetChannelData | 变量与状态读写 |
| XNode.Dial | 外呼（bgapi originate，立即回 202 + job_uuid，结果走 Event.Result） |
| XNode.JStatus | 节点状态：sessions/peak/sps/uptime/version |
| XNode.NativeApp / NativeAPI / NativeJSAPI | 逃生舱：任意 dialplan app / fs API / JSON API |
| Event.Channel / Event.CDR / Event.Result | 事件与异步结果 |

**未实现（规划中）**：UnBridge2、Transfer、Hold、ThreeWay、Mute、ReadDTMF、DetectSpeech（ASR，可对接 mod_ws_audio）、Record、Conference 系列、MediaFork——过渡期均可通过 NativeApp/NativeAPI 透传实现。

## 4. 配置（conf/autoload_configs/nats.conf.xml）

```xml
<configuration name="nats.conf" description="NATS Bus">
  <settings>
    <!-- 逗号分隔多 URL，客户端自动故障切换 -->
    <param name="urls" value="nats://127.0.0.1:4222,nats://127.0.0.1:4223"/>
    <!-- subject 前缀；要即插即用官方 xctrl SDK 改为 cn.xswitch. -->
    <param name="subject-prefix" value="nats.fs."/>
    <!-- 节点 ID，留空则每次加载生成随机 UUID（生产建议固定） -->
    <param name="node-uuid" value="fs-node-01"/>
    <!-- 认证三选一：账号密码 / JWT credentials 文件 -->
    <param name="user" value="fs"/>
    <param name="password" value="secret"/>
    <!-- <param name="credentials" value="/etc/freeswitch/nats.creds"/> -->
    <param name="publish-events" value="true"/>
    <param name="enable-cdr" value="true"/>
    <!-- JetStream：流在 nats-server 上预先建好，主题与下面 subject 一致。需要 nats.c >= 3.0 -->
    <param name="js-cdr" value="false"/>
    <param name="js-metrics" value="false"/>
    <!-- broadcast=只发公共主题；mailbox=已接管通道只发信箱；both=两者都发（默认） -->
    <param name="event-routing" value="both"/>
    <!-- 转发全部原生 FS 事件（默认关，高 CPS 节点慎开） -->
    <param name="publish-native-events" value="false"/>
    <!-- 事件附加通道变量白名单（逗号分隔） -->
    <param name="channel-params" value="hangup_cause,sip_from_user"/>
    <!-- 独立 CDR subject（默认 {prefix}event.cdr） -->
    <param name="cdr-subject" value=""/>
    <param name="workers" value="4"/>
    <param name="pub-qsize" value="8192"/>
    <param name="req-qsize" value="2048"/>
  </settings>
</configuration>
```

CLI：

```
fsctl> nats status    # 连接状态/收发计数/丢弃计数/会话数
fsctl> nats reload    # 重读配置
```

## 5. 编译

依赖：[nats.c](https://github.com/nats-io/nats.c) >= 3.0（JetStream API 在 `nats.h` 中，没有单独的 `NATS_HAS_JETSTREAM` 宏）。启用 TLS 时加 `-DNATS_HAS_TLS` 构建 libnats。本模块通过 pkg-config（`nats.pc`）发现依赖。

```bash
# 安装 libnats（Linux 示例）
cmake -B build -S . -DNATS_BUILD_WITH_TLS=OFF && cmake --build build && cmake --install build

# FreeSWITCH 侧
sed -i 's/#event_handlers\/mod_nats/event_handlers\/mod_nats/' build/modules.conf.in
./configure   # 重新生成（新增了 NATS pkg-config 检测，需先 autoreconf -i）
make mod_nats
make mod_nats-install
```

Windows：用 vcpkg（`vcpkg install nats.c`）或 CMake 自行构建 nats.c，工程文件待补充（.vcxproj）。

## 6. 快速联调

```bash
# 1. 起 NATS server
nats-server -p 4222

# 2. fs 里 load mod_nats，确认订阅建立
fs_cli -x "nats status"

# 3. 用 nats CLI 发一个 JStatus 请求
nats request 'nats.fs.node.<node_uuid>' \
  '{"jsonrpc":"2.0","id":"t1","method":"XNode.JStatus","params":{}}'

# 4. 订阅事件
nats sub 'nats.fs.event.channel.>'
nats sub 'nats.fs.event.cdr'

# 5. 可选：把 CDR / metrics 交给 JetStream（流要先在服务端建好）
nats stream add FS_CDR --subjects 'nats.fs.event.cdr' --storage file --defaults
nats stream add FS_METRICS --subjects 'nats.fs.metrics' --storage file --defaults
```

打开 `js-cdr` / `js-metrics` 后，模块对这两个 subject 做同步 `js_Publish`（等待 ack 最多 1 秒）。stream 不存在时退回 core NATS，并在 `nats status` 的 `fallbacks` 里计数。ack 超时不重发，避免同一条 CDR 进流两次。

线上消息形状见 `schema/catalog.json`。

## 7. 设计决策记录

1. **持久化**：`js-cdr` / `js-metrics` 打开后，CDR 和 metrics 走 JetStream 同步发布；流由 nats-server 配置，模块不创建 stream。通道事件保持 core NATS。发布线程在 ack 上最多阻塞 1 秒，不阻塞 FS 核心线程。
2. **背压策略**：发布/请求双有界队列，满则丢弃并计数（`nats status` 可观测），绝不阻塞 FS core 线程。
3. **角色**：`fs.channel.accept` 取得控制权；`fs.channel.observe` 只收信箱事件。`event-routing` 决定公共主题和信箱是否同时发。默认 `both`，与 v0.2 的「广播 + owner 信箱」一致。`DESTROY` 在注销绑定之前发出。
4. **同名避让**：刻意不叫 mod_xcc（XSwitch 官方闭源模块名），协议兼容但实现独立，无 license 争议（协议定义 MIT/Apache 开源）。
5. **ESL 保持不动**：mod_event_socket 保留为运维通道（fs_cli），不参与新集成。

## 8. 已知限制

- `XNode.Dial` 立刻回 202。拨号在独立线程里进行，数量与 `workers` 相同，队列与 `req-qsize` 相同。队列满返回 503。结果仍是 ctrl 信箱上的 Event.Result。卸载时，已经在拨的呼叫会拨完；还排在队列里的回 480 shutting down。
- 未 Accept 的通道，任何知道 uuid 的客户端仍可执行控制方法。Accept 之后只有 owner 可以。
- `fs.native.api` / `fs.native.jsapi` 没有鉴权。
- `global_params` 会原样写成 originate 变量。
- 需要 libnats >= 3.0 才能编译。Windows 工程（.vcxproj）未创建。
- `nats reload` 会重新读取开关并在 JetStream 上下文还不存在时补绑一次，不会重连，也不会拆掉已经绑上的上下文。
