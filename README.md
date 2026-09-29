# mod_nats 模块设计文档

NATS 消息总线集成模块。为 FreeSWITCH 提供通用的呼叫控制面、通道事件与 CDR 发布：任意外部系统通过 NATS 上的 JSON-RPC 接入，媒体留在 FreeSWITCH，控制走总线。控制协议与 XSwitch XCC 兼容，官方 XCtrl 可以作为客户端接入，自研的 Java、Go 或其他语言客户端按同一套主题和方法接入。

## 1. 架构

```
┌───────────────────────────────┐            ┌────────────────────────────┐
│  FreeSWITCH (媒体节点)         │            │  外部控制器                 │
│                               │   NATS     │  Java / Go / 自研服务       │
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

线程模型：cnats 回调线程只做拷贝入队（永不阻塞）；N 个请求工作线程执行 FS 调用；事件回调只复制通道事件需要的头部，序列化线程把每条 JSON 打印一次再扇出。应答线程发送 RPC 回复和 Event.Result。通道事件走 core NATS 发布线程。CDR 和 metrics 走另一条线程，JetStream ack 不会挡住通道事件。断线时发布线程等待重连而不是清队列。队列满则丢弃并计数（事件流 at-most-once，关键数据走 CDR/JetStream）。入站 RPC 超过 1 MiB 直接拒绝。

## 2. Subject 布局

前缀可配置（`subject-prefix`），默认 `nats.fs.`。

| Subject | 方向 | 用途 |
|---------|------|------|
| `{prefix}node.{node_uuid}` | 控制器 → FS | JSON-RPC 请求入口 |
| `{prefix}ctrl.{ctrl_uuid}` | FS → 控制器 | 控制者信箱，以及 `fs.channel.observe` 观察者信箱：通道事件、Event.Result |
| `{prefix}event.channel.{STATE}` | FS → 广播 | 通道状态事件。`event-routing=mailbox` 且通道已被 Accept 时不发 |
| `{prefix}event.{event_name}` | FS → 广播 | 原生事件转发（默认关闭） |
| `{prefix}event.cdr` | FS → 广播 | CDR（建议服务端配 JetStream stream 持久化） |

**与官方 XCtrl 对接（可选）**：模块的默认前缀是 `nats.fs.`，自研客户端直接用。官方 xctrl Go SDK（`xswitch-cn/xctrl`）把 `cn.xswitch.` 前缀写死在 `ctrl/ctrl.go` 里，没有配置项。要接这套 SDK 时：
- 把本模块 `subject-prefix` 配成 `cn.xswitch.` → 官方 Go/Java SDK 即插即用；
- 坚持自有前缀 `nats.fs.` → fork SDK 改前缀（Apache 2.0/MIT 允许），或自研客户端。

## 3. 集成步骤

下面按一台新机器从零接到第一条控制请求的顺序写。主题前缀用默认 `nats.fs.`，节点 ID 建议在生产环境写死。

1. **起 NATS。** 安装并启动 `nats-server`。只要 CDR 或 metrics 走 JetStream，服务端就要先开 JetStream，并建好流。模块不会创建 stream。

   ```bash
   nats-server -js -p 4222
   nats stream add FS_CDR --subjects 'nats.fs.event.cdr' --storage file --defaults
   nats stream add FS_METRICS --subjects 'nats.fs.metrics' --storage file --defaults
   ```

   服务端默认 `max_payload` 是 1MB。模块自身拒绝大于 1MiB 的 RPC。若要把这条限制测到，把服务端上限调到大于 1MiB（例如 4MB）。默认 1MB 时，过大的请求会被客户端或服务端在到达模块之前丢掉。

2. **安装 libnats。** 需要 [nats.c](https://github.com/nats-io/nats.c) >= 3.0。启用 TLS 时用 `-DNATS_BUILD_WITH_TLS=ON` 编译。安装后系统里要有 `libnats.pc` 或 `nats.pc`，`pkg-config --modversion libnats`（或 `nats`）至少是 3.0。

3. **放进 FreeSWITCH 源码树并编译。** 把本仓库放到 `src/mod/event_handlers/mod_nats`。这里的 `Makefile.am` 用 pkg-config 直接找 libnats，不依赖 FreeSWITCH `configure.ac` 里的检测。然后：

   ```bash
   sed -i 's/#event_handlers\/mod_nats/event_handlers\/mod_nats/' build/modules.conf.in
   ./bootstrap.sh -j          # 或 autoreconf -i
   ./configure
   make mod_nats
   make mod_nats-install
   ```

   Windows 的 `.vcxproj` 还没有。

4. **放配置。** 把本仓库根目录的 `nats.conf.xml` 拷到运行配置的 `autoload_configs/`。至少改这几项：

   - `urls`：NATS 地址，多个用逗号分隔
   - `node-uuid`：固定节点 ID。留空则每次加载随机生成，控制器无法写死订阅主题
   - `subject-prefix`：默认 `nats.fs.`。要直接用官方 xctrl SDK 时改为 `cn.xswitch.`
   - 认证：`user`/`password` 或 `credentials`，二选一
   - `js-cdr` / `js-metrics`：流已经建好再开

   在 `autoload_configs/modules.conf.xml` 里加入 `<load module="mod_nats"/>`。

5. **加载并确认。**

   ```bash
   fs_cli -x "load mod_nats"
   fs_cli -x "nats status"
   ```

   `conn_state` 应为 `UP`，`listen` 是控制器要发请求的主题，形如 `nats.fs.node.<node-uuid>`。

6. **集成方先订阅，再发请求。** 控制器进程订阅第 4 节里的上报主题（至少 `event.channel.>`、`event.cdr`，以及自己的 `ctrl.<ctrl_uuid>`）。然后再向 `listen` 那条主题发 JSON-RPC。先订阅再产生呼叫，避免错过 `Event.NodeUp` 和第一批通道事件。这些上报走的是 core NATS，不是请求的 reply inbox。

7. **接管呼叫。** 入向通道出现 `Event.Channel` / `START` 之后，用 `XNode.Accept`（或 `fs.channel.accept`）带上 `uuid` 和 `ctrl_uuid`。只有 Accept 成功的一方能执行控制方法。观察者不抢控制权，用 `fs.channel.observe`。外呼用 `XNode.Dial`，立即得到 202，最终结果在 `ctrl.<ctrl_uuid>` 上以 `Event.Result` 送达。

本地 Docker 全链路和压测记录见 [TEST-REPORT.md](TEST-REPORT.md)。脚本、消费端和启动方式在 [examples/docker-lab](examples/docker-lab)。

## 4. 上报事件与可消费事件

方向以 FreeSWITCH 为参照。**上报**是模块发到总线上、供外部订阅的消息。**消费**是模块从总线上收下来并处理的消息。模块只订阅节点主题 `{prefix}node.{node_uuid}`，不订阅通道事件主题。

### 4.1 模块上报（外部应订阅）

`publish-events=true`（默认）时上报。`event-routing` 决定通道事件发到公共主题、信箱，或两边都发（默认 `both`）。

| 消息 | Subject | 何时出现 |
|------|---------|----------|
| `Event.NodeUp` | `{prefix}event.nodeup` | 模块加载成功时发一次 |
| `Event.Channel` | `{prefix}event.channel.{STATE}`，以及已登记 ctrl 的 `{prefix}ctrl.{ctrl_uuid}` | 下表中的通道状态。`mailbox` 模式下，通道已被 Accept 后不再发公共主题 |
| `Event.CDR` | `{prefix}event.cdr`（可用 `cdr-subject` 改） | `enable-cdr=true` 时，挂机完成。`js-cdr=true` 且流存在时走 JetStream |
| `Event.Metrics` | `{prefix}metrics` | 配置了 `metrics-interval`（秒，大于 0）之后按该间隔发送。`js-metrics=true` 时走 JetStream |
| `Event.Result` | `{prefix}ctrl.{ctrl_uuid}` | `XNode.Dial` 的异步结果，带原来的 JSON-RPC `id`。不走 JetStream |
| `Event.NativeEvent` | `{prefix}event.{事件名小写}` | 仅 `publish-native-events=true`。`SWITCH_EVENT_LOG` 一律丢弃 |

`Event.Channel` 的 `params.state` 与 FreeSWITCH 事件的对应关系：

| `params.state` | FreeSWITCH 事件 |
|----------------|-----------------|
| `START` | `CHANNEL_CREATE` |
| `RINGING` | `CHANNEL_PROGRESS` |
| `MEDIA` | `CHANNEL_PROGRESS_MEDIA` |
| `ANSWERED` | `CHANNEL_ANSWER` |
| `BRIDGE` | `CHANNEL_BRIDGE` |
| `UNBRIDGE` | `CHANNEL_UNBRIDGE` |
| `DESTROY` | `CHANNEL_HANGUP_COMPLETE` |

`CHANNEL_DESTROY` 不再单独上报，挂机完成即 `DESTROY`。通道事件里固定带上的字段：`uuid`、`direction`、`caller_id_name`、`caller_id_number`、`destination_number`、`network_addr`、`context`、`peer_uuid`、`hangup_cause`、`timestamp`。`channel-params` 和 Accept 时提交的变量白名单会额外附在同一条消息上。

CDR 字段：`uuid`、`caller_id_name`、`caller_id_number`、`destination_number`、`direction`、`context`、`hangup_cause`、`duration`、`billsec`、`start_stamp`、`answer_stamp`、`end_stamp`。

推荐的外部订阅：

```bash
nats sub 'nats.fs.event.>'      # NodeUp、通道事件、CDR、原生事件
nats sub 'nats.fs.metrics'      # 节点心跳
nats sub 'nats.fs.ctrl.>'       # 信箱：通道事件与 Event.Result
```

### 4.2 模块消费（外部应发到节点主题）

只消费发到 `{prefix}node.{node_uuid}` 的 JSON-RPC 2.0 请求。没有 `id` 的通知会执行，但不回复。回复发到 NATS 的 reply inbox，不发到上报主题。

| 规范名 | XCC 别名（`compat-xcc` 默认开） | 需要已 Accept 的通道 |
|--------|--------------------------------|----------------------|
| `fs.node.hello` | — | 否 |
| `fs.node.status` | `XNode.JStatus` | 否 |
| `fs.channel.accept` | `XNode.Accept` | 否（本方法取得控制权） |
| `fs.channel.observe` | — | 否 |
| `fs.channel.unobserve` | — | 否 |
| `fs.channel.dial` | `XNode.Dial` | 否（成功后该 uuid 归 `ctrl_uuid`） |
| `fs.channel.answer` | `XNode.Answer` | 是 |
| `fs.channel.hangup` | `XNode.Hangup` | 是 |
| `fs.channel.play` | `XNode.Play` | 是 |
| `fs.channel.stop` | `XNode.Stop` | 是 |
| `fs.channel.broadcast` | `XNode.Broadcast` | 是 |
| `fs.channel.bridge` | `XNode.Bridge` | 是，两条腿都必须是本 ctrl |
| `fs.channel.setvar` | `XNode.SetVar` | 是 |
| `fs.channel.getvar` | `XNode.GetVar` | 是 |
| `fs.channel.getstate` | `XNode.GetState` | 是 |
| `fs.channel.data` | `XNode.GetChannelData` | 是 |
| `fs.native.app` | `XNode.NativeApp` | 是 |
| `fs.native.api` | `XNode.NativeAPI` | 否，且默认关闭 |
| `fs.native.jsapi` | `XNode.NativeJSAPI` | 否，且默认关闭 |

未列出的方法返回 501。入站正文超过 1MiB 返回 400 `payload too large`。

## 5. 协议

控制面使用 JSON-RPC 2.0。方法同时提供本模块的 `fs.*` 名称和 XCC 的 `XNode.*` 别名（`compat-xcc` 默认开启）。信封与 [XCC 设计](https://docs.xswitch.cn/xcc-api/design/) 以及 xswitch-cn/proto 的 `xctrl.proto` 一致，XCtrl 和自研客户端都按下面三种消息来写：

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
| XNode.Answer / Hangup | 应答 / 挂机。必须先 Accept；非 owner 返回 419 |
| XNode.Play / Stop / Broadcast | 放音 / 停止放音 / 广播媒体 |
| XNode.Bridge / ChannelBridge | 桥接两条通道 |
| XNode.SetVar / GetVar / GetState / GetChannelData | 变量与状态读写 |
| XNode.Dial | 外呼（bgapi originate，立即回 202 + job_uuid，结果走 Event.Result） |
| XNode.JStatus | 节点状态：sessions/peak/sps/uptime/version |
| XNode.NativeApp / NativeAPI / NativeJSAPI | NativeApp 需 owner，把 `cmd::args` 排进会话线程后立即返回，不等应用跑完。NativeAPI / NativeJSAPI 默认关闭，见 `allow-native-api` |
| Event.Channel / Event.CDR / Event.Result | 事件与异步结果 |

**未实现（规划中）**：UnBridge2、Transfer、Hold、ThreeWay、Mute、ReadDTMF、DetectSpeech（ASR，可对接 mod_ws_audio）、Record、Conference 系列、MediaFork。NativeApp 在通道已被 Accept 后仍可把 dialplan app 排进该通道的会话线程。

**未 Accept 的通道拒绝控制方法**（400 `channel not accepted`）。`fs.channel.accept` 之后只有 owner 可以控制。`fs.channel.observe` 在 Accept 之前也可以订阅信箱。

`fs.native.api` / `fs.native.jsapi` 默认关闭，配置 `allow-native-api=true` 才放行。`Dial` 的 `global_params` 和 `dial_string` 里如果出现 `execute_on_*`、`api_on_*`、`api_hangup_hook`、`exec_after_*`，整次外呼返回 400。

外呼在 `originate` 之前用 `origination_uuid` 占住 owner。失败或卸载时会释放这个绑定。

## 6. 配置（conf/autoload_configs/nats.conf.xml）

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
    <!-- 未 Accept 的入向呼叫超时挂机，0 关闭（默认）。打开后任务挂在 mod_nats 这个 scheduler group 上 -->
    <param name="accept-timeout" value="0"/>
    <!-- fs.native.api / fs.native.jsapi。默认关 -->
    <param name="allow-native-api" value="false"/>
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

## 7. 编译

依赖：[nats.c](https://github.com/nats-io/nats.c) >= 3.0（JetStream API 在 `nats.h` 中，没有单独的 `NATS_HAS_JETSTREAM` 宏）。启用 TLS 时加 `-DNATS_HAS_TLS` 构建 libnats。本仓库的 `Makefile.am` 通过 pkg-config 发现依赖，优先 `libnats.pc`，没有则回退 `nats.pc`。

```bash
# 安装 libnats（Linux 示例）
cmake -B build -S . -DNATS_BUILD_WITH_TLS=OFF && cmake --build build && cmake --install build

# 把本仓库放到 src/mod/event_handlers/mod_nats 之后
sed -i 's/#event_handlers\/mod_nats/event_handlers\/mod_nats/' build/modules.conf.in
./bootstrap.sh -j
./configure
make mod_nats
make mod_nats-install
```

Windows：用 vcpkg（`vcpkg install nats.c`）或 CMake 自行构建 nats.c，工程文件待补充（.vcxproj）。

## 8. 快速联调

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

打开 `js-cdr` / `js-metrics` 后，模块在独立线程上对这两个 subject 做同步 `js_Publish`（等待 ack 最多 1 秒）。通道事件在另一条线程上发出，不等这个 ack。stream 不存在时退回 core NATS，并在 `nats status` 的 `fallbacks` 里计数。ack 超时不重发，避免同一条 CDR 进流两次。

线上消息形状见 `schema/catalog.json`。

## 9. 设计决策记录

1. **持久化**：`js-cdr` / `js-metrics` 打开后，CDR 和 metrics 走独立线程上的 JetStream 同步发布；流由 nats-server 配置，模块不创建 stream。通道事件保持 core NATS，由另一条线程发出，不等 ack。应答再走一条线程，核心线程只入队。
2. **背压策略**：通道事件、JetStream、应答、请求、事件复制五条有界队列，满则丢弃并计数（`nats status` 可观测），绝不阻塞 FS core 线程。订阅还有 4096 条 / 8 MiB 的 pending 上限。断线时发布线程握住当前这一条并等待重连，不会把队列清掉。RPC 应答和 Event.Result 不等 JetStream ack。每条通道事件只序列化一次，再按受众扇出。
3. **角色**：`fs.channel.accept` 取得控制权；未 Accept 的通道拒绝控制。`fs.channel.observe` 只收信箱事件。`event-routing` 决定公共主题和信箱是否同时发。默认 `both`。`DESTROY` 在注销绑定之前发出。Accept 超时默认关闭。
4. **同名避让**：刻意不叫 mod_xcc（XSwitch 官方闭源模块名），协议兼容但实现独立，无 license 争议（协议定义 MIT/Apache 开源）。
5. **ESL 保持不动**：mod_event_socket 保留为运维通道（fs_cli），不参与新集成。

## 10. 已知限制

- `XNode.Dial` 立刻回 202。拨号在独立线程里进行，数量与 `workers` 相同，队列与 `req-qsize` 相同。队列满返回 503。结果仍是 ctrl 信箱上的 Event.Result。卸载时，已经在拨的呼叫会拨完；还排在队列里的回 480 shutting down。外呼前会占住 `origination_uuid`。
- 未 Accept 的通道不能执行控制方法。`fs.native.api` / `fs.native.jsapi` 默认关闭。
- `global_params` 会写成 originate 变量，但 `execute_on_*` / `api_on_*` / `api_hangup_hook` / `exec_after_*` 会被拒绝。
- 需要 libnats >= 3.0 才能编译。Windows 工程（.vcxproj）未创建。
- `nats reload` 会重绑事件订阅并应用运行期开关。连接 URL、前缀、node-uuid、账号和队列长度保持加载时的值，要改这些需要重启模块。JetStream 上下文一旦绑上就不会拆。
- 通道事件在独立线程里序列化，只复制白名单头部。`publish-native-events` 关闭时只绑定通道状态事件，不再订阅 `SWITCH_EVENT_ALL`。打开后仍丢弃 `SWITCH_EVENT_LOG`，避免日志回流。`XNode.NativeApp` 返回 200 表示应用已排队，不表示应用已经结束。
