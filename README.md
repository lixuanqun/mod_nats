# mod_nats

[![build](https://github.com/lixuanqun/mod_nats/actions/workflows/build.yml/badge.svg)](https://github.com/lixuanqun/mod_nats/actions/workflows/build.yml)

**FreeSWITCH NATS message bus module** — XCC-compatible call control over NATS.

FreeSWITCH 的 NATS 消息总线集成模块：为 FreeSWITCH 提供基于 NATS 的呼叫控制面（XCC 协议兼容）、事件流与 CDR 发布。目标是替代 ESL 成为外部系统集成的正门，实现**媒体与控制分层**——RTP/录音/ASR 等媒体处理留在 FS 节点内，业务控制器（Java/Go/任意语言）只通过消息队列收发控制指令，从业务视角驱动呼叫。

- 协议规范参考：[XCC API](https://docs.xswitch.cn/xcc-api/)（信封与语义）、[xctrl.proto](https://github.com/xswitch-cn/proto)（消息结构）
- 独立实现，与 XSwitch 官方闭源模块（mod_xcc）无代码关系；刻意不占用其命名
- v0.1.0，协议兼容子集，状态见文末

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
| `nats_methods.c` | XNode.* 方法实现、Accept 通道绑定、bgapi 任务关联 |
| `nats_events.c` | Event.Channel / Event.CDR / Event.Result 事件发布 |

线程模型：cnats 回调线程只做拷贝入队（永不阻塞）；N 个请求工作线程执行 FS 调用；单发布线程排空发布队列，**队列满则丢弃并计数**（事件流 at-most-once，关键数据走 CDR/JetStream）。

## 2. Subject 布局

前缀可配置（`subject-prefix`），默认 `nats.fs.`。

| Subject | 方向 | 用途 |
|---------|------|------|
| `{prefix}node.{node_uuid}` | 控制器 → FS | JSON-RPC 请求入口 |
| `{prefix}ctrl.{ctrl_uuid}` | FS → 控制器 | 控制器信箱：Accept 后该通道的事件、Event.Result |
| `{prefix}event.channel.{STATE}` | FS → 广播 | 通道状态事件（START/RINGING/MEDIA/ANSWERED/BRIDGE/UNBRIDGE/DESTROY） |
| `{prefix}event.{event_name}` | FS → 广播 | 原生事件转发（默认关闭） |
| `{prefix}event.cdr` | FS → 广播 | CDR（建议服务端配 JetStream stream 持久化） |

**SDK 兼容性**：官方 xctrl Go SDK（`xswitch-cn/xctrl`）将 `cn.xswitch.` 前缀硬编码在 `ctrl/ctrl.go` 中，无配置接口。两种用法：

- 把本模块 `subject-prefix` 配成 `cn.xswitch.` → 官方 Go/Java SDK 即插即用；
- 坚持自有前缀 `nats.fs.` → fork SDK 改前缀（Apache 2.0/MIT 许可允许），或自研客户端。

## 3. 协议

XCC 风格 JSON-RPC 2.0：

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
| XNode.Accept | 接管通道（首个成功者获得控制权，后续 419；可携带 channel_params 数组按通道订阅额外变量） |
| XNode.Answer / Hangup | 应答 / 挂机（cause 可配，默认 NORMAL_CLEARING） |
| XNode.Play / Stop / Broadcast | 放音 / 停止放音 / 广播媒体 |
| XNode.Bridge / ChannelBridge | 桥接两条通道 |
| XNode.SetVar / GetVar / GetState / GetChannelData | 变量与状态读写 |
| XNode.Dial | 外呼（switch_ivr_originate 直调，无命令拼接注入面；立即回 202 + job_uuid，结果走 Event.Result 且携带原始 rpc id；主叫控制器即刻拥有 b-leg） |
| XNode.JStatus | 节点状态：sessions/peak/sps/uptime/version |
| XNode.NativeApp / NativeAPI / NativeJSAPI | 逃生舱：任意 dialplan app / fs API / JSON API |
| Event.Channel / Event.CDR / Event.Result | 事件与异步结果 |

**未实现（规划中）**：UnBridge2、Transfer、Hold、ThreeWay、Mute、ReadDTMF、DetectSpeech（ASR）、Record、Conference 系列、MediaFork——过渡期均可通过 NativeApp/NativeAPI 透传实现。

## 4. 安装（嵌入 FreeSWITCH 源码树）

依赖：[nats.c](https://github.com/nats-io/nats.c)（官方 NATS C 客户端 >= 2.0，pkg-config 名 `nats`）。

```bash
# 1) 安装 libnats
git clone https://github.com/nats-io/nats.c && cd nats.c
cmake -B build -S . -DNATS_BUILD_WITH_TLS=ON && cmake --build build && cmake --install build
ldconfig

# 2) 拷贝进 FreeSWITCH 源码树
cp -r mod_nats /path/to/freeswitch/src/mod/event_handlers/

# 3) build/modules.conf.in 取消注释
sed -i 's/#event_handlers\/mod_nats/event_handlers\/mod_nats/' /path/to/freeswitch/build/modules.conf.in

# 4) configure.ac 的 AC_CONFIG_FILES 里加一行（新模块必须，FS 硬编码模块清单）：
#    在 src/mod/event_handlers/mod_event_socket/Makefile 后面加
#    src/mod/event_handlers/mod_nats/Makefile

# 5) 编译（Makefile.am 通过 pkg-config 自动发现 libnats）
cd /path/to/freeswitch
autoreconf -i
./configure
make mod_nats
make mod_nats-install
```

配置文件 `nats.conf.xml`（见本仓库同名文件）放到 `conf/autoload_configs/`，并在 `modules.conf.xml` 加 `<load module="mod_nats"/>`。

CLI：

```
fsctl> nats status    # 连接状态/收发计数/丢弃计数/会话数
fsctl> nats reload    # 重读配置
```

## 5. 快速联调

```bash
nats-server -p 4222                          # 1. 起 NATS server
fs_cli -x "nats status"                      # 2. 确认模块订阅建立

# 3. 发一个 JStatus 请求（<node_uuid> 从 nats status 里取）
nats request 'nats.fs.node.<node_uuid>' \
  '{"jsonrpc":"2.0","id":"t1","method":"XNode.JStatus","params":{}}'

# 4. 订阅事件
nats sub 'nats.fs.event.channel.>'
nats sub 'nats.fs.event.cdr'
```

## 6. 设计决策记录

1. **持久化交给 JetStream**：CDR/关键数据在 nats-server 侧对 `event.cdr` 配 stream 即可，模块无需感知；事件流保持 core NATS at-most-once。
2. **背压策略**：发布/请求双有界队列，满则丢弃并计数（`nats status` 可观测），绝不阻塞 FS core 线程。
3. **同名避让**：不叫 mod_xcc（XSwitch 官方闭源模块名），协议兼容但实现独立，无 license 争议（协议定义为 MIT/Apache 开源）。
4. **ESL 保持不动**：mod_event_socket 保留为运维通道（fs_cli），不参与新集成。

## 7. 已知限制（v0.1）

- 非法 JSON 请求按 JSON-RPC 2.0 规范回 `id:null` 错误；请求队列满时回 503（v0.1.2）；
- v0.1.3：XNode.Dial 改为 switch_ivr_originate 直调（dial_string 不再拼接进 api 命令行，消除注入面），Event.Result 携带原始 rpc id，主叫控制器隐式接管 b-leg；Accept 支持 per-channel channel_params；新增 accept-timeout（inbound 通道超时无人接管自动挂机，默认 10s，0 关闭）；卸载路径移除 cnats 连接回调注册（消灭 reload 期 asyncCbs 线程崩溃源）并加入事件回调 drain；
- Accept 无"10 秒无人接管挂机"逻辑（XCC 语义），来话需 dialplan 配合 park；
- 未在所有平台编译验证，首次编译可能需修正个别 API 签名差异；
- Windows 工程（.vcxproj）未创建（建议 vcpkg 安装 nats.c）。

## License

MIT（见 [LICENSE](LICENSE)）。协议规范版权归其各自所有方；本仓库与 XSwitch/SignalWire 无隶属关系。
