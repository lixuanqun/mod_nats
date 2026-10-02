# mod_nats 部署指南：多节点、控制器 HA 与 NATS 拓扑

本文覆盖生产部署的三类问题：多台 FreeSWITCH 如何接到一条总线上、控制器如何做主备、NATS 自身如何组网与控权。模块配置项的含义见主 [README](../README.md)。

## 1. 多 FS 节点

每台 FreeSWITCH 跑一个 mod_nats，**`node-uuid` 必须固定且全局唯一**（写死在配置里，不要留空——留空每次加载随机生成，控制器无法寻址）：

```xml
<param name="node-uuid" value="fs-bj-01"/>
```

- 每个节点的请求入口是 `{prefix}node.{node_uuid}`，天然多节点寻址，控制器侧维护一个 node 列表即可。
- 节点上下线：加载成功发一次 `Event.NodeUp`（带 capabilities 和 `proto_version`），`{prefix}metrics` 心跳按 `metrics-interval` 周期上报（sessions/cpu/mem/load）。**节点注册表 = 订阅 NodeUp + 心跳超时判断**。
- 通道事件公共主题 `{prefix}event.channel.{STATE}` 不带节点信息，多节点部署建议控制器以 `event-routing=mailbox` 模式为主：Accept 后事件只进控制器信箱，公共主题留给节点级的无人认领呼叫。
- 升级节点：`reload mod_nats` 会清空该节点的通道绑定与幂等缓存；滚动升级时先把节点从调度摘除（停排新呼叫），等存量呼叫自然结束再 reload。

## 2. 控制器 HA（active / passive）

控制面是有状态协议（Accept 取得所有权），**同一条通道永远只应有一个控制者**。不要让两台控制器用同一个 `ctrl_uuid` 同时跑——那是脑裂，不是 HA。正确做法：

1. active 与 passive 用**不同的 `ctrl_uuid`**。
2. 节点侧开启 `owner-lease-ttl`（建议 30s，大于最长的 Dial 超时）。
3. passive 平时只订阅公共事件主题（`event-routing` 为 `both` 或 `broadcast`），不发起控制。
4. active 死亡 → 其所有通道的租约在 ttl 内到期 → 每条通道广播 `Event.OwnerLost`（原 owner 信箱 + `event.ownerlost`）→ passive 收到后按业务状态决定是否 `Accept` 接管。
5. active 恢复后不要抢回存量通道，只接新呼叫。

代价：接管延迟 = `owner-lease-ttl`（≤30s 的静默窗口）。呼叫本身不受影响（lease 只释放绑定，不挂机）。给"接不接"定策略：例如只接管 `ANSWERED` 之后、非 `DESTROY` 的通道，并在接管的 `Accept` 里带上 `channel_params` 恢复上下文。

幂等缓存与租约的配合：active 重试中丢回复的请求，passive 接管后**不会**命中 active 的缓存（缓存作用域含 `ctrl_uuid`）——passive 重放时是全新执行，这是预期行为；需要端到端去重的业务状态放业务侧。

## 3. NATS 拓扑

- **单机**：只适合实验。生产至少两节点 cluster（`-cluster` + seed 节点），客户端 `urls` 填全部成员，cnats 自动故障切换。
- **集群**：3 或 5 节点。CDR/metrics 走 JetStream 时 stream 的副本数 ≥2（`nats stream add FS_CDR --replicas 2 ...`）。
- **跨机房/边缘**：leaf node 把边缘站点的 FS 接回中心集群，FS 的 `urls` 指向本地 leaf，中心故障时 leaf 支持本地缓存的连接重试。控制流量不大，leaf 足够。
- **super-cluster**：除非全球多 region，否则用不上。

### Accounts 与最小权限

总线上有三类流量：控制器 → 节点主题（请求）、节点 → 信箱/公共主题（事件）、CDR → JetStream。用两个 account 隔离，控制器不能伪造节点、节点不能伪造控制器：

```conf
# nats-server.conf 片段
accounts {
  FS_NODES {
    users = [ { user: fs_bj01, password: "..." } ]
    permissions {
      publish = ["nats.fs.ctrl.>", "nats.fs.event.>", "nats.fs.metrics"]
      subscribe = ["nats.fs.node.fs-bj-01", "_INBOX.>"]
    }
  }
  CONTROLLERS {
    users = [ { user: ctrl_main, password: "..." } ]
    permissions {
      publish = ["nats.fs.node.>"]
      subscribe = ["nats.fs.ctrl.>", "nats.fs.event.>", "nats.fs.metrics", "_INBOX.>"]
    }
  }
}
```

要点：每个节点只能订阅自己的 `node.{node_uuid}`；控制器只允许发到 `node.>`（不能发 `ctrl.>`/`event.>` 伪造事件）。多租户时按前缀分 account（`tenantA.fs.` 与 `tenantB.fs.`），模块的 `subject-prefix` 天然支持。

## 4. 容量与背压

- 压测基线见 [TEST-REPORT](../TEST-REPORT.md)：单节点 JStatus ~9.6k rps（p99 6.4ms），回执路径无瓶颈；呼叫容量取决于 FS 本身，不取决于 mod_nats。
- 队列与 worker：默认 `workers=4`/`req-qsize=2048`；高 CPS 建议拉到 `workers=8`，队列满的丢弃都计数在 `nats status`（`dropped`），告警盯这个数。
- 事件风暴：`publish-native-events` 保持关闭，只走通道事件；事件扇出按受众复制的是同一条序列化消息，成本可控。
- JetStream：CDR 流建议 `--retention limits --max-age 7d`，消费慢不会拖垮通话路径（CDR 在独立线程发布，ack 超时不重发）。

## 5. 安全清单

- 总线启用 TLS（nats-server `-tls` + 模块编译 libnats 时带 `NATS_BUILD_WITH_TLS=ON`，客户端配 `credentials` 或 user/pass）。
- `allow-native-api` 保持默认关闭；开了等于把 fs_cli 交给总线上任何有凭据的人。
- `accept-timeout` 打开（例如 15s），防止无人认领的入向呼叫永久占资源。
- Dial 的注入面已封（`execute_on_*`/`api_hangup_hook` 黑名单），但仍建议 NATS 权限收窄到只有控制器账号能发 `node.>`。
