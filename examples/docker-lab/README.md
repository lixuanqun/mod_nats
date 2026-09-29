# Docker 全链路与压测

这个目录是本地集成示例：NATS、带 `mod_nats` 的 FreeSWITCH、一个消费端，外加一条把功能测试和压测串起来的命令。数字记录在仓库根目录的 [TEST-REPORT.md](../../TEST-REPORT.md)。更小的控制器示例在 [tests/go-interop](../../tests/go-interop)。

| 文件 | 作用 |
|------|------|
| `docker-compose.yml` | 拉起 NATS、FreeSWITCH、消费端 |
| `Dockerfile.fs` | 在已有的 FreeSWITCH 镜像上编译 `mod_nats` 和 `mod_loopback` |
| `Dockerfile.tools` | 编译测试程序 `lab` |
| `main.go` | `consume` 订阅上报；`drive` 跑功能用例和压测 |
| `nats-server.conf` | NATS：JetStream，`max_payload` 4MB |
| `nats.conf.xml` | 实验室节点 `fs-lab-1`，前缀 `nats.fs.` |
| `entrypoint-wrap.sh` | 启动前加载 `mod_loopback` 和 `mod_nats` |
| `run.sh` | 构建、启动，然后执行 `drive` |

## 前置

- Docker，并且本机已经有一套带 FreeSWITCH 头文件和 `libfreeswitch` 的镜像。默认基础镜像是 `aicc-fs:local-verify`。换成自己的镜像：

  ```bash
  FS_IMAGE=your-freeswitch-image ./run.sh
  ```

- 构建 `mod_nats` 时会下载 nats.c 3.11.0。Dial 用例需要 `mod_loopback`，镜像里没有时会下载该模块的单个源文件再编译。GitHub 不可达时，Dockerfile 会改走 `ghproxy.net`。
- 测试程序的 Go 模块走 `goproxy.cn`。

## 跑一遍

在本目录：

```bash
./run.sh
```

等价于：

```bash
docker compose up -d --build nats fs consumer
docker compose run --rm --no-deps consumer drive
```

`consumer` 服务一直执行 `lab consume`，订阅 `nats.fs.>`，并建好 JetStream 流 `FS_CDR`（`nats.fs.event.cdr`）和 `FS_METRICS`（`nats.fs.metrics`）。统计写到共享卷 `/data/stats.json`。

`drive` 再起一个容器，对节点 `nats.fs.node.fs-lab-1` 发 JSON-RPC：先跑功能用例，再压 `XNode.JStatus`（32 并发、15 秒），然后压 `XNode.Dial`（4 并发、24 路，`loopback/park`）。结果打印到标准输出，并写入 `/data/report.json`。

看模块状态：

```bash
docker compose exec fs fs_cli -x "nats status"
```

停掉：

```bash
docker compose down
```

## `drive` 覆盖的内容

功能用例包括：未知方法、超大请求、未接管通道上的控制方法、`fs.node.hello`、`XNode.JStatus`、Accept / Observe、Play / Broadcast / NativeApp、SetVar / GetVar、Answer / Hangup / Bridge，以及 Dial 的 202 和随后的 `Event.Result`。`fs.native.api` 在配置关闭时返回 403。

压测不经过 SIP。呼叫时延是 loopback 上从 Dial 到 Result 的时间。
