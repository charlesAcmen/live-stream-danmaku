# 🎬 Live-Stream-Danmaku — 高性能直播弹幕后端系统

> Bilibili / YouTube 级直播弹幕系统，Go 语言从零搭建，单机支撑 **6万+ 长连接**、**150万 QPS 下行广播**。

<p align="center">
  <img src="https://img.shields.io/badge/Go-1.25-00ADD8?style=flat&logo=go" alt="Go">
  <img src="https://img.shields.io/badge/Redis-Pub%2FSub-DC382D?style=flat&logo=redis&logoColor=white" alt="Redis">
  <img src="https://img.shields.io/badge/Kafka-Async%20Producer-231F20?style=flat&logo=apachekafka&logoColor=white" alt="Kafka">
  <img src="https://img.shields.io/badge/MySQL-8.0-4479A1?style=flat&logo=mysql&logoColor=white" alt="MySQL">
  <img src="https://img.shields.io/badge/License-MIT-green" alt="License">
</p>

---

## ❓ 项目背景

直播弹幕系统看似简单，实则暗藏多个技术难点：

1. **高并发连接** — 几十万人同时在线，服务器如何抗住？
2. **广播风暴** — 1 条弹幕如何实时发给同一直播间的几万用户？
3. **削峰填谷** — 瞬时百万条弹幕，MySQL 怎么撑住不崩？
4. **数据一致性** — 分布式环境下，在线人数、点赞数如何实时且准确？

本项目是对上述问题的完整工程实践。

---

## 📊 性能数据

在本地 WSL2 环境（模拟单机物理机）下测得：

| 指标 | 数据 |
|------|------|
| ⚡ 连接能力 | 稳定支撑 **6万+** 静态长连接（受限于本地端口/内存） |
| 🚀 吞吐极限 | 峰值下行广播达 **150万 QPS** |
| ⬆️ 上行处理 | 单机稳定处理 **2.5万 QPS** 上行请求（写入 Redis + Kafka） |
| 🛡️ 交付保障 | 极限负载下，周期丢包率 **< 1%**，系统稳定无 Error |
| 💾 落盘效率 | Kafka 消费者批量写入，**500万条积压弹幕 2 分钟内全部落盘**，平均 40k QPS |

另外实现了直播房间在线人数 + 点赞数定时广播。

---

## 🏗️ 系统架构

```
┌─────────────┐     WebSocket      ┌──────────────────────────────────────────┐
│   Client     │ ◄──────────────► │         Go Server (Gin + Gorilla WS)      │
│  (浏览器/APP) │                   │                                          │
└─────────────┘                   │  ┌─────────┐  ┌──────────┐  ┌─────────┐ │
                                  │  │ Handler  │─►│ Service  │─►│  Repo   │ │
                                  │  └────┬────┘  └──────────┘  └────┬────┘ │
                                  │       │                          │       │
                                  │       ▼                          ▼       │
                                  │  ┌─────────────────────────────────┐    │
                                  │  │        WebSocket Manager         │    │
                                  │  │  ┌───────────────────────────┐  │    │
                                  │  │  │  Register / Unregister    │  │    │
                                  │  │  │  Broadcast Channel        │  │    │
                                  │  │  │  Rooms Map (RoomID→Clients)│ │    │
                                  │  │  │  100 Broadcast Workers    │  │    │
                                  │  │  │  sync.Pool (GC 优化)      │  │    │
                                  │  │  └───────────────────────────┘  │    │
                                  │  └──────────┬──────────┬────────────┘    │
                                  └─────────────┼──────────┼─────────────────┘
                                                │          │
                              ┌─────────────────▼──┐  ┌────▼──────────────┐
                              │   Redis (Pub/Sub)   │  │  Kafka (Async)    │
                              │   ┌──────────────┐  │  │  ┌─────────────┐ │
                              │   │ room:{id}:   │  │  │  │ danmaku_    │ │
                              │   │   pubsub     │  │  │  │ save_topic  │ │
                              │   ├──────────────┤  │  │  └──────┬──────┘ │
                              │   │ room:{id}:   │  │  └─────────┼────────┘
                              │   │   likes      │  │            │
                              │   ├──────────────┤  │   ┌────────▼────────┐
                              │   │ room:{id}:   │  │   │ Kafka Consumer  │
                              │   │   online:{s} │  │   │ Group (Batch)   │
                              │   └──────────────┘  │   └────────┬────────┘
                              └────────────────────┘            │
                                                          ┌─────▼─────┐
                                                          │  MySQL 8.0 │
                                                          │  (GORM)    │
                                                          └───────────┘
```

---

## 💡 技术选型与设计

### 1. 🚀 Server 层

- **Go + Gin + Gorilla WebSocket**：协程处理长 WebSocket 连接，内存占用极低，天然适合直播间用户连接
- **分层架构**：Controller → Service → Repo，依赖注入，职责清晰
- **策略模式**：`Dispatcher` + `ActionRegistry` 路由不同消息类型（弹幕 / 点赞 / 统计）

### 2. 📡 分布式消息 — Redis

- **Redis Pub/Sub**：实现多台服务器之间的弹幕实时广播，秒级送达
- **按直播间房号区分频道**：`room:{roomID}:pubsub`，天然隔离
- **Redis 原子计数**：`INCRBY` 处理点赞的超高频读写
- **心跳式在线人数**：每 3 秒各 Server 上报本机在线数（带 TTL），聚合计算全局在线人数，Server 宕机自动过期

### 3. 📈 削峰填谷 — Kafka

- **Async Producer**：作为海量弹幕的缓冲池，异步写入，不阻塞广播链路
- **Snappy 压缩 + 批量 Flush**：减少网络请求次数，提升吞吐
- **Consumer Group + 批量落盘**：`dbWorker` 协程从 Channel 消费，攒够 batchSize 或超时后批量 `INSERT`，500万积压弹幕 2 分钟落盘

### 4. 🛡️ 广播优化

- **Sharding Workers**：100 个 `broadcastWorker` 协程并行消费 Job Channel，避免单点瓶颈
- **sync.Pool 复用 Client 切片**：减少 GC 压力，避免频繁 `make([]*Client)`
- **非阻塞 safeSend**：Client `Send` Channel 缓冲 4096，满时跳过而非阻塞 Worker
- **读写锁分离**：`RLock` 广播遍历 + `Lock` 注册/注销，广播不阻塞连接管理

---

## 📁 项目结构

```
.
├── cmd/
│   ├── server/          # 弹幕 WebSocket 服务器入口
│   ├── consumer/        # Kafka 消费者落盘服务入口
│   ├── client/          # 交互式 CLI 客户端
│   └── benchmark/       # 压测工具（WebSocket + Kafka Producer）
│       └── kafka/       # Kafka 纯写入吞吐压测
├── internal/
│   ├── api/             # Handler 层 (Gin Controller + WebSocket Upgrade)
│   ├── service/         # Service 层 (业务逻辑)
│   ├── repo/            # Repo 层 (GORM 数据访问)
│   ├── model/           # 数据模型 (WsPacket / DanmakuMessage / StatsData / Like)
│   ├── ws/              # WebSocket 核心 (Manager / Client / Dispatcher / Router)
│   ├── consumer/        # Kafka 消费者 Handler (批量落盘 + 重试)
│   ├── infra/           # 基础设施 (Redis / Kafka / MySQL 初始化)
│   └── logger/          # Zap 日志 (dev 彩色 / prod JSON)
├── docker-compose.yaml  # 一键启动 Redis + MySQL + Zookeeper + Kafka
├── go.mod
└── go.sum
```

---

## 🔌 WebSocket 协议

所有通信均使用 `WsPacket` 信封格式：

```json
{
  "type": 101,
  "room_id": "room_0",
  "data": { ... }
}
```

| type | 常量 | 方向 | data 内容 |
|------|------|------|-----------|
| 101 | `TypeDanmaku` | C→S / S→C | `{"room_id":"...", "user_id":123, "content":"hello", "send_time":"..."}` |
| 102 | `TypeStats` | S→C | `{"online":50000, "likes":12000}` |
| 103 | `ActionLike` | C→S | `{"count":1}` |

---

## 🚀 快速开始

### 前置依赖

- Go 1.25+
- Docker & Docker Compose

### 1. 启动基础设施

```bash
docker-compose up -d
```

将启动 Redis (6379)、MySQL (3306)、Zookeeper (2181)、Kafka (9092)。

### 2. 启动弹幕服务器

```bash
go run cmd/server/main.go -port 8080
```

### 3. 启动 Kafka 消费者（落盘服务）

```bash
go run cmd/consumer/main.go
```

### 4. 连接客户端

```bash
# 交互式 CLI 客户端
go run cmd/client/main.go -uid 1001 -room 1001 -port 8080

# 输入文字发送弹幕，输入 1 发送点赞
```

### 5. 压测

```bash
# WebSocket 压测（默认 30000 连接）
go run cmd/benchmark/main.go -c 60000 -r 1s

# Kafka 纯写入吞吐压测
go run cmd/benchmark/kafka/kafka.go
```

---

## 🔧 关键配置参数

### Manager 常量 (`internal/ws/manager.go`)

| 参数 | 值 | 说明 |
|------|----|------|
| `BroadCastInterVal` | 3s | 统计广播间隔 |
| `BroadcastChanSize` | 1024 | Broadcast Channel 缓冲 |
| `BroadcastJobChanSize` | 1000 | Worker Job Channel 缓冲 |
| `WorkerCount` | 100 | 广播 Worker 协程数 |
| `InitClientPoolCap` | 500 | sync.Pool 初始切片容量 |

### Client 常量 (`internal/ws/client.go`)

| 参数 | 值 | 说明 |
|------|----|------|
| `SendChanSize` | 4096 | 单客户端发送缓冲 |

### Kafka Producer 常量 (`internal/infra/kafka.go`)

| 参数 | 值 | 说明 |
|------|----|------|
| `ProducerChanBufSize` | 16384 | Sarama Channel 缓冲 |
| `Flush.Messages` | 50 | 批量消息数阈值 |
| `Flush.Frequency` | 50ms | 批量等待时间阈值 |
| `Compression` | Snappy | 压缩算法 |

### Consumer 常量 (`internal/consumer/handler.go`)

| 参数 | 值 | 说明 |
|------|----|------|
| `FlushInterval` | 2s | DB 刷盘间隔 |
| `MaxDBRetries` | 3 | DB 写入重试次数 |
| `ChannelBufferSize` | 2000 | 消费者→Worker Channel 缓冲 |
| `BackoffDuration` | 500ms | 重试退避时间 |

---

## 🧩 核心流程

### 弹幕发送流程

```
Client ──WS──► Handler ──► Router (TypeDanmaku)
                                │
                    ┌───────────┼───────────┐
                    ▼                       ▼
            Kafka Async Producer      Redis Pub/Sub
            (持久化缓冲)              (跨服务器广播)
                    │                       │
                    ▼                       ▼
            Kafka Cluster          其他 Server 的
                                       subscribeToRoom
                                           │
                                           ▼
                                  broadcastToLocalClients
                                           │
                                           ▼
                                  100 Workers ──► Client.Send
```

### 在线人数 / 点赞数广播流程

```
Manager (每3秒 Ticker)
    │
    ├─ 1. 本地点赞批量聚合 → Redis INCRBY
    ├─ 2. 上报本机在线数 (SETEX + TTL) → Redis Pipeline
    ├─ 3. 聚合全局在线数 (SMembers + MGet)
    ├─ 4. 获取全局点赞数 (GET)
    │
    ▼
broadcastToLocalClients (TypeStats)
    │
    ▼
所有本房间客户端收到 {"online": N, "likes": M}
```

---

## 📌 技术亮点

- **信封协议设计**：`WsPacket` + `json.RawMessage` 延迟解析，路由层无需反序列化内部数据
- **双缓冲落盘**：Consumer `dbWorker` 攒批写入，`buffer[:0]` 复用底层数组，零分配
- **优雅退出**：Client `sync.Once` + `done` Channel；Consumer `Cleanup` 关闭 `msgChan` + `wg.Wait` 确保数据刷完
- **Kafka 背压**：`msgChan` 满时阻塞 `ConsumeClaim`，自然限速
- **Server 心跳**：Redis Key 带 TTL，Server 宕机后自动清理，在线数自动修正

---

## 🛣️ 未来规划

- [ ] JWT 鉴权接入
- [ ] 配置文件化（Viper / YAML）
- [ ] Kubernetes 部署 + HPA 弹性伸缩
- [ ] 弹幕敏感词过滤
- [ ] WebSocket 压缩扩展 (permessage-deflate)
- [ ] gRPC 内部通信
- [ ] Prometheus + Grafana 监控

---

## 👤 作者

**charlesAcmen**

- 📕 小红书：[charlesAcmen](https://www.xiaohongshu.com/user/profile/60b9ea030000000001006c68)
- 🐙 GitHub：[charlesAcmen](https://github.com/charlesAcmen)

---

## 📄 开源协议

本项目基于 [MIT License](LICENSE) 开源，欢迎 Star、Fork 和 PR！
