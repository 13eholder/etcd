# Event 存储改造：Bitcask 方案（取代 RingBuffer 方案）

本方案承接 [event-ringbuffer.md](./event-ringbuffer.md) 的整体思路——在 `EtcdServer.Put/Range/DeleteRange/Watch` 处按 key 前缀拦截
Event，绕过 Raft/MVCC/BoltDB——但把底层存储从纯内存 RingBuffer 换成 Bitcask（顺序追加日志文件 + 内存索引）。

## 背景 / 换成 Bitcask 的动机

RingBuffer 方案要求把 Event 的完整数据全量放在内存里，大规模集群下（K8s Event churn 很高）内存压力大。
Bitcask 把数据本体写到磁盘顺序日志文件，内存只保留每个 key 的索引项（文件号+偏移+长度，几十字节），
在保留"绕过 Raft/MVCC 换取高吞吐"这一核心目标的同时显著降低内存占用。

## 目标路径（与原方案一致）

```
K8S API Server PUT /registry/events/...
      ↓
   gRPC → EtcdServer.Put()
      ↓
   按 key 前缀检测为 Event → 直接写入 EventStore（Bitcask）
      ↓
   返回响应（跳过 Raft/MVCC/BoltDB）
      ↓
   Watcher 直接通知
```

拦截点、gRPC 中间件链、`Range`/`DeleteRange`/`Watch` 的路由方式与原方案完全一致，不再重复列出，
只是 `EventStore` 内部实现从 `ringBuffer` 换成 Bitcask 引擎。

## 关键决策记录（与 RingBuffer 方案的差异）

| 决策点 | 结论 |
|---|---|
| 重启恢复 | **不需要**。启动时清空 Bitcask 数据目录，全新开始；不扫描历史文件、不重建索引。语义上仍是 ephemeral，只是运行期用磁盘做数据本体存储以省内存 |
| 跨节点一致性 | **单节点本地存储，不复制**。不同 etcd member 上 Event 数据可能不一致，与原方案定位一致（K8s Event 本身 best-effort） |
| TTL | **DB 级全局固定 TTL（仿 Riak bitcask 的 bucket 级过期）**。磁盘只存 `tstamp`（写入时间），过期判断为 `now - tstamp >= TTL`；不再为每条记录单独存绝对到期时间 |
| TTL 数值来源 | **不再查 lessor**。`EventStore` 用一个固定常量 `DefaultTTL`（1 小时，对应 K8s Event 实际约定的 TTL）作为全 store 统一 TTL，Put 时不查询、不依赖具体 leaseID 的剩余时间。LeaseGrant/Revoke 仍走 Raft/BoltDB 正常记账，但 EventStore 完全不关心它们 |
| Watch 历史 | **只做实时通知**，不保留多版本、不支持 `start_revision` 重放 |
| 内存索引结构 | **B-tree（google/btree）有序索引**，而非教科书式 hash map，用来支撑 `Range` 前缀/区间扫描 |

## Bitcask 存储设计

### 磁盘记录格式

顺序追加写入 active file，每条记录：

```
| tstamp(8B) | keySize(4B) | valueSize(4B) | key | value |
```

没有 CRC、没有单独的 `expireAt` 字段、也没有 tombstone 记录：

- **不需要 CRC**：不做崩溃恢复/日志重放，没有谁会去校验一条可能写坏的记录。
- **不单独存 expireAt**：过期改成"DB 级全局 TTL + 每条记录的写入时间 `tstamp`"，`now - tstamp >= TTL` 即视为过期，
  比每条记录各自存一个绝对到期时间更省空间（8 字节直接省掉），代价是所有 key 共用同一个 TTL，不能再各自配置
  不同的过期时长——这对 K8s Event 场景是成立的，因为 K8s 本身就是所有 Event 统一用 1 小时 TTL。
- **不需要 tombstone**：Delete 直接从 B-tree 摘除索引项即可。

**关键约束**：`tstamp` 必须是记录最初的写入时间。它既是单条记录过期判断的依据，也是它所在文件被整体回收
（见下面"空间回收"一节）的依据。

### 文件布局

```
<data-dir>/member/event-bitcask/
  000001.data
  000002.data
  ...
  00000N.data     ← 当前 active file，只追加写
```

- active file 达到阈值（建议 64MB）后 seal，滚动新文件，fileID 单调递增。
- 启动时：`RemoveAll(event-bitcask/)` 后重新创建目录，对应"不需要恢复"的决定。

### 内存索引（B-tree）

```go
type keydirEntry struct {
    fileID    uint32
    valuePos  int64
    valueSize uint32
    tstamp    int64 // unix nano 写入时间；过期 = DB 级 TTL 之后
}
```

按 key 字符串排序存于 B-tree。`Get`/`Range` 先查 B-tree 定位，再对目标数据文件 `pread` 取出 value。

### 并发模型

- 单一 active file + 单写锁：同一时刻只有一个 goroutine 追加写，多个 Put 请求排队（Bitcask 经典的单写者模型）。
- B-tree 读多写少，用 `RWMutex` 保护。
- **fsync 策略放宽**：由于确认不需要崩溃恢复，写入不必每条 fsync，可依赖 OS page cache 异步落盘（甚至完全不主动
  fsync），换取更高吞吐——这是 Bitcask 方案相对 MVCC/Raft 路径的核心性能收益之一。*(这点之前没单独确认，请一并
  确认是否接受：进程崩溃 = 该节点自重启前未 fsync 的 Event 数据丢失，反正重启也会整体清空，影响一致)*

### 过期与删除：全惰性，不做后台索引扫描

- `DeleteRange`：直接从 B-tree 摘除匹配的索引项，不写任何磁盘记录。
- TTL 过期：**只在被查到时才处理**（Get/Range 里已有的 `now - tstamp >= TTL` 检查）——过期就视为不存在，
  顺手把这一条从 B-tree 摘掉。**没有任何后台任务会主动遍历整棵 B-tree 去找过期 key**：一个 key 只要没人再
  查它，它的索引项可以在内存里一直留到进程重启（重启本来就会清空整个 store，语义上不算泄漏，只是运行期间
  会有一点常驻内存膨胀，可接受）。

### 空间回收：整文件删除，不做 merge/compaction

**不再有 merge**。回收磁盘空间的单位是整个已封存（sealed）文件，而不是单条记录：

- 每个文件维护一个 `deadline`——它最后一条写入记录的 `tstamp`，在这个文件被封存（滚动到下一个文件）的那一刻
  冻结下来。
- 因为同一个文件里所有记录的 `tstamp` 都 ≤ 这个 `deadline`，只要 `now - deadline >= TTL`，就能断定**这个文件
  里的每一条记录都已经过期**——不需要挨条检查，直接把整个文件从磁盘删掉（`os.Remove`）。
- 后台监测进程（默认每 10 分钟跑一次）只做一件事：从"当前最老的、还没被回收的已封存文件"开始，看它的
  `deadline` 是否已经过期；过期就删掉、把游标移到下一个文件，直到遇到一个还没过期的文件（或者追到当前
  active file）为止。因为文件是按创建顺序封存的，`deadline` 天然随文件号单调不减，所以只要一直往后追、遇到
  第一个没过期的就能停，不需要扫描所有文件。
- **删除文件时完全不碰内存索引**：如果 B-tree 里还有 key 指向这个刚被删掉的文件，那这个 key 的 `tstamp` 必然
  也早就超过 TTL 了（同一个不等式），所以下次真被 `Get`/`Range` 查到时，"过期与删除"一节里的惰性检查会先一步
  判定它过期，根本不会走到"去这个文件里读 value"那一步——两边完全解耦，不需要互相同步。
- active file 永远不会被这套机制碰——只有封存过的文件才有 `deadline`，正在写的文件没有。

这套方案完全不需要"挑出还活着的记录、搬到新文件"这一步，比 merge 简单得多：代价是回收粒度变粗了（必须等
一整个文件里最新的那条记录也过期，才能整体删除），但 Event 场景本身高频覆盖写、TTL 统一，一个文件里的记录
写入时间本来就很接近，这个粒度损失可以接受。

## API

- `bitcask.DB.Put(key, value)` — 不再接受 expireAt/TTL 参数，过期由 `Options.TTL`（DB 级全局值）统一控制
- `bitcask.DB.Get(key)` / `Range(prefix, limit)`
- `bitcask.DB.DeleteRange(startKey, endKey)`
- `EventStore.Watch(prefix)` — 实时 fan-out，不做历史重放

## Lease 交互

- `EventStore.Put` **不再查询 lessor**：Event 的过期时间统一是"写入时刻 + `DefaultTTL`（1 小时）"，与请求携带
  的具体 leaseID、该 lease 的剩余 TTL 完全无关。`r.Lease` 只是原样存进 `mvccpb.KeyValue.Lease` 字段，供客户端
  读回时看到自己填的值，不做任何校验（不存在的 leaseID 也会被接受）。
- LeaseGrant/LeaseRevoke 不拦截，按原路径走 Raft → lessor → BoltDB。lease 到期后 `Revoke` 因为没有 attach 的
  key，`RangeDeleter` 是空操作，无副作用。
- 代价：每个 Event 仍对应一次 LeaseGrant 落 Raft/BoltDB（原 ringbuffer 方案已指出的"空 lease"开销），但相比
  Event Put 本身走 Raft/MVCC/BoltDB，这个代价小得多。
- 如果某个 Event 的 lease TTL 被 K8s 设置成不是 1 小时，这里不会跟随——过期时间始终按 `DefaultTTL` 走，这是
  用"全局统一、更省空间"换来的已知限制。

## 实现步骤

1. **新增 `server/storage/bitcask/`**：通用小型 KV 引擎，与 event 无关
   - `bitcask.go`：`Open/Put/Get/Delete/Range/Close`
   - `datafile.go`：数据文件读写、记录编解码
   - `keydir.go`：基于 `google/btree` 的内存索引
   - `reap.go`：后台监测进程，按文件 `deadline` 整体删除已过期的封存文件
2. **新增 `server/storage/eventstore/event_store.go`**：在原方案基础上，内部存储由 `ringBuffer` 换成 `bitcask.DB`
   - 维护独立局部 revision（仅用于响应头，不做历史 Watch）
   - `New()` 不再需要 `lessor` 参数；`bitcask.Open` 时传入 `Options{TTL: DefaultTTL}`（1 小时）
   - 维护 watcher 列表，写入时 fan-out（不变）
3. **`server/etcdserver/api/v3rpc/quota.go`**：不变，同原方案
4. **`server/etcdserver/v3_server.go`**：不变，同原方案；`EventStore` 字段类型换底层实现
   - `NewServer` 初始化时先 `RemoveAll(<data-dir>/member/event-bitcask/)` 再 `Open` 全新 `bitcask.DB`
5. **`server/etcdserver/api/v3rpc/watch.go`**：不变，同原方案
6. 优雅关闭：flush + close 数据文件句柄（非必需，但避免泄漏）

## 已确认：fsync 策略与参数

- **完全不主动 fsync**：写入只到 OS page cache，物理落盘交给内核 writeback 线程按默认节奏处理（通常几十秒
  内）。同进程读走的是页缓存 + 内存 B-tree 索引，不受影响。进程崩溃或宿主机断电导致的数据丢失，与"重启即
  清空目录"的整体语义一致，不引入新的不一致风险，换取最高写入吞吐。
- **具体参数（active file 滚动阈值 64MB、回收监测周期 10 分钟）先按建议值写死在代码里实现，不做成配置项**，
  后续按实测需要再调整或开放配置。
