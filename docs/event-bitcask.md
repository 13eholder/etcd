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
| TTL | **Bitcask 记录级过期**。写入时算出 `expireAt` 存入记录，读取时过滤已过期记录，merge 时物理回收 |
| TTL 数值来源 | **沿用 LeaseGrant，但不 Attach**。LeaseGrant/Revoke 仍走 Raft/BoltDB 正常记账；Put 时查 `lessor.Lookup(leaseID)` 拿剩余 TTL 算出 `expireAt`，不调用 `le.Attach()` |
| Watch 历史 | **只做实时通知**，不保留多版本、不支持 `start_revision` 重放 |
| 内存索引结构 | **B-tree（google/btree）有序索引**，而非教科书式 hash map，用来支撑 `Range` 前缀/区间扫描 |

## Bitcask 存储设计

### 磁盘记录格式

顺序追加写入 active file，每条记录：

```
| crc32(4B) | tstamp(8B) | expireAt(8B, 0=永不过期) | keySize(4B) | valueSize(4B) | key | value |
```

因为不需要崩溃恢复、也不需要历史版本，**不需要 tombstone 记录**：Delete 直接从 B-tree 摘除索引项即可，
旧文件里对应的字节等 merge 时按"B-tree 里还有没有指向它的活记录"来判定是否可回收。

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
    expireAt  int64 // unix nano, 0 = 永不过期
}
```

按 key 字符串排序存于 B-tree。`Get`/`Range` 先查 B-tree 定位，再对目标数据文件 `pread` 取出 value。

### 并发模型

- 单一 active file + 单写锁：同一时刻只有一个 goroutine 追加写，多个 Put 请求排队（Bitcask 经典的单写者模型）。
- B-tree 读多写少，用 `RWMutex` 保护。
- **fsync 策略放宽**：由于确认不需要崩溃恢复，写入不必每条 fsync，可依赖 OS page cache 异步落盘（甚至完全不主动
  fsync），换取更高吞吐——这是 Bitcask 方案相对 MVCC/Raft 路径的核心性能收益之一。*(这点之前没单独确认，请一并
  确认是否接受：进程崩溃 = 该节点自重启前未 fsync 的 Event 数据丢失，反正重启也会整体清空，影响一致)*

### 过期与删除

- `DeleteRange`：直接从 B-tree 摘除匹配的索引项，不写任何磁盘记录。
- TTL 过期：读取（Get/Range）时惰性检查 `expireAt < now`，过期则视为不存在并顺手摘除索引；另起一个后台定时
  任务主动扫描 B-tree 清理已过期但长期未被读取的 key，避免索引/磁盘空间被"僵尸" Event 占用。

### Merge / Compaction

- 目的仅是回收旧文件中已被覆盖/删除/过期记录占用的磁盘空间（不需要为恢复保留任何东西）。
- 触发：定时（如每 10 分钟）或总磁盘占用超过阈值。
- 过程：遍历 B-tree，把仍然指向"非当前 active file"的 entry 的 value 重新写入新的 merge 输出文件并更新索引
  指针，merge 完成后删除原文件。因为单节点、无需恢复，merge 可以直接跳过 B-tree 里已摘除（含已过期）的 key，
  无需为"其他副本"保留任何中间状态。

## API

- `Put(key, value, leaseID)` — 内部据 leaseID 换算 `expireAt`
- `Get(key)` / `Range(prefix, limit)`
- `DeleteRange(startKey, endKey)`
- `Watch(prefix)` — 实时 fan-out，不做历史重放

## Lease 交互

- Put 带 leaseID：`EventStore.Put` 调 `lessor.Lookup(leaseID)` 拿剩余 TTL，算出 `expireAt` 写入 Bitcask 记录，
  不调用 `le.Attach()`。
- LeaseGrant/LeaseRevoke 不拦截，按原路径走 Raft → lessor → BoltDB。lease 到期后 `Revoke` 因为没有 attach 的
  key，`RangeDeleter` 是空操作，无副作用。
- 代价：每个 Event 仍对应一次 LeaseGrant 落 Raft/BoltDB（原 ringbuffer 方案已指出的"空 lease"开销），但相比
  Event Put 本身走 Raft/MVCC/BoltDB，这个代价小得多。

## 实现步骤

1. **新增 `server/storage/bitcask/`**：通用小型 KV 引擎，与 event 无关
   - `bitcask.go`：`Open/Put/Get/Delete/Range/Close`
   - `datafile.go`：数据文件读写、记录编解码
   - `keydir.go`：基于 `google/btree` 的内存索引
   - `merge.go`：后台 merge/compaction
2. **新增 `server/storage/eventstore/event_store.go`**：在原方案基础上，内部存储由 `ringBuffer` 换成 `bitcask.DB`
   - 维护独立局部 revision（仅用于响应头，不做历史 Watch）
   - Put 时接受 leaseID，查询 lessor 计算 `expireAt`
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
- **具体参数（active file 滚动阈值 64MB、merge 触发周期 10 分钟）先按建议值写死在代码里实现，不做成配置项**，
  后续按实测需要再调整或开放配置。
