# nested-transactions-go

支持**命名保存点**、**嵌套事务**、**可串行化并发隔离**与**崩溃恢复**的内存 KV 事务执行器（Go，包 `github.com/guoyyit/nested-transactions-go/tx`）。

```go
m, _ := tx.Open("./data", nil)           // 打开或崩溃恢复
defer m.Close()

m.Run(ctx, func(txn *tx.Tx) error {      // 冲突时自动重试
    txn.Put("a", []byte("1"))
    txn.Savepoint("sp")
    txn.Put("b", []byte("2"))
    return txn.RollbackToSavepoint("sp") // b 被撤销，a 保留
})

m.Run(ctx, func(root *tx.Tx) error {
    child, _ := root.Begin()
    child.Put("c", []byte("3"))
    child.Commit()                       // 仅合并进父事务
    return nil
})                                       // 根提交后 c 才对其他事务可见
```

## 隔离级别与并发语义

* 隔离级别等价于**可串行化（serializable）**：采用乐观并发控制（OCC），每个键带有单调版本号；事务读取已提交数据时记录所观察到的版本，根事务提交时先在临界区内校验读集（read set），通过后才原子发布写集。所有已提交历史等价于按根提交顺序排列的某个串行序列。
* 冲突策略：读集校验失败时提交返回 `ErrConflict`，事务被中止（绝不允许覆盖其他事务的更新，也绝不产生字段级混合状态——同一事务的写集要么整体可见、要么整体不可见）。
* `Manager.Run` 封装了“开启 → 执行 → 提交，遇 `ErrConflict` 退避重试”的完整循环（`Options.MaxRetries`、`RetryBaseBackoff`，退避指数增长且封顶，并响应 `context` 取消）。
* 盲写（未先读同一键）不触发冲突，按提交顺序 last-writer-wins；任何读后写冲突都会被拒绝。
* 根提交的“校验 + WAL 落盘 + 发布”由全局提交锁串行化，保证校验与发布之间不存在窗口。

## 嵌套事务语义

* `Tx.Begin()` 开启子事务，深度从 1 开始计数，超过 `Options.MaxDepth`（默认 8）返回 `ErrMaxDepth`。
* **可见性**：子事务可以读到自身及各祖先的未提交写入；写入在**相邻一层**内传播——子事务 `Commit` 只把自己的操作日志合并进父事务，根事务提交前，任何外部事务都看不到这些写入。
* **子事务提交**：独立于根提交，合并后子事务结束；父事务随后可继续修改同一批键。
* **回滚连带**：任一事务 `Rollback` 都会强制回滚其全部活跃后代；父事务回滚时，已经“提交”到父事务中的子事务结果也一并撤销（因为根尚未提交，它们从未离开本事务家族）。
* 根事务提交时若仍有活跃子事务，这些子事务会被强制回滚（不会静默提交）。
* 锁按“父先于子”的顺序自上而下获取，整个事务家族建议由同一 goroutine 驱动。

## 保存点生命周期

| 操作 | 语义 |
|---|---|
| `Savepoint(name)` | 在当前操作日志位置创建命名保存点，记录在返回前 **fsync 落盘**；重名返回 `ErrSavepointExists`；每事务存活保存点上限 `Options.MaxSavepoints`（默认 64），超限返回 `ErrTooManySavepoints` |
| `RollbackToSavepoint(name)` | 只撤销该保存点**之后**的写入：截断操作日志并重建写集；更早的写入、更早的保存点、其他事务均不受影响；保存点之后创建的保存点被销毁；保存点之后开启的子事务被强制回滚。保存点自身保留，可重复回滚 |
| `ReleaseSavepoint(name)` | 不撤销写入，仅销毁保存点（记录 fsync 落盘）；对未知/已释放保存点再次释放返回 `ErrSavepointNotFound`；**释放后不可再回滚** |

保存点只存在于所属事务内，随事务结束而消失。

## 崩溃恢复

所有状态变更先写 **WAL**（`dir/wal.log`），再发布到内存存储。

* **帧格式**：`[payloadLen uint32][crc32 IEEE uint32][payload]`，文件头含 magic 与版本号；每条记录自带校验和。
* **原子提交**：根事务的整个写集（含各级子事务合并后的最终结果）封装为**一条** commit 记录，一次 `write` + 一次 `fsync`：要么完整出现，要么完全没有，不存在半提交。
* **元数据原子落盘**：创建保存点、释放保存点的记录均立即 fsync，崩溃不会丢失保存点。
* **落盘策略**：commit / savepoint / release 记录强制 fsync；begin / rollback / 子事务提交记录仅作审计、不要求 fsync——恢复正确性从不依赖它们（没有 commit 记录的事务一律视为已回滚）。
* **恢复过程**（`Open` 时自动执行）：
  1. 顺序重放全部 CRC 校验通过的记录；按日志顺序重新应用所有根 commit 的写集；
  2. 尾部部分帧（崩溃中断的撕裂写）或校验失败的记录及之后内容一律**截断丢弃**；
  3. 未完成事务的 begin/savepoint 记录被忽略——其写入从未进入提交存储，故不会残留未提交写入；
  4. 已提交数据完整保留；事务 ID 计数器从日志中最大 ID 继续，重启后不重用 ID。

## 文件组织

| 文件 | 职责 |
|---|---|
| `tx/manager.go` | 管理器：打开/恢复、开启事务、提交串行化、`Run` 重试、关闭 |
| `tx/transaction.go` | 事务：缓冲写、读集、保存点、嵌套提交/回滚 |
| `tx/store.go` | 已提交 KV 存储与每键版本号（OCC 校验） |
| `tx/wal.go` | CRC 帧 WAL：编码、fsync 追加、崩溃重放与尾部截断 |
| `tx/options.go` / `tx/errors.go` | 配置项与错误哨兵 |
