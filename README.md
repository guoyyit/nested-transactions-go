# nested-transactions-go

A transaction executor with **named savepoints** and **nested transactions**
built on top of a flat, WAL-backed key-value storage layer.

```
txn/    transaction executor: nesting, savepoints, isolation, retries
store/  flat committed-state layer: versions, atomic commit, WAL replay
wal/    write-ahead log: length+CRC framed records, torn-tail truncation
```

## Usage

```go
e, _ := txn.Open("/var/lib/mydb")
defer e.Close()

tx, _ := e.Begin()
tx.Put("account:1", []byte(`{"balance":100}`))

tx.Savepoint("before-transfer")

child, _ := tx.BeginChild()        // nested transaction
child.Put("account:2", []byte(`{"balance":50}`))
child.Commit()                     // merges into parent, still uncommitted

tx.RollbackTo("before-transfer")   // undoes the child's merged commit too

tx.Commit()                        // only now do writes become visible
```

Automatic conflict retry:

```go
err := e.Run(func(tx *txn.Tx) error {   // retries the whole closure
    v, _, _ := tx.Get("counter")        // on ErrConflict, with backoff
    n, _ := strconv.Atoi(string(v))
    return tx.Put("counter", []byte(strconv.Itoa(n+1)))
})
```

## Isolation level

**Serializable.** Every transaction tree records the committed version of
each key it touches (reads *and* writes). At root commit, versions are
validated and — if unchanged — the commit record is fsynced and all writes
of the tree are applied under a single lock. Committed transactions are
therefore equivalent to the serial order of their commit points.

* Writes of a transaction tree are visible only inside that tree until the
  **root** commits; the root commit publishes them atomically.
* Reads inside a tree are repeatable: the first read of a key is cached
  for the whole tree.
* Values are read and written atomically per key — readers never observe
  field-level mixed state.
* **Conflict policy:** a commit that finds a touched key changed by
  another transaction is rejected with `ErrConflict` and the whole tree is
  rolled back. Callers may retry; `Executor.Run` does so automatically
  with exponential backoff (budget: `WithMaxRetries`, default 10).

## Nested transactions

* `tx.BeginChild()` opens a child that sees the parent's live uncommitted
  state. Maximum depth is bounded (`WithMaxDepth`, default 8); exceeding
  it fails with `ErrMaxDepth`.
* Child `Commit()` merges the child's writes into the parent. They remain
  invisible to other transactions until the **root** commits.
* Child `Rollback()` discards only the child's own subtree.
* Rolling back any node also undoes everything its descendants did —
  including commits already merged into it.
* Committing or rolling back a node aborts its still-open descendants;
  using any ended node fails with `ErrTxnClosed`.

## Savepoint lifecycle

* `Savepoint(name)` creates a savepoint **and fsyncs it to the WAL** before
  returning. Re-using an existing name shadows the older savepoint
  (SQL semantics).
* `RollbackTo(name)` undoes this node's writes made after the savepoint —
  including writes merged in from children that committed after it — and
  aborts children begun after it. Earlier writes are untouched, as are
  other transactions. Savepoints created *after* the target are
  invalidated by the rollback.
* `ReleaseSavepoint(name)` destroys a savepoint. Rolling back to a
  released savepoint fails with `ErrSavepointReleased`; releasing twice
  (or releasing an unknown name) is rejected; the name may be re-created
  afterwards.
* All savepoints of a transaction die with it (commit or rollback).

## Crash recovery

* Every metadata record — begin, savepoint, commit, rollback — is
  **fsynced before the call returns**, so no acknowledged operation can be
  lost or half-written.
* WAL records are framed with length + CRC32. On open, the log is scanned
  and any torn tail (crash mid-write) is truncated: each record is either
  fully durable or entirely absent.
* Recovery replays commit records only. Transactions without a commit
  record leave **no trace** — no leftover uncommitted writes — and every
  committed write survives, together with its transaction's metadata.
* `Executor.Compact()` atomically rewrites the log as a single snapshot
  record (write temp file, fsync, rename).

## Testing

```
go test ./... -race
```

Coverage includes: nested commit/rollback chains, parent rollback
cascading over committed children, partial rollback to savepoints,
savepoint release/shadowing rules, depth limits, write-write and
read-write conflict rejection, a concurrent counter proving no lost
updates, a mixed-state detector, and crash recovery — both by
reopening the store mid-transaction and by SIGKILLing a real helper
process between operations.
