package tx

import "sync"

type txState int

const (
	stActive txState = iota
	stCommitted
	stRolledBack
)

// op is a single buffered write in a transaction's op log.
type op struct {
	key string
	val []byte
	del bool
}

// savepointInfo locates a savepoint in the op log and in the transaction's
// event stream (used to find children begun after the savepoint).
type savepointInfo struct {
	opIndex int
	evSeq   uint64
}

// Tx is a (possibly nested) transaction.
//
// Writes are buffered in the transaction's own op log. A child transaction
// sees its ancestors' uncommitted writes; a child's Commit only merges its
// op log into the parent — writes become globally visible (and durable) only
// when the root transaction commits. Rolling back any transaction discards
// the merged results of all its descendants.
//
// Locking: methods are safe for concurrent use, but a whole transaction
// family should be driven from a single goroutine; locks are always acquired
// top-down (parent before child) to avoid deadlock.
type Tx struct {
	id     uint64
	m      *Manager
	parent *Tx
	depth  int

	mu       sync.Mutex
	state    txState
	opLog    []op
	writeSet map[string]op
	sp       map[string]savepointInfo
	spOrder  []string          // savepoint names in creation order
	children map[uint64]*Tx    // active children
	childEv  map[uint64]uint64 // child id -> parent evSeq at begin
	evSeq    uint64            // per-tx event counter
	readSet  map[string]uint64 // root only: key -> committed version observed
}

func newTx(m *Manager, id uint64, parent *Tx, depth int) *Tx {
	return &Tx{
		id:       id,
		m:        m,
		parent:   parent,
		depth:    depth,
		state:    stActive,
		writeSet: make(map[string]op),
		sp:       make(map[string]savepointInfo),
		children: make(map[uint64]*Tx),
		childEv:  make(map[uint64]uint64),
		readSet:  make(map[string]uint64),
	}
}

// ID returns the transaction's unique id.
func (tx *Tx) ID() uint64 { return tx.id }

// Depth returns the nesting depth (root = 1).
func (tx *Tx) Depth() int { return tx.depth }

func (tx *Tx) root() *Tx {
	r := tx
	for r.parent != nil {
		r = r.parent
	}
	return r
}

func copyOf(p []byte) []byte {
	if p == nil {
		return nil
	}
	out := make([]byte, len(p))
	copy(out, p)
	return out
}

// Get reads key. Own writes win, then ancestors' uncommitted writes
// (nearest first), then committed state. Reads of committed state are
// recorded in the root's read set for OCC validation at commit time.
func (tx *Tx) Get(key string) ([]byte, bool, error) {
	tx.mu.Lock()
	if tx.state != stActive {
		tx.mu.Unlock()
		return nil, false, ErrTxClosed
	}
	if o, ok := tx.writeSet[key]; ok {
		tx.mu.Unlock()
		if o.del {
			return nil, false, nil
		}
		return copyOf(o.val), true, nil
	}
	tx.mu.Unlock()

	// Walk ancestors without holding our own lock (locks go top-down only).
	for p := tx.parent; p != nil; p = p.parent {
		p.mu.Lock()
		o, ok := p.writeSet[key]
		p.mu.Unlock()
		if ok {
			if o.del {
				return nil, false, nil
			}
			return copyOf(o.val), true, nil
		}
	}

	val, ver, ok := tx.m.store.get(key)
	tx.root().recordRead(key, ver)
	return val, ok, nil
}

// recordRead records the committed version observed for key. First
// observation wins; a later concurrent commit invalidating it makes the
// root's commit fail with ErrConflict.
func (tx *Tx) recordRead(key string, ver uint64) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.state != stActive {
		return
	}
	if _, ok := tx.readSet[key]; !ok {
		tx.readSet[key] = ver
	}
}

// Put buffers a write of key. The value is copied.
func (tx *Tx) Put(key string, val []byte) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.state != stActive {
		return ErrTxClosed
	}
	v := copyOf(val)
	tx.opLog = append(tx.opLog, op{key: key, val: v})
	tx.writeSet[key] = op{key: key, val: v}
	tx.evSeq++
	return nil
}

// Delete buffers a deletion of key.
func (tx *Tx) Delete(key string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.state != stActive {
		return ErrTxClosed
	}
	tx.opLog = append(tx.opLog, op{key: key, del: true})
	tx.writeSet[key] = op{key: key, del: true}
	tx.evSeq++
	return nil
}

// Savepoint creates a named savepoint at the current position. The record is
// fsynced to the WAL before returning. Redefining an existing name is
// rejected.
func (tx *Tx) Savepoint(name string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.state != stActive {
		return ErrTxClosed
	}
	if _, ok := tx.sp[name]; ok {
		return ErrSavepointExists
	}
	if len(tx.sp) >= tx.m.opts.MaxSavepoints {
		return ErrTooManySavepoints
	}
	idx := len(tx.opLog)
	if err := tx.m.wal.logSavepoint(tx.id, name, idx); err != nil {
		return err
	}
	tx.evSeq++
	tx.sp[name] = savepointInfo{opIndex: idx, evSeq: tx.evSeq}
	tx.spOrder = append(tx.spOrder, name)
	return nil
}

// RollbackToSavepoint undoes all writes made after the named savepoint.
// Savepoints created after it are destroyed, and child transactions begun
// after it are force-rolled-back. Earlier writes and savepoints are
// unaffected. Other transactions are never affected.
func (tx *Tx) RollbackToSavepoint(name string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.state != stActive {
		return ErrTxClosed
	}
	info, ok := tx.sp[name]
	if !ok {
		return ErrSavepointNotFound
	}

	// Truncate the op log and rebuild the write set from what remains.
	tx.opLog = tx.opLog[:info.opIndex]
	tx.rebuildWriteSetLocked()

	// Destroy savepoints created after this one.
	for i, n := range tx.spOrder {
		if n == name {
			for _, dropped := range tx.spOrder[i+1:] {
				delete(tx.sp, dropped)
			}
			tx.spOrder = tx.spOrder[:i+1]
			break
		}
	}

	// Force-roll-back children begun after the savepoint.
	var firstErr error
	for id, ev := range tx.childEv {
		if ev <= info.evSeq {
			continue
		}
		if c := tx.children[id]; c != nil {
			c.mu.Lock()
			if err := c.rollbackLocked(); err != nil && firstErr == nil {
				firstErr = err
			}
			c.mu.Unlock()
		}
		delete(tx.children, id)
		delete(tx.childEv, id)
	}
	tx.evSeq++
	return firstErr
}

// ReleaseSavepoint destroys a savepoint without rolling back. Releasing an
// unknown or already-released savepoint is rejected, and a released
// savepoint can no longer be rolled back to.
func (tx *Tx) ReleaseSavepoint(name string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.state != stActive {
		return ErrTxClosed
	}
	if _, ok := tx.sp[name]; !ok {
		return ErrSavepointNotFound
	}
	if err := tx.m.wal.logReleaseSavepoint(tx.id, name); err != nil {
		return err
	}
	delete(tx.sp, name)
	for i, n := range tx.spOrder {
		if n == name {
			tx.spOrder = append(tx.spOrder[:i], tx.spOrder[i+1:]...)
			break
		}
	}
	tx.evSeq++
	return nil
}

// Begin opens a child transaction. The child sees the parent's uncommitted
// state as of now plus its own writes.
func (tx *Tx) Begin() (*Tx, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.state != stActive {
		return nil, ErrTxClosed
	}
	if tx.depth+1 > tx.m.opts.MaxDepth {
		return nil, ErrMaxDepth
	}
	child := newTx(tx.m, tx.m.nextTxID.Add(1), tx, tx.depth+1)
	if err := tx.m.wal.logBegin(child.id, tx.id); err != nil {
		return nil, err
	}
	tx.evSeq++
	tx.children[child.id] = child
	tx.childEv[child.id] = tx.evSeq
	return child, nil
}

// Commit commits the transaction. For a child, its op log is merged into the
// parent (visible to the parent and its later children only). For the root,
// the read set is validated (OCC), the whole write set is appended to the
// WAL as one atomic record and fsynced, and only then published. Still-active
// descendants are rolled back first.
func (tx *Tx) Commit() error {
	if tx.parent == nil {
		return tx.commitRoot()
	}
	return tx.commitChild()
}

func (tx *Tx) commitChild() error {
	p := tx.parent
	p.mu.Lock()
	tx.mu.Lock()
	defer tx.mu.Unlock()
	defer p.mu.Unlock()

	if tx.state != stActive {
		return ErrTxClosed
	}
	if p.state != stActive {
		// Cannot happen by invariant (parent close rolls children back
		// first); kept as a defensive check.
		return ErrTxClosed
	}
	if err := tx.rollbackChildrenLocked(); err != nil {
		return err
	}
	if err := tx.m.wal.logChildCommit(tx.id, tx.opLog); err != nil {
		// Write failed: we cannot know how many bytes reached the log, so
		// the merge must not be attempted and the child is poisoned.
		tx.state = stRolledBack
		delete(p.children, tx.id)
		delete(p.childEv, tx.id)
		return err
	}
	for _, o := range tx.opLog {
		p.opLog = append(p.opLog, o)
		p.writeSet[o.key] = o
	}
	p.evSeq++
	tx.state = stCommitted
	delete(p.children, tx.id)
	delete(p.childEv, tx.id)
	return nil
}

func (tx *Tx) commitRoot() error {
	m := tx.m
	m.commitMu.Lock()
	defer m.commitMu.Unlock()

	tx.mu.Lock()
	if tx.state != stActive {
		tx.mu.Unlock()
		return ErrTxClosed
	}
	if err := tx.rollbackChildrenLocked(); err != nil {
		tx.mu.Unlock()
		return err
	}

	if !m.store.validate(tx.readSet) {
		// Conflict: abort. The transaction must be retried by the caller.
		tx.state = stRolledBack
		tx.mu.Unlock()
		_ = m.wal.logRollback(tx.id)
		m.unregister(tx.id)
		return ErrConflict
	}

	// Durable first: one atomic, fsynced commit record...
	if err := m.wal.logCommit(tx.id, tx.opLog); err != nil {
		// Persistence failed: retrying could append a duplicate commit
		// record and disturb serial order on replay, so poison the tx.
		tx.state = stRolledBack
		tx.mu.Unlock()
		m.unregister(tx.id)
		return err
	}
	// ...then publish.
	m.store.apply(tx.opLog)
	tx.state = stCommitted
	tx.mu.Unlock()
	m.unregister(tx.id)
	return nil
}

// Rollback aborts the transaction and force-rolls-back all its active
// descendants, discarding their committed and uncommitted writes alike.
func (tx *Tx) Rollback() error {
	if tx.parent != nil {
		p := tx.parent
		p.mu.Lock()
		defer p.mu.Unlock()
		tx.mu.Lock()
		defer tx.mu.Unlock()
		return tx.rollbackLocked()
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.rollbackLocked()
}

// rollbackLocked requires tx.mu held (and parent.mu held first, if a child).
func (tx *Tx) rollbackLocked() error {
	if tx.state != stActive {
		return ErrTxClosed
	}
	var firstErr error
	if err := tx.rollbackChildrenLocked(); err != nil {
		firstErr = err
	}
	tx.state = stRolledBack
	tx.opLog = nil
	tx.writeSet = make(map[string]op)
	tx.sp = make(map[string]savepointInfo)
	tx.spOrder = nil
	if err := tx.m.wal.logRollback(tx.id); err != nil && firstErr == nil {
		firstErr = err
	}
	if tx.parent != nil {
		delete(tx.parent.children, tx.id)
		delete(tx.parent.childEv, tx.id)
	} else {
		tx.m.unregister(tx.id)
	}
	return firstErr
}

// rollbackChildrenLocked force-rolls-back all active descendants, deepest
// first. Requires tx.mu held.
func (tx *Tx) rollbackChildrenLocked() error {
	var firstErr error
	for id, c := range tx.children {
		c.mu.Lock()
		if err := c.rollbackLocked(); err != nil && firstErr == nil {
			firstErr = err
		}
		c.mu.Unlock()
		delete(tx.children, id)
		delete(tx.childEv, id)
	}
	return firstErr
}

// rebuildWriteSetLocked recomputes the write set from the op log after a
// savepoint rollback truncated it. Requires tx.mu held.
func (tx *Tx) rebuildWriteSetLocked() {
	tx.writeSet = make(map[string]op, len(tx.opLog))
	for _, o := range tx.opLog {
		tx.writeSet[o.key] = o
	}
}
