package txn

import (
	"errors"
	"sort"
	"sync"

	"github.com/guoyyit/nested-transactions-go/store"
	"github.com/guoyyit/nested-transactions-go/wal"
)

// writeEnt is one staged write in a node's overlay.
type writeEnt struct {
	val []byte
	del bool
}

// undoEnt restores the previous overlay entry during savepoint rollback.
type undoEnt struct {
	key     string
	prev    writeEnt
	hadPrev bool
}

// savepoint captures the undo-log height and the set of active children
// at creation time, so a later rollback can restore exactly that state.
type savepoint struct {
	undoLen  int
	childSet map[*Tx]bool
}

// readEnt is a committed value cached at first read (repeatable reads).
type readEnt struct {
	val   []byte
	found bool
}

// treeState is shared by all nodes of one transaction tree.
type treeState struct {
	exec   *Executor
	id     uint64
	mu     sync.Mutex
	closed bool
	reads  map[string]uint64  // key -> committed version at first touch
	cache  map[string]readEnt // key -> committed value at first read
}

// Tx is one node in a transaction tree. A root transaction is created by
// Executor.Begin; children are created by Tx.BeginChild. A Tx is not safe
// for concurrent use by multiple goroutines; different transaction trees
// are.
type Tx struct {
	tree     *treeState
	parent   *Tx
	depth    int
	overlay  map[string]writeEnt // this node's own staged writes
	undo     []undoEnt           // undo log backing savepoints
	sps      map[string]savepoint
	spOrder  []string // savepoint creation order, for invalidation
	released map[string]bool
	children map[*Tx]bool // active (uncommitted, unaborted) children
	done     bool
}

func newTx(tr *treeState, parent *Tx, depth int) *Tx {
	return &Tx{
		tree:     tr,
		parent:   parent,
		depth:    depth,
		overlay:  make(map[string]writeEnt),
		sps:      make(map[string]savepoint),
		released: make(map[string]bool),
		children: make(map[*Tx]bool),
	}
}

// checkActiveLocked verifies the node can still be used.
func (t *Tx) checkActiveLocked() error {
	if t.done || t.tree.closed {
		return ErrTxnClosed
	}
	return nil
}

// Get reads key, seeing the transaction tree's own uncommitted writes
// (this node first, then ancestors) and otherwise the committed store.
// Committed reads are cached tree-wide, so repeated reads are stable.
func (t *Tx) Get(key string) ([]byte, bool, error) {
	t.tree.mu.Lock()
	defer t.tree.mu.Unlock()
	if err := t.checkActiveLocked(); err != nil {
		return nil, false, err
	}
	for n := t; n != nil; n = n.parent {
		if w, ok := n.overlay[key]; ok {
			if w.del {
				return nil, false, nil
			}
			return append([]byte(nil), w.val...), true, nil
		}
	}
	if e, ok := t.tree.cache[key]; ok {
		return append([]byte(nil), e.val...), e.found, nil
	}
	val, ver, found := t.tree.exec.st.Get(key)
	t.tree.cache[key] = readEnt{val: val, found: found}
	t.touchLocked(key, ver)
	return val, found, nil
}

// Put stages a write of key.
func (t *Tx) Put(key string, value []byte) error {
	t.tree.mu.Lock()
	defer t.tree.mu.Unlock()
	if err := t.checkActiveLocked(); err != nil {
		return err
	}
	t.recordUndoLocked(key)
	t.overlay[key] = writeEnt{val: append([]byte(nil), value...)}
	t.touchKeyLocked(key)
	return nil
}

// Delete stages a deletion of key.
func (t *Tx) Delete(key string) error {
	t.tree.mu.Lock()
	defer t.tree.mu.Unlock()
	if err := t.checkActiveLocked(); err != nil {
		return err
	}
	t.recordUndoLocked(key)
	t.overlay[key] = writeEnt{del: true}
	t.touchKeyLocked(key)
	return nil
}

// touchKeyLocked records the committed version of key the first time the
// tree touches it, for commit-time conflict validation.
func (t *Tx) touchKeyLocked(key string) {
	if _, ok := t.tree.reads[key]; ok {
		return
	}
	_, ver, _ := t.tree.exec.st.Get(key)
	t.touchLocked(key, ver)
}

func (t *Tx) touchLocked(key string, ver uint64) {
	t.tree.reads[key] = ver
}

// recordUndoLocked pushes an undo entry for key's current overlay state.
func (t *Tx) recordUndoLocked(key string) {
	prev, had := t.overlay[key]
	t.undo = append(t.undo, undoEnt{key: key, prev: prev, hadPrev: had})
}

// Savepoint creates a named savepoint inside this transaction node.
// Re-creating an existing name shadows the older savepoint (SQL
// semantics). The savepoint is fsynced to the WAL before returning.
func (t *Tx) Savepoint(name string) error {
	t.tree.mu.Lock()
	defer t.tree.mu.Unlock()
	if err := t.checkActiveLocked(); err != nil {
		return err
	}
	if err := t.tree.exec.st.LogSavepoint(t.tree.id, name); err != nil {
		return err
	}
	if _, ok := t.sps[name]; ok {
		t.spOrder = removeName(t.spOrder, name)
	}
	t.spOrder = append(t.spOrder, name)
	childSet := make(map[*Tx]bool, len(t.children))
	for c := range t.children {
		childSet[c] = true
	}
	t.sps[name] = savepoint{undoLen: len(t.undo), childSet: childSet}
	delete(t.released, name)
	return nil
}

// RollbackTo undoes all writes of this node (and merged descendants) made
// after the named savepoint, and aborts children begun after it. Earlier
// writes are kept. Savepoints created after the target are invalidated.
func (t *Tx) RollbackTo(name string) error {
	t.tree.mu.Lock()
	defer t.tree.mu.Unlock()
	if err := t.checkActiveLocked(); err != nil {
		return err
	}
	if t.released[name] {
		return ErrSavepointReleased
	}
	sp, ok := t.sps[name]
	if !ok {
		return ErrSavepointNotFound
	}
	// Abort children begun after the savepoint; their writes never merged.
	for c := range t.children {
		if !sp.childSet[c] {
			c.abortLocked()
			delete(t.children, c)
		}
	}
	// Undo this node's writes back to the savepoint height. Writes merged
	// from children that committed after the savepoint are undone here too.
	for len(t.undo) > sp.undoLen {
		u := t.undo[len(t.undo)-1]
		t.undo = t.undo[:len(t.undo)-1]
		if u.hadPrev {
			t.overlay[u.key] = u.prev
		} else {
			delete(t.overlay, u.key)
		}
	}
	// Invalidate savepoints created after the target.
	idx := indexOf(t.spOrder, name)
	for _, n := range t.spOrder[idx+1:] {
		delete(t.sps, n)
	}
	t.spOrder = t.spOrder[:idx+1]
	return nil
}

// ReleaseSavepoint destroys a savepoint; it can no longer be rolled back
// to. Releasing twice, or releasing an unknown name, is an error.
func (t *Tx) ReleaseSavepoint(name string) error {
	t.tree.mu.Lock()
	defer t.tree.mu.Unlock()
	if err := t.checkActiveLocked(); err != nil {
		return err
	}
	if t.released[name] {
		return ErrSavepointReleased
	}
	if _, ok := t.sps[name]; !ok {
		return ErrSavepointNotFound
	}
	delete(t.sps, name)
	t.spOrder = removeName(t.spOrder, name)
	t.released[name] = true
	return nil
}

// BeginChild opens a nested child transaction. The child sees the parent's
// current uncommitted state; its commit merges into the parent and stays
// invisible to other transactions until the root commits.
func (t *Tx) BeginChild() (*Tx, error) {
	t.tree.mu.Lock()
	defer t.tree.mu.Unlock()
	if err := t.checkActiveLocked(); err != nil {
		return nil, err
	}
	if t.depth+1 > t.tree.exec.maxDepth {
		return nil, ErrMaxDepth
	}
	c := newTx(t.tree, t, t.depth+1)
	t.children[c] = true
	return c, nil
}

// Commit commits this node. For a child, its writes merge into the parent
// and stay invisible to other transactions until the root commits. For the
// root, the tree's read versions are validated and all writes are logged
// and applied atomically; a conflict rejects the commit with ErrConflict
// and rolls the whole tree back.
func (t *Tx) Commit() error {
	t.tree.mu.Lock()
	defer t.tree.mu.Unlock()
	if err := t.checkActiveLocked(); err != nil {
		return err
	}
	if t.parent != nil {
		// Child commit: merge staged writes into the parent's overlay.
		for k, w := range t.overlay {
			t.parent.recordUndoLocked(k)
			t.parent.overlay[k] = w
		}
		delete(t.parent.children, t)
		t.closeLocked()
		return nil
	}
	// Root commit: validate, log and apply atomically.
	writes := flattenWrites(t.overlay)
	err := t.tree.exec.st.Commit(t.tree.id, t.tree.reads, writes)
	t.tree.closed = true
	t.closeLocked()
	if err != nil {
		if errors.Is(err, store.ErrConflict) {
			return ErrConflict
		}
		return err
	}
	return nil
}

// Rollback aborts this node and all its descendants, discarding their
// committed and uncommitted writes. Rolling back the root additionally
// records the abort in the WAL.
func (t *Tx) Rollback() error {
	t.tree.mu.Lock()
	defer t.tree.mu.Unlock()
	if err := t.checkActiveLocked(); err != nil {
		return err
	}
	if t.parent == nil {
		t.tree.closed = true
		if err := t.tree.exec.st.LogRollback(t.tree.id); err != nil {
			return err
		}
	} else {
		delete(t.parent.children, t)
	}
	t.abortLocked()
	return nil
}

// closeLocked marks the node done and aborts still-open descendants.
func (t *Tx) closeLocked() {
	t.done = true
	for c := range t.children {
		c.abortLocked()
	}
	t.children = make(map[*Tx]bool)
}

// abortLocked discards the node's writes and aborts its descendants.
func (t *Tx) abortLocked() {
	t.closeLocked()
	t.overlay = make(map[string]writeEnt)
	t.undo = nil
	t.sps = make(map[string]savepoint)
	t.spOrder = nil
}

// flattenWrites turns the overlay into a deterministic write list.
func flattenWrites(overlay map[string]writeEnt) []wal.Write {
	keys := make([]string, 0, len(overlay))
	for k := range overlay {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	ws := make([]wal.Write, 0, len(keys))
	for _, k := range keys {
		w := overlay[k]
		ws = append(ws, wal.Write{Key: k, Value: w.val, Delete: w.del})
	}
	return ws
}

func removeName(s []string, name string) []string {
	out := s[:0]
	for _, n := range s {
		if n != name {
			out = append(out, n)
		}
	}
	return out
}

func indexOf(s []string, name string) int {
	for i, n := range s {
		if n == name {
			return i
		}
	}
	return -1
}
