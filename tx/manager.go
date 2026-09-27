package tx

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const walFileName = "wal.log"

// Manager owns the committed store, the WAL, and the set of active root
// transactions. Use Open to create or recover one.
//
// Isolation: transactions run under optimistic concurrency control. Reads of
// committed state record key versions; at root commit the read set is
// validated and, if still valid, the write set is persisted and published
// atomically. Committed histories are therefore equivalent to the serial
// order of root commits. A losing transaction gets ErrConflict and may be
// retried (Manager.Run does this automatically).
type Manager struct {
	opts  Options
	dir   string
	store *committedStore
	wal   *wal

	commitMu sync.Mutex // serializes root commit validate+log+apply

	activeMu sync.Mutex
	active   map[uint64]*Tx // active root transactions
	closed   bool

	nextTxID atomic.Uint64
}

// Open creates or recovers a Manager rooted at dir. On recovery it replays
// all committed write sets from the WAL in log order; incomplete
// transactions are discarded (their writes never reached the store), and a
// torn tail record left by a crash is truncated.
func Open(dir string, opts *Options) (*Manager, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	o := Options{}.withDefaults()
	if opts != nil {
		o = opts.withDefaults()
	}
	walPath := filepath.Join(dir, walFileName)
	rec, err := recoverWAL(walPath)
	if err != nil {
		return nil, err
	}
	store := newCommittedStore()
	for _, ops := range rec.commits {
		store.apply(ops)
	}
	w, err := openWAL(walPath)
	if err != nil {
		return nil, err
	}
	m := &Manager{
		opts:   o,
		dir:    dir,
		store:  store,
		wal:    w,
		active: make(map[uint64]*Tx),
	}
	m.nextTxID.Store(rec.maxTxID)
	return m, nil
}

// Begin opens a root transaction.
func (m *Manager) Begin() (*Tx, error) {
	m.activeMu.Lock()
	defer m.activeMu.Unlock()
	if m.closed {
		return nil, ErrManagerClosed
	}
	tx := newTx(m, m.nextTxID.Add(1), nil, 1)
	if err := m.wal.logBegin(tx.id, 0); err != nil {
		return nil, err
	}
	m.active[tx.id] = tx
	return tx, nil
}

// Run executes fn inside a root transaction and commits it. If commit fails
// with ErrConflict, the whole function is retried with backoff, up to
// Options.MaxRetries attempts. A non-nil error from fn aborts the
// transaction and is returned as-is.
func (m *Manager) Run(ctx context.Context, fn func(*Tx) error) error {
	var err error
	for attempt := 0; attempt <= m.opts.MaxRetries; attempt++ {
		if err = ctx.Err(); err != nil {
			return err
		}
		var tx *Tx
		tx, err = m.Begin()
		if err != nil {
			return err
		}
		if err = fn(tx); err != nil {
			_ = tx.Rollback()
			return err
		}
		err = tx.Commit()
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrConflict) {
			return err
		}
		// Conflict: back off (capped) and retry with a fresh transaction.
		delay := m.opts.RetryBaseBackoff << attempt
		if max := m.opts.RetryBaseBackoff << 6; delay > max {
			delay = max
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return fmt.Errorf("tx: retries exhausted: %w", err)
}

// Close rolls back all active root transactions (cascading to their
// descendants) and closes the WAL. Committed data is already durable.
func (m *Manager) Close() error {
	m.activeMu.Lock()
	if m.closed {
		m.activeMu.Unlock()
		return nil
	}
	m.closed = true
	roots := make([]*Tx, 0, len(m.active))
	for _, tx := range m.active {
		roots = append(roots, tx)
	}
	m.activeMu.Unlock()

	var firstErr error
	for _, tx := range roots {
		if err := tx.Rollback(); err != nil && !errors.Is(err, ErrTxClosed) && firstErr == nil {
			firstErr = err
		}
	}
	if err := m.wal.close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// unregister removes a finished root transaction from the active set.
func (m *Manager) unregister(id uint64) {
	m.activeMu.Lock()
	delete(m.active, id)
	m.activeMu.Unlock()
}
