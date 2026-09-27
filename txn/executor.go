// Package txn implements a transaction executor with named savepoints and
// nested transactions on top of the flat storage layer (package store).
//
// Isolation: transactions are serializable. Each transaction tree records
// the committed version of every key it touches; at root commit the
// versions are validated and the writes are logged and applied under a
// single lock, so committed transactions are equivalent to the serial
// order of their commit points. A conflicting commit is rejected with
// ErrConflict and the whole tree is rolled back; callers may retry
// (Executor.Run does so automatically).
package txn

import (
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/guoyyit/nested-transactions-go/store"
)

const (
	// DefaultMaxDepth is the default maximum transaction nesting depth.
	DefaultMaxDepth = 8
	// DefaultMaxRetries is the default conflict retry budget for Run.
	DefaultMaxRetries = 10
)

// Executor runs nested transactions on top of a store.Store.
type Executor struct {
	st         *store.Store
	maxDepth   int
	maxRetries int
}

// Option configures an Executor.
type Option func(*Executor)

// WithMaxDepth sets the maximum transaction nesting depth (root = 1).
func WithMaxDepth(d int) Option { return func(e *Executor) { e.maxDepth = d } }

// WithMaxRetries sets the conflict retry budget used by Run.
func WithMaxRetries(n int) Option { return func(e *Executor) { e.maxRetries = n } }

// Open opens an Executor rooted at dir, recovering committed state from
// the WAL. Transactions that were incomplete at crash time are discarded.
func Open(dir string, opts ...Option) (*Executor, error) {
	st, err := store.Open(dir)
	if err != nil {
		return nil, err
	}
	e := &Executor{st: st, maxDepth: DefaultMaxDepth, maxRetries: DefaultMaxRetries}
	for _, o := range opts {
		o(e)
	}
	return e, nil
}

// Close closes the executor and its storage.
func (e *Executor) Close() error { return e.st.Close() }

// Compact rewrites the WAL as a single snapshot of the committed state.
func (e *Executor) Compact() error { return e.st.Compact() }

// Begin starts a new root transaction.
func (e *Executor) Begin() (*Tx, error) {
	id := e.st.NextTxnID()
	if err := e.st.LogBegin(id); err != nil {
		return nil, err
	}
	tr := &treeState{
		exec:  e,
		id:    id,
		reads: make(map[string]uint64),
		cache: make(map[string]readEnt),
	}
	return newTx(tr, nil, 1), nil
}

// Run executes fn inside a root transaction and commits it. If the commit
// fails with a conflict, the whole function is retried with a fresh
// transaction, up to the configured budget, with exponential backoff and
// jitter between attempts. fn must therefore be safe to re-execute. Any
// non-conflict error aborts the transaction and is returned to the caller.
func (e *Executor) Run(fn func(*Tx) error) error {
	for attempt := 0; attempt <= e.maxRetries; attempt++ {
		if attempt > 0 {
			backoffSleep(attempt)
		}
		tx, err := e.Begin()
		if err != nil {
			return err
		}
		if err := fn(tx); err != nil {
			tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			if errors.Is(err, ErrConflict) {
				continue
			}
			return err
		}
		return nil
	}
	return fmt.Errorf("%w (budget %d)", ErrTooManyRetries, e.maxRetries)
}

// backoffSleep waits a jittered exponential backoff for the given
// (1-based) retry attempt.
func backoffSleep(attempt int) {
	d := time.Millisecond << uint(attempt-1)
	if d > 64*time.Millisecond {
		d = 64 * time.Millisecond
	}
	time.Sleep(d/2 + time.Duration(rand.Int63n(int64(d))))
}
