// Package store implements the flat, single-layer transactional storage
// layer: committed key-value state held in memory and made durable through
// a write-ahead log. The nested transaction executor (package txn) builds
// savepoints and nested transactions on top of it.
//
// Durability rules:
//   - every mutation record is fsynced to the WAL before it is applied to
//     the in-memory committed state;
//   - on Open the WAL is replayed: committed transactions are applied,
//     incomplete or rolled-back ones leave no trace.
package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/guoyyit/nested-transactions-go/wal"
)

// ErrConflict is returned by Commit when a validated version no longer
// matches the committed state.
var ErrConflict = errors.New("store: version conflict")

type entry struct {
	val []byte
	ver uint64
}

// Store holds the committed key-value state.
type Store struct {
	mu      sync.RWMutex
	walf    *wal.File
	data    map[string]entry
	ver     uint64 // global version counter, bumped per applied write
	nextTxn uint64 // next transaction id to hand out
}

// Open opens the store rooted at dir, replaying the WAL to recover the
// committed state. Incomplete transactions are discarded.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	walf, err := wal.Open(filepath.Join(dir, "wal.log"))
	if err != nil {
		return nil, err
	}
	s := &Store{walf: walf, data: make(map[string]entry), nextTxn: 1}
	for _, rec := range walf.Records() {
		switch rec.Type {
		case wal.RecordSnapshot:
			s.data = make(map[string]entry)
			for _, w := range rec.Writes {
				s.applyLocked(w)
			}
			s.nextTxn = rec.TxnID
		case wal.RecordCommit:
			for _, w := range rec.Writes {
				s.applyLocked(w)
			}
		}
		if rec.TxnID >= s.nextTxn {
			s.nextTxn = rec.TxnID + 1
		}
	}
	return s, nil
}

// Close closes the store and its WAL.
func (s *Store) Close() error { return s.walf.Close() }

// NextTxnID returns a fresh transaction id.
func (s *Store) NextTxnID() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.nextTxn
	s.nextTxn++
	return id
}

// Get returns the committed value and version of key. A missing key
// reports version 0 and found=false.
func (s *Store) Get(key string) (val []byte, ver uint64, found bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	if !ok {
		return nil, 0, false
	}
	return append([]byte(nil), e.val...), e.ver, true
}

// LogBegin durably records that txnID began.
func (s *Store) LogBegin(txnID uint64) error {
	return s.walf.Append(wal.Record{Type: wal.RecordBegin, TxnID: txnID})
}

// LogSavepoint durably records creation of a savepoint under txnID.
func (s *Store) LogSavepoint(txnID uint64, name string) error {
	return s.walf.Append(wal.Record{Type: wal.RecordSavepoint, TxnID: txnID, Name: name})
}

// LogRollback durably records that txnID rolled back.
func (s *Store) LogRollback(txnID uint64) error {
	return s.walf.Append(wal.Record{Type: wal.RecordRollback, TxnID: txnID})
}

// Commit first validates that every key in validate still has the recorded
// version, then fsyncs the commit record and applies the writes — all
// under one lock, so the commit is atomic and serializable by commit order.
func (s *Store) Commit(txnID uint64, validate map[string]uint64, writes []wal.Write) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, v := range validate {
		if cur := s.data[k].ver; cur != v {
			return fmt.Errorf("%w: key %q changed (version %d != %d)", ErrConflict, k, cur, v)
		}
	}
	if err := s.walf.Append(wal.Record{Type: wal.RecordCommit, TxnID: txnID, Writes: writes}); err != nil {
		return err
	}
	for _, w := range writes {
		s.applyLocked(w)
	}
	return nil
}

// Compact rewrites the WAL as a single snapshot of the committed state.
func (s *Store) Compact() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	writes := make([]wal.Write, 0, len(s.data))
	for k, e := range s.data {
		writes = append(writes, wal.Write{Key: k, Value: append([]byte(nil), e.val...)})
	}
	return s.walf.Compact(s.nextTxn, writes)
}

// applyLocked applies one write to the committed state, bumping versions.
func (s *Store) applyLocked(w wal.Write) {
	s.ver++
	if w.Delete {
		delete(s.data, w.Key)
		return
	}
	s.data[w.Key] = entry{val: append([]byte(nil), w.Value...), ver: s.ver}
}
