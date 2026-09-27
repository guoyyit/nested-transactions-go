package store

import (
	"errors"
	"testing"

	"github.com/guoyyit/nested-transactions-go/wal"
)

func TestCommitAndRecover(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	id1 := s.NextTxnID()
	s.LogBegin(id1)
	if err := s.Commit(id1, nil, []wal.Write{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("2")},
	}); err != nil {
		t.Fatal(err)
	}
	// An incomplete transaction: begun, never committed.
	id2 := s.NextTxnID()
	s.LogBegin(id2)
	s.LogSavepoint(id2, "sp")
	// Simulate crash: do NOT close cleanly via rollback; just close the file.
	s.Close()

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if v, _, ok := s2.Get("a"); !ok || string(v) != "1" {
		t.Fatalf("committed write lost: %q %v", v, ok)
	}
	if _, _, ok := s2.Get("zzz"); ok {
		t.Fatal("phantom key")
	}
	// Transaction ids must not be reused after recovery.
	if id := s2.NextTxnID(); id <= id2 {
		t.Fatalf("txn id reused after recovery: %d <= %d", id, id2)
	}
}

func TestRolledBackLeavesNoTrace(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	id := s.NextTxnID()
	s.LogBegin(id)
	s.LogRollback(id)
	s.Close()

	s2, _ := Open(dir)
	defer s2.Close()
	if _, _, ok := s2.Get("a"); ok {
		t.Fatal("rolled-back write leaked")
	}
}

func TestCommitValidation(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	defer s.Close()
	id1 := s.NextTxnID()
	s.Commit(id1, nil, []wal.Write{{Key: "k", Value: []byte("v1")}})
	_, ver, _ := s.Get("k")

	// A commit validating the stale version must fail...
	id2 := s.NextTxnID()
	s.LogBegin(id2)
	if err := s.Commit(id2, map[string]uint64{"k": ver}, []wal.Write{{Key: "k", Value: []byte("v2")}}); err != nil {
		t.Fatal(err)
	}
	id3 := s.NextTxnID()
	s.LogBegin(id3)
	err := s.Commit(id3, map[string]uint64{"k": ver}, []wal.Write{{Key: "k", Value: []byte("v3")}})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("want ErrConflict, got %v", err)
	}
	// ...and the failed commit must not have applied anything.
	v, _, _ := s.Get("k")
	if string(v) != "v2" {
		t.Fatalf("failed commit applied writes: %q", v)
	}
}

func TestCompactKeepsState(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	id := s.NextTxnID()
	s.Commit(id, nil, []wal.Write{{Key: "x", Value: []byte("9")}})
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s2, _ := Open(dir)
	defer s2.Close()
	if v, _, ok := s2.Get("x"); !ok || string(v) != "9" {
		t.Fatalf("state lost across compact: %q %v", v, ok)
	}
}
