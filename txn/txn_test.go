package txn

import (
	"errors"
	"testing"
)

func open(t *testing.T, opts ...Option) *Executor {
	t.Helper()
	e, err := Open(t.TempDir(), opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

func mustGet(t *testing.T, tx *Tx, key string) (string, bool) {
	t.Helper()
	v, ok, err := tx.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	return string(v), ok
}

func TestBasicCommitVisible(t *testing.T) {
	e := open(t)
	tx, _ := e.Begin()
	tx.Put("a", []byte("1"))
	tx.Put("b", []byte("2"))
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tx2, _ := e.Begin()
	defer tx2.Rollback()
	if v, ok := mustGet(t, tx2, "a"); !ok || v != "1" {
		t.Fatalf("a = %q %v", v, ok)
	}
}

func TestUncommittedInvisible(t *testing.T) {
	e := open(t)
	tx, _ := e.Begin()
	tx.Put("a", []byte("1"))

	other, _ := e.Begin()
	defer other.Rollback()
	if _, ok := mustGet(t, other, "a"); ok {
		t.Fatal("uncommitted write visible to another transaction")
	}
	// ...but visible to itself.
	if v, ok := mustGet(t, tx, "a"); !ok || v != "1" {
		t.Fatalf("own write not visible: %q %v", v, ok)
	}
	tx.Rollback()
	if _, ok := mustGet(t, other, "a"); ok {
		t.Fatal("rolled-back write visible")
	}
}

func TestSavepointPartialRollback(t *testing.T) {
	e := open(t)
	tx, _ := e.Begin()
	tx.Put("a", []byte("1"))
	tx.Savepoint("sp1")
	tx.Put("b", []byte("2"))
	tx.Savepoint("sp2")
	tx.Put("c", []byte("3"))

	// Roll back only the writes after sp2.
	if err := tx.RollbackTo("sp2"); err != nil {
		t.Fatal(err)
	}
	if _, ok := mustGet(t, tx, "c"); ok {
		t.Fatal("c should be undone")
	}
	if v, ok := mustGet(t, tx, "b"); !ok || v != "2" {
		t.Fatalf("b must survive: %q %v", v, ok)
	}

	// Roll back to sp1: b goes too, a stays.
	if err := tx.RollbackTo("sp1"); err != nil {
		t.Fatal(err)
	}
	if _, ok := mustGet(t, tx, "b"); ok {
		t.Fatal("b should be undone")
	}
	if v, ok := mustGet(t, tx, "a"); !ok || v != "1" {
		t.Fatalf("a must survive: %q %v", v, ok)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	check, _ := e.Begin()
	defer check.Rollback()
	if v, ok := mustGet(t, check, "a"); !ok || v != "1" {
		t.Fatalf("committed a = %q %v", v, ok)
	}
	if _, ok := mustGet(t, check, "b"); ok {
		t.Fatal("rolled-back b leaked into commit")
	}
	if _, ok := mustGet(t, check, "c"); ok {
		t.Fatal("rolled-back c leaked into commit")
	}
}

func TestSavepointOverwriteAndDelete(t *testing.T) {
	e := open(t)
	tx, _ := e.Begin()
	tx.Put("k", []byte("v1"))
	tx.Savepoint("sp")
	tx.Put("k", []byte("v2"))
	tx.Delete("k")
	tx.RollbackTo("sp")
	if v, ok := mustGet(t, tx, "k"); !ok || v != "v1" {
		t.Fatalf("rollback must restore v1, got %q %v", v, ok)
	}
	tx.Commit()
}

func TestSavepointInvalidatesLaterOnes(t *testing.T) {
	e := open(t)
	tx, _ := e.Begin()
	tx.Savepoint("s1")
	tx.Put("x", []byte("1"))
	tx.Savepoint("s2")
	tx.RollbackTo("s1")
	// s2 was created after s1: it is now invalid.
	if err := tx.RollbackTo("s2"); !errors.Is(err, ErrSavepointNotFound) {
		t.Fatalf("later savepoint must be invalidated, got %v", err)
	}
	tx.Rollback()
}

func TestSavepointShadowing(t *testing.T) {
	e := open(t)
	tx, _ := e.Begin()
	tx.Put("k", []byte("v1"))
	tx.Savepoint("sp")
	tx.Put("k", []byte("v2"))
	tx.Savepoint("sp") // shadows the first
	tx.RollbackTo("sp")
	if v, ok := mustGet(t, tx, "k"); !ok || v != "v2" {
		t.Fatalf("shadowing savepoint must capture v2, got %q %v", v, ok)
	}
	tx.Rollback()
}

func TestSavepointReleaseLifecycle(t *testing.T) {
	e := open(t)
	tx, _ := e.Begin()
	tx.Savepoint("sp")
	if err := tx.ReleaseSavepoint("sp"); err != nil {
		t.Fatal(err)
	}
	// Double release is rejected.
	if err := tx.ReleaseSavepoint("sp"); !errors.Is(err, ErrSavepointReleased) {
		t.Fatalf("double release must fail, got %v", err)
	}
	// Rollback to a released savepoint is rejected.
	if err := tx.RollbackTo("sp"); !errors.Is(err, ErrSavepointReleased) {
		t.Fatalf("rollback to released savepoint must fail, got %v", err)
	}
	// Releasing a savepoint that never existed is rejected.
	if err := tx.ReleaseSavepoint("nope"); !errors.Is(err, ErrSavepointNotFound) {
		t.Fatalf("release of unknown savepoint must fail, got %v", err)
	}
	// The name may be reused after release.
	if err := tx.Savepoint("sp"); err != nil {
		t.Fatal(err)
	}
	tx.Rollback()
}

func TestClosedTxnRejected(t *testing.T) {
	e := open(t)
	tx, _ := e.Begin()
	tx.Put("a", []byte("1"))
	tx.Commit()
	if err := tx.Put("b", []byte("2")); !errors.Is(err, ErrTxnClosed) {
		t.Fatalf("use after commit must fail, got %v", err)
	}
	if _, _, err := tx.Get("a"); !errors.Is(err, ErrTxnClosed) {
		t.Fatalf("read after commit must fail, got %v", err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrTxnClosed) {
		t.Fatalf("double commit must fail, got %v", err)
	}
}

func TestRunHelperRetries(t *testing.T) {
	e := open(t)
	calls := 0
	err := e.Run(func(tx *Tx) error {
		calls++
		return tx.Put("k", []byte("v"))
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("fn called %d times, want 1", calls)
	}
}
