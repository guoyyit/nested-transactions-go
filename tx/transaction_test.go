package tx

import (
	"errors"
	"testing"
)

func openTest(t *testing.T, opts *Options) *Manager {
	t.Helper()
	m, err := Open(t.TempDir(), opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { m.Close() })
	return m
}

func mustGet(t *testing.T, tx *Tx, key string) (string, bool) {
	t.Helper()
	v, ok, err := tx.Get(key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	return string(v), ok
}

// getCommitted reads a key through a throwaway root transaction.
func getCommitted(t *testing.T, m *Manager, key string) (string, bool) {
	t.Helper()
	tx, err := m.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback()
	return mustGet(t, tx, key)
}

func TestBasicCommitAndRead(t *testing.T) {
	m := openTest(t, nil)

	tx, _ := m.Begin()
	if err := tx.Put("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	if v, ok := getCommitted(t, m, "a"); !ok || v != "1" {
		t.Fatalf("got %q,%v want 1,true", v, ok)
	}
}

func TestRollbackDiscardsWrites(t *testing.T) {
	m := openTest(t, nil)

	tx, _ := m.Begin()
	tx.Put("a", []byte("1"))
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, ok := getCommitted(t, m, "a"); ok {
		t.Fatal("rolled-back write leaked")
	}
}

func TestReadYourWritesOnly(t *testing.T) {
	m := openTest(t, nil)

	tx, _ := m.Begin()
	tx.Put("a", []byte("1"))
	if v, ok := mustGet(t, tx, "a"); !ok || v != "1" {
		t.Fatalf("own write not visible: %q,%v", v, ok)
	}
	// Another transaction must not see the uncommitted write.
	if _, ok := getCommitted(t, m, "a"); ok {
		t.Fatal("uncommitted write visible to other transaction")
	}
	tx.Rollback()
}

func TestDelete(t *testing.T) {
	m := openTest(t, nil)

	tx, _ := m.Begin()
	tx.Put("a", []byte("1"))
	tx.Commit()

	tx2, _ := m.Begin()
	if err := tx2.Delete("a"); err != nil {
		t.Fatal(err)
	}
	if _, ok := mustGet(t, tx2, "a"); ok {
		t.Fatal("deleted key still visible inside tx")
	}
	tx2.Commit()
	if _, ok := getCommitted(t, m, "a"); ok {
		t.Fatal("deleted key still visible after commit")
	}
}

func TestSavepointPartialRollback(t *testing.T) {
	m := openTest(t, nil)

	tx, _ := m.Begin()
	tx.Put("a", []byte("1"))
	if err := tx.Savepoint("sp1"); err != nil {
		t.Fatal(err)
	}
	tx.Put("b", []byte("2"))
	tx.Savepoint("sp2")
	tx.Put("c", []byte("3"))

	// Roll back to sp1: writes b and c must vanish, a must survive.
	if err := tx.RollbackToSavepoint("sp1"); err != nil {
		t.Fatal(err)
	}
	if v, ok := mustGet(t, tx, "a"); !ok || v != "1" {
		t.Fatalf("a = %q,%v want 1,true", v, ok)
	}
	if _, ok := mustGet(t, tx, "b"); ok {
		t.Fatal("b should have been rolled back")
	}
	if _, ok := mustGet(t, tx, "c"); ok {
		t.Fatal("c should have been rolled back")
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if v, ok := getCommitted(t, m, "a"); !ok || v != "1" {
		t.Fatalf("committed a = %q,%v", v, ok)
	}
	if _, ok := getCommitted(t, m, "b"); ok {
		t.Fatal("rolled-back b was committed")
	}
}

func TestSavepointRollbackDestroysNewerSavepoints(t *testing.T) {
	m := openTest(t, nil)

	tx, _ := m.Begin()
	tx.Put("a", []byte("1"))
	tx.Savepoint("sp1")
	tx.Put("b", []byte("2"))
	tx.Savepoint("sp2")

	if err := tx.RollbackToSavepoint("sp1"); err != nil {
		t.Fatal(err)
	}
	// sp2 was created after sp1: it must be gone.
	if err := tx.RollbackToSavepoint("sp2"); !errors.Is(err, ErrSavepointNotFound) {
		t.Fatalf("rollback to destroyed sp2: %v", err)
	}
	// sp1 itself is still usable (idempotent rollback).
	if err := tx.RollbackToSavepoint("sp1"); err != nil {
		t.Fatal(err)
	}
	tx.Rollback()
}

func TestSavepointDuplicateRejected(t *testing.T) {
	m := openTest(t, nil)

	tx, _ := m.Begin()
	defer tx.Rollback()
	if err := tx.Savepoint("s"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Savepoint("s"); !errors.Is(err, ErrSavepointExists) {
		t.Fatalf("duplicate savepoint: %v", err)
	}
}

func TestSavepointReleaseLifecycle(t *testing.T) {
	m := openTest(t, nil)

	tx, _ := m.Begin()
	defer tx.Rollback()
	tx.Put("a", []byte("1"))
	tx.Savepoint("s")
	tx.Put("b", []byte("2"))

	if err := tx.ReleaseSavepoint("s"); err != nil {
		t.Fatal(err)
	}
	// Released savepoint can no longer be rolled back to.
	if err := tx.RollbackToSavepoint("s"); !errors.Is(err, ErrSavepointNotFound) {
		t.Fatalf("rollback to released savepoint: %v", err)
	}
	// Double release is explicitly rejected.
	if err := tx.ReleaseSavepoint("s"); !errors.Is(err, ErrSavepointNotFound) {
		t.Fatalf("double release: %v", err)
	}
	// Writes are untouched by the release.
	if v, ok := mustGet(t, tx, "b"); !ok || v != "2" {
		t.Fatalf("b = %q,%v want 2,true", v, ok)
	}
}

func TestSavepointLimit(t *testing.T) {
	m := openTest(t, &Options{MaxSavepoints: 2})

	tx, _ := m.Begin()
	defer tx.Rollback()
	tx.Savepoint("s1")
	tx.Savepoint("s2")
	if err := tx.Savepoint("s3"); !errors.Is(err, ErrTooManySavepoints) {
		t.Fatalf("third savepoint: %v", err)
	}
	// Releasing one frees a slot.
	tx.ReleaseSavepoint("s1")
	if err := tx.Savepoint("s3"); err != nil {
		t.Fatalf("savepoint after release: %v", err)
	}
}

func TestNestedChildCommitVisibleToParentOnly(t *testing.T) {
	m := openTest(t, nil)

	parent, _ := m.Begin()
	parent.Put("p", []byte("parent"))

	child, err := parent.Begin()
	if err != nil {
		t.Fatal(err)
	}
	child.Put("c", []byte("child"))
	if err := child.Commit(); err != nil {
		t.Fatal(err)
	}

	// Parent sees the merged child write.
	if v, ok := mustGet(t, parent, "c"); !ok || v != "child" {
		t.Fatalf("parent sees c = %q,%v", v, ok)
	}
	// The outside world does not, until the root commits.
	if _, ok := getCommitted(t, m, "c"); ok {
		t.Fatal("child write visible before root commit")
	}
	if _, ok := getCommitted(t, m, "p"); ok {
		t.Fatal("parent write visible before root commit")
	}

	if err := parent.Commit(); err != nil {
		t.Fatal(err)
	}
	if v, ok := getCommitted(t, m, "c"); !ok || v != "child" {
		t.Fatalf("after root commit c = %q,%v", v, ok)
	}
	if v, ok := getCommitted(t, m, "p"); !ok || v != "parent" {
		t.Fatalf("after root commit p = %q,%v", v, ok)
	}
}

func TestNestedChildRollback(t *testing.T) {
	m := openTest(t, nil)

	parent, _ := m.Begin()
	child, _ := parent.Begin()
	child.Put("c", []byte("child"))
	if err := child.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, ok := mustGet(t, parent, "c"); ok {
		t.Fatal("rolled-back child write visible to parent")
	}
	parent.Put("p", []byte("1"))
	if err := parent.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, ok := getCommitted(t, m, "c"); ok {
		t.Fatal("rolled-back child write committed")
	}
}

func TestParentRollbackCascades(t *testing.T) {
	m := openTest(t, nil)

	parent, _ := m.Begin()
	child, _ := parent.Begin()
	grand, _ := child.Begin()
	grand.Put("g", []byte("grand"))
	if err := grand.Commit(); err != nil { // merges into child
		t.Fatal(err)
	}
	if err := child.Commit(); err != nil { // merges into parent
		t.Fatal(err)
	}
	parent.Put("p", []byte("parent"))

	// Parent rollback must undo committed descendants' writes too.
	if err := parent.Rollback(); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"g", "p"} {
		if _, ok := getCommitted(t, m, k); ok {
			t.Fatalf("key %q survived parent rollback", k)
		}
	}
}

func TestParentCommitAutoRollsBackActiveChild(t *testing.T) {
	m := openTest(t, nil)

	parent, _ := m.Begin()
	child, _ := parent.Begin()
	child.Put("c", []byte("child"))
	parent.Put("p", []byte("parent"))

	// Committing the parent with a still-active child rolls the child back
	// rather than silently committing its work.
	if err := parent.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := child.Get("c"); !errors.Is(err, ErrTxClosed) {
		t.Fatalf("child after parent commit: %v", err)
	}
	if _, ok := getCommitted(t, m, "c"); ok {
		t.Fatal("active child's write was committed by parent")
	}
	if v, ok := getCommitted(t, m, "p"); !ok || v != "parent" {
		t.Fatalf("p = %q,%v", v, ok)
	}
}

func TestNestingDepthLimit(t *testing.T) {
	m := openTest(t, &Options{MaxDepth: 2})

	root, _ := m.Begin()
	defer root.Rollback()
	child, err := root.Begin()
	if err != nil {
		t.Fatalf("depth 2 should be allowed: %v", err)
	}
	if _, err := child.Begin(); !errors.Is(err, ErrMaxDepth) {
		t.Fatalf("depth 3: %v", err)
	}
}

func TestSavepointRollsBackLaterChildren(t *testing.T) {
	m := openTest(t, nil)

	parent, _ := m.Begin()
	defer parent.Rollback()

	// Child begun BEFORE the savepoint survives the rollback.
	early, _ := parent.Begin()
	parent.Put("a", []byte("1"))
	parent.Savepoint("sp")

	// Child begun AFTER the savepoint is force-rolled-back.
	late, _ := parent.Begin()
	late.Put("x", []byte("x"))
	parent.Put("b", []byte("2"))

	if err := parent.RollbackToSavepoint("sp"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := late.Get("x"); !errors.Is(err, ErrTxClosed) {
		t.Fatalf("late child should be rolled back: %v", err)
	}
	if _, _, err := early.Get("whatever"); err != nil {
		t.Fatalf("early child should survive: %v", err)
	}
	if v, ok := mustGet(t, parent, "a"); !ok || v != "1" {
		t.Fatalf("a = %q,%v", v, ok)
	}
	if _, ok := mustGet(t, parent, "b"); ok {
		t.Fatal("b should be rolled back")
	}
}

func TestThreeLevelNesting(t *testing.T) {
	m := openTest(t, nil)

	root, _ := m.Begin()
	child, _ := root.Begin()
	grand, _ := child.Begin()

	grand.Put("k", []byte("grand"))
	if err := grand.Commit(); err != nil {
		t.Fatal(err)
	}
	// Merged into the child: visible to the child (its parent)...
	if v, ok := mustGet(t, child, "k"); !ok || v != "grand" {
		t.Fatalf("child sees k = %q,%v", v, ok)
	}
	// ...but not yet to the root: writes propagate one level per commit.
	if _, ok := mustGet(t, root, "k"); ok {
		t.Fatal("root should not see grandchild write before child commits")
	}
	// Still invisible outside.
	if _, ok := getCommitted(t, m, "k"); ok {
		t.Fatal("grandchild write visible before root commit")
	}

	child.Commit()
	// Now the child's (merged) writes are visible to the root.
	if v, ok := mustGet(t, root, "k"); !ok || v != "grand" {
		t.Fatalf("root sees k = %q,%v", v, ok)
	}
	root.Commit()
	if v, ok := getCommitted(t, m, "k"); !ok || v != "grand" {
		t.Fatalf("after root commit k = %q,%v", v, ok)
	}
}

func TestClosedTxRejectsOps(t *testing.T) {
	m := openTest(t, nil)

	tx, _ := m.Begin()
	tx.Savepoint("s")
	tx.Commit()

	if err := tx.Put("a", []byte("1")); !errors.Is(err, ErrTxClosed) {
		t.Fatalf("Put on closed tx: %v", err)
	}
	if _, _, err := tx.Get("a"); !errors.Is(err, ErrTxClosed) {
		t.Fatalf("Get on closed tx: %v", err)
	}
	if err := tx.Savepoint("s2"); !errors.Is(err, ErrTxClosed) {
		t.Fatalf("Savepoint on closed tx: %v", err)
	}
	if err := tx.RollbackToSavepoint("s"); !errors.Is(err, ErrTxClosed) {
		t.Fatalf("RollbackToSavepoint on closed tx: %v", err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrTxClosed) {
		t.Fatalf("double commit: %v", err)
	}
	if _, err := tx.Begin(); !errors.Is(err, ErrTxClosed) {
		t.Fatalf("Begin on closed tx: %v", err)
	}
}

// A savepoint must undo writes merged in by a child committed after the
// savepoint was taken, even though the child itself is already "committed"
// (its commit is only local to the parent until the root commits).
func TestSavepointUndoesCommittedChildWrites(t *testing.T) {
	m := openTest(t, nil)

	parent, _ := m.Begin()
	defer parent.Rollback()
	parent.Put("a", []byte("1"))
	if err := parent.Savepoint("sp"); err != nil {
		t.Fatal(err)
	}

	child, _ := parent.Begin()
	child.Put("c", []byte("child"))
	if err := child.Commit(); err != nil {
		t.Fatal(err)
	}
	parent.Put("b", []byte("2"))

	if v, ok := mustGet(t, parent, "c"); !ok || v != "child" {
		t.Fatalf("before rollback c = %q,%v", v, ok)
	}
	if err := parent.RollbackToSavepoint("sp"); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"b", "c"} {
		if _, ok := mustGet(t, parent, k); ok {
			t.Fatalf("key %q survived savepoint rollback", k)
		}
	}
	if v, ok := mustGet(t, parent, "a"); !ok || v != "1" {
		t.Fatalf("a = %q,%v want 1,true", v, ok)
	}
}

func TestNestedVisibilityRules(t *testing.T) {
	m := openTest(t, nil)

	parent, _ := m.Begin()
	defer parent.Rollback()
	parent.Put("p", []byte("parent"))

	child, err := parent.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer child.Rollback()

	// Child sees parent's uncommitted writes.
	if v, ok := mustGet(t, child, "p"); !ok || v != "parent" {
		t.Fatalf("child sees p = %q,%v want parent,true", v, ok)
	}
	// Parent does NOT see the child's uncommitted writes.
	child.Put("c", []byte("child"))
	if _, ok := mustGet(t, parent, "c"); ok {
		t.Fatal("parent must not see uncommitted child write")
	}
	// Child override shadows parent's value for itself.
	child.Put("p", []byte("override"))
	if v, _ := mustGet(t, child, "p"); v != "override" {
		t.Fatalf("child p = %q want override", v)
	}
	if v, _ := mustGet(t, parent, "p"); v != "parent" {
		t.Fatalf("parent p = %q want parent", v)
	}
}
