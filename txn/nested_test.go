package txn

import (
	"errors"
	"testing"
)

func TestChildCommitMergesToParent(t *testing.T) {
	e := open(t)
	tx, _ := e.Begin()
	tx.Put("parent", []byte("p"))

	child, _ := tx.BeginChild()
	child.Put("child", []byte("c"))
	// Child sees parent's uncommitted write.
	if v, ok := mustGet(t, child, "parent"); !ok || v != "p" {
		t.Fatalf("child must see parent write: %q %v", v, ok)
	}
	if err := child.Commit(); err != nil {
		t.Fatal(err)
	}

	// Merged into parent...
	if v, ok := mustGet(t, tx, "child"); !ok || v != "c" {
		t.Fatalf("parent must see committed child write: %q %v", v, ok)
	}
	// ...but still invisible to other transactions.
	other, _ := e.Begin()
	defer other.Rollback()
	if _, ok := mustGet(t, other, "child"); ok {
		t.Fatal("child write visible before root commit")
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	after, _ := e.Begin()
	defer after.Rollback()
	if v, ok := mustGet(t, after, "child"); !ok || v != "c" {
		t.Fatalf("child write must be visible after root commit: %q %v", v, ok)
	}
}

func TestChildRollbackDiscardsOnlyChild(t *testing.T) {
	e := open(t)
	tx, _ := e.Begin()
	tx.Put("parent", []byte("p"))

	child, _ := tx.BeginChild()
	child.Put("child", []byte("c"))
	if err := child.Rollback(); err != nil {
		t.Fatal(err)
	}

	if _, ok := mustGet(t, tx, "child"); ok {
		t.Fatal("rolled-back child write leaked into parent")
	}
	if v, ok := mustGet(t, tx, "parent"); !ok || v != "p" {
		t.Fatalf("parent write must survive child rollback: %q %v", v, ok)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestParentRollbackUndoesCommittedChildren(t *testing.T) {
	e := open(t)
	tx, _ := e.Begin()
	child, _ := tx.BeginChild()
	child.Put("a", []byte("1"))
	if err := child.Commit(); err != nil {
		t.Fatal(err)
	}
	grand, _ := child2(t, tx)
	grand.Put("b", []byte("2"))
	if err := grand.Commit(); err != nil {
		t.Fatal(err)
	}

	// Parent (root) rollback must discard everything, including the
	// already-committed child and grandchild.
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	check, _ := e.Begin()
	defer check.Rollback()
	if _, ok := mustGet(t, check, "a"); ok {
		t.Fatal("committed child write survived root rollback")
	}
	if _, ok := mustGet(t, check, "b"); ok {
		t.Fatal("committed grandchild write survived root rollback")
	}
}

func child2(t *testing.T, tx *Tx) (*Tx, *Tx) {
	t.Helper()
	c, err := tx.BeginChild()
	if err != nil {
		t.Fatal(err)
	}
	g, err := c.BeginChild()
	if err != nil {
		t.Fatal(err)
	}
	return c, g
}

func TestDeepNestingCommitChain(t *testing.T) {
	e := open(t)
	tx, _ := e.Begin()
	tx.Put("l1", []byte("1"))
	c1, _ := tx.BeginChild()
	c1.Put("l2", []byte("2"))
	c2, _ := c1.BeginChild()
	c2.Put("l3", []byte("3"))
	c3, _ := c2.BeginChild()
	c3.Put("l4", []byte("4"))

	// Commit bottom-up.
	if err := c3.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := c2.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := c1.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	check, _ := e.Begin()
	defer check.Rollback()
	for _, kv := range [][2]string{{"l1", "1"}, {"l2", "2"}, {"l3", "3"}, {"l4", "4"}} {
		if v, ok := mustGet(t, check, kv[0]); !ok || v != kv[1] {
			t.Fatalf("%s = %q %v, want %s", kv[0], v, ok, kv[1])
		}
	}
}

func TestMiddleChildRollbackCutsSubtree(t *testing.T) {
	e := open(t)
	tx, _ := e.Begin()
	tx.Put("root", []byte("r"))
	c1, _ := tx.BeginChild()
	c1.Put("c1", []byte("1"))
	c2, _ := c1.BeginChild()
	c2.Put("c2", []byte("2"))
	if err := c2.Commit(); err != nil { // c2 merges into c1
		t.Fatal(err)
	}
	// c1 rolls back: must discard c2's merged commit too.
	if err := c1.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	check, _ := e.Begin()
	defer check.Rollback()
	if v, ok := mustGet(t, check, "root"); !ok || v != "r" {
		t.Fatalf("root write lost: %q %v", v, ok)
	}
	if _, ok := mustGet(t, check, "c1"); ok {
		t.Fatal("c1 leaked")
	}
	if _, ok := mustGet(t, check, "c2"); ok {
		t.Fatal("c2's commit must be undone by c1's rollback")
	}
}

func TestMaxDepthEnforced(t *testing.T) {
	e := open(t, WithMaxDepth(3))
	tx, _ := e.Begin()
	defer tx.Rollback()
	c1, err := tx.BeginChild() // depth 2
	if err != nil {
		t.Fatal(err)
	}
	c2, err := c1.BeginChild() // depth 3
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c2.BeginChild(); !errors.Is(err, ErrMaxDepth) { // depth 4
		t.Fatalf("depth 4 must be rejected, got %v", err)
	}
}

func TestSavepointUndoesChildMerge(t *testing.T) {
	e := open(t)
	tx, _ := e.Begin()
	tx.Put("before", []byte("b"))
	tx.Savepoint("sp")

	child, _ := tx.BeginChild()
	child.Put("childkey", []byte("c"))
	if err := child.Commit(); err != nil { // merged into root's overlay
		t.Fatal(err)
	}

	// Rolling back to sp must undo the merged child commit.
	if err := tx.RollbackTo("sp"); err != nil {
		t.Fatal(err)
	}
	if _, ok := mustGet(t, tx, "childkey"); ok {
		t.Fatal("savepoint rollback must undo merged child writes")
	}
	if v, ok := mustGet(t, tx, "before"); !ok || v != "b" {
		t.Fatalf("earlier write must survive: %q %v", v, ok)
	}
	tx.Commit()

	check, _ := e.Begin()
	defer check.Rollback()
	if _, ok := mustGet(t, check, "childkey"); ok {
		t.Fatal("undone child write leaked into committed state")
	}
}

func TestSavepointAbortsLaterBegunChild(t *testing.T) {
	e := open(t)
	tx, _ := e.Begin()
	tx.Savepoint("sp")
	child, _ := tx.BeginChild()
	child.Put("k", []byte("v"))

	// The child was begun after sp: rolling back to sp aborts it.
	if err := tx.RollbackTo("sp"); err != nil {
		t.Fatal(err)
	}
	if err := child.Put("x", []byte("y")); !errors.Is(err, ErrTxnClosed) {
		t.Fatalf("child begun after savepoint must be aborted, got %v", err)
	}
	tx.Rollback()
}

func TestChildSeesParentLiveWrites(t *testing.T) {
	e := open(t)
	tx, _ := e.Begin()
	child, _ := tx.BeginChild()
	tx.Put("late", []byte("l")) // parent writes after child began
	if v, ok := mustGet(t, child, "late"); !ok || v != "l" {
		t.Fatalf("child must see parent's live writes: %q %v", v, ok)
	}
	tx.Rollback()
}

func TestParentCommitAbortsOpenChild(t *testing.T) {
	e := open(t)
	tx, _ := e.Begin()
	child, _ := tx.BeginChild()
	child.Put("k", []byte("v"))
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	// The still-open child is aborted by the root commit.
	if err := child.Put("x", []byte("y")); !errors.Is(err, ErrTxnClosed) {
		t.Fatalf("open child must be closed by root commit, got %v", err)
	}
	check, _ := e.Begin()
	defer check.Rollback()
	if _, ok := mustGet(t, check, "k"); ok {
		t.Fatal("uncommitted child write must not be committed by root")
	}
}
