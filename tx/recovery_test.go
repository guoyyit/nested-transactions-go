package tx

import (
	"os"
	"path/filepath"
	"testing"
)

// reopen simulates a crash + restart: the old manager is abandoned without
// Close (its in-flight transactions simply vanish, as in a process crash)
// and a fresh manager recovers from the same directory.
func reopen(t *testing.T, dir string) *Manager {
	t.Helper()
	m, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { m.Close() })
	return m
}

func TestRecoverCommittedData(t *testing.T) {
	dir := t.TempDir()

	m1, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	tx, _ := m1.Begin()
	tx.Put("a", []byte("1"))
	tx.Put("b", []byte("2"))
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	// Crash: no Close.
	_ = m1

	m2 := reopen(t, dir)
	if v, ok := getCommitted(t, m2, "a"); !ok || v != "1" {
		t.Fatalf("a = %q,%v after recovery", v, ok)
	}
	if v, ok := getCommitted(t, m2, "b"); !ok || v != "2" {
		t.Fatalf("b = %q,%v after recovery", v, ok)
	}
}

func TestRecoverDiscardsUncommitted(t *testing.T) {
	dir := t.TempDir()

	m1, _ := Open(dir, nil)
	tx, _ := m1.Begin()
	tx.Put("committed", []byte("yes"))
	tx.Commit()

	// Uncommitted transaction, still active at "crash".
	tx2, _ := m1.Begin()
	tx2.Put("pending", []byte("no"))
	tx2.Savepoint("sp")
	tx2.Put("pending2", []byte("no"))

	m2 := reopen(t, dir)
	if v, ok := getCommitted(t, m2, "committed"); !ok || v != "yes" {
		t.Fatalf("committed write lost: %q,%v", v, ok)
	}
	for _, k := range []string{"pending", "pending2"} {
		if _, ok := getCommitted(t, m2, k); ok {
			t.Fatalf("uncommitted key %q survived recovery", k)
		}
	}
}

func TestRecoverNestedCommit(t *testing.T) {
	dir := t.TempDir()

	m1, _ := Open(dir, nil)
	root, _ := m1.Begin()
	root.Put("r", []byte("root"))
	child, _ := root.Begin()
	child.Put("c", []byte("child"))
	grand, _ := child.Begin()
	grand.Put("g", []byte("grand"))
	grand.Commit()
	child.Commit()
	root.Savepoint("sp")
	root.Put("r2", []byte("root2"))
	root.Commit()
	// Crash.

	m2 := reopen(t, dir)
	for k, want := range map[string]string{"r": "root", "c": "child", "g": "grand", "r2": "root2"} {
		if v, ok := getCommitted(t, m2, k); !ok || v != want {
			t.Fatalf("%s = %q,%v want %q", k, v, ok, want)
		}
	}
}

func TestRecoverSavepointRollbackState(t *testing.T) {
	dir := t.TempDir()

	m1, _ := Open(dir, nil)
	tx, _ := m1.Begin()
	tx.Put("keep", []byte("1"))
	tx.Savepoint("sp")
	tx.Put("drop", []byte("2"))
	tx.RollbackToSavepoint("sp")
	tx.Commit() // only "keep" is in the commit record
	// Crash.

	m2 := reopen(t, dir)
	if v, ok := getCommitted(t, m2, "keep"); !ok || v != "1" {
		t.Fatalf("keep = %q,%v", v, ok)
	}
	if _, ok := getCommitted(t, m2, "drop"); ok {
		t.Fatal("savepoint-rolled-back write resurrected by recovery")
	}
}

func TestRecoverTornTail(t *testing.T) {
	dir := t.TempDir()

	m1, _ := Open(dir, nil)
	tx, _ := m1.Begin()
	tx.Put("a", []byte("1"))
	tx.Commit()
	if err := m1.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a torn write: half of a record frame at the tail.
	walPath := filepath.Join(dir, walFileName)
	f, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0x10, 0x00, 0x00, 0x00, 0xde, 0xad}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	m2 := reopen(t, dir)
	if v, ok := getCommitted(t, m2, "a"); !ok || v != "1" {
		t.Fatalf("a = %q,%v after torn-tail recovery", v, ok)
	}
	// The log must still accept new commits after truncation.
	tx2, _ := m2.Begin()
	tx2.Put("b", []byte("2"))
	if err := tx2.Commit(); err != nil {
		t.Fatal(err)
	}

	m3 := reopen(t, dir)
	if v, ok := getCommitted(t, m3, "b"); !ok || v != "2" {
		t.Fatalf("b = %q,%v after second recovery", v, ok)
	}
}

func TestRecoverCorruptLastRecord(t *testing.T) {
	dir := t.TempDir()

	m1, _ := Open(dir, nil)
	t1, _ := m1.Begin()
	t1.Put("k1", []byte("1"))
	t1.Commit()
	t2, _ := m1.Begin()
	t2.Put("k2", []byte("2"))
	t2.Commit()
	m1.Close()

	// Corrupt one payload byte of the last (k2 commit) record: its CRC no
	// longer matches, so recovery must discard it and everything after.
	walPath := filepath.Join(dir, walFileName)
	st, err := os.Stat(walPath)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(walPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	last := make([]byte, 1)
	if _, err := f.ReadAt(last, st.Size()-1); err != nil {
		t.Fatal(err)
	}
	last[0] ^= 0xff
	if _, err := f.WriteAt(last, st.Size()-1); err != nil {
		t.Fatal(err)
	}
	f.Close()

	m2 := reopen(t, dir)
	if v, ok := getCommitted(t, m2, "k1"); !ok || v != "1" {
		t.Fatalf("k1 = %q,%v want 1,true", v, ok)
	}
	if _, ok := getCommitted(t, m2, "k2"); ok {
		t.Fatal("corrupt commit record was applied")
	}
}

func TestTxIDMonotonicAcrossRestart(t *testing.T) {
	dir := t.TempDir()

	m1, _ := Open(dir, nil)
	tx, _ := m1.Begin()
	tx.Put("a", []byte("1"))
	tx.Commit()
	oldID := tx.ID()
	// Crash.

	m2 := reopen(t, dir)
	tx2, err := m2.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if tx2.ID() <= oldID {
		t.Fatalf("tx id reused across restart: old=%d new=%d", oldID, tx2.ID())
	}
	tx2.Rollback()
}

func TestRecoverAfterCleanClose(t *testing.T) {
	dir := t.TempDir()

	m1, _ := Open(dir, nil)
	tx, _ := m1.Begin()
	tx.Put("a", []byte("1"))
	tx.Commit()
	pending, _ := m1.Begin()
	pending.Put("p", []byte("x"))
	if err := m1.Close(); err != nil { // clean close rolls back active txs
		t.Fatal(err)
	}

	m2 := reopen(t, dir)
	if v, ok := getCommitted(t, m2, "a"); !ok || v != "1" {
		t.Fatalf("a = %q,%v", v, ok)
	}
	if _, ok := getCommitted(t, m2, "p"); ok {
		t.Fatal("tx active at Close survived recovery")
	}
}
