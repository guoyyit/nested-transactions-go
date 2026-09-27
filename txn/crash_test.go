package txn

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/guoyyit/nested-transactions-go/wal"
)

// reopen simulates a crash: the executor is abandoned without rolling
// back open transactions, and a fresh executor opens the same directory.
// (Every WAL append is fsynced, so closing the handle loses nothing that
// a crash would not also have preserved.)
func reopen(t *testing.T, e *Executor, dir string) *Executor {
	t.Helper()
	e.Close()
	e2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e2.Close() })
	return e2
}

func TestCrashDiscardsUncommitted(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	tx, _ := e.Begin()
	tx.Put("committed", []byte("yes"))
	tx.Savepoint("sp1")
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// A transaction tree that gets interrupted mid-flight: writes staged,
	// savepoints taken, a child even committed into the parent — but the
	// root never commits.
	victim, _ := e.Begin()
	victim.Put("staged", []byte("no"))
	victim.Savepoint("sp-a")
	child, _ := victim.BeginChild()
	child.Put("child-staged", []byte("no"))
	child.Commit()
	victim.Savepoint("sp-b")

	e = reopen(t, e, dir)

	check, _ := e.Begin()
	defer check.Rollback()
	if v, ok := mustGet(t, check, "committed"); !ok || v != "yes" {
		t.Fatalf("committed write lost after crash: %q %v", v, ok)
	}
	if _, ok := mustGet(t, check, "staged"); ok {
		t.Fatal("uncommitted write survived crash")
	}
	if _, ok := mustGet(t, check, "child-staged"); ok {
		t.Fatal("uncommitted child write survived crash")
	}
}

func TestCrashKeepsCommittedSavepointMetadata(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	tx, _ := e.Begin()
	tx.Put("k", []byte("v"))
	tx.Savepoint("checkpoint")
	tx.Put("k2", []byte("v2"))
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	e = reopen(t, e, dir)

	// The WAL must still hold the savepoint and commit records of the
	// committed transaction, complete and uncorrupted.
	w, err := wal.Open(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	var sawSavepoint, sawCommit bool
	for _, r := range w.Records() {
		if r.Type == wal.RecordSavepoint && r.Name == "checkpoint" {
			sawSavepoint = true
		}
		if r.Type == wal.RecordCommit && len(r.Writes) == 2 {
			sawCommit = true
		}
	}
	if !sawSavepoint {
		t.Fatal("savepoint metadata of committed txn lost")
	}
	if !sawCommit {
		t.Fatal("commit record lost")
	}
}

func TestCrashWithTornCommit(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	tx, _ := e.Begin()
	tx.Put("a", []byte("1"))
	tx.Commit()
	e.Close()

	// Corrupt the tail as if a commit record was half-written.
	path := filepath.Join(dir, "wal.log")
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	f.Write([]byte{0x30, 0x00, 0x00, 0x00, 0x01, 0x02})
	f.Close()

	e2, err := Open(dir)
	if err != nil {
		t.Fatalf("torn tail must be truncated, not fatal: %v", err)
	}
	defer e2.Close()
	check, _ := e2.Begin()
	defer check.Rollback()
	if v, ok := mustGet(t, check, "a"); !ok || v != "1" {
		t.Fatalf("committed data lost: %q %v", v, ok)
	}
}

// TestCrashRecoveryRealProcess kills a real process with SIGKILL in the
// middle of its work and verifies recovery guarantees afterwards.
func TestCrashRecoveryRealProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") == "1" {
		crashHelper(t)
		return
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=TestCrashRecoveryRealProcess")
	cmd.Env = append(os.Environ(), "GO_WANT_HELPER_PROCESS=1", "CRASH_DIR="+dir)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	// Wait for the helper to signal it has committed one transaction and
	// staged another, then kill it without any cleanup.
	ready := filepath.Join(dir, "ready")
	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			cmd.Process.Kill()
			cmd.Wait()
			t.Fatal("helper never became ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cmd.Process.Kill()
	cmd.Wait()

	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	check, _ := e.Begin()
	defer check.Rollback()
	if v, ok := mustGet(t, check, "committed"); !ok || v != "yes" {
		t.Fatalf("committed write lost after SIGKILL: %q %v", v, ok)
	}
	if _, ok := mustGet(t, check, "uncommitted"); ok {
		t.Fatal("uncommitted write survived SIGKILL")
	}
	if _, ok := mustGet(t, check, "uncommitted-child"); ok {
		t.Fatal("uncommitted child write survived SIGKILL")
	}
}

// crashHelper runs in the subprocess. It commits one transaction (with a
// savepoint), stages a second one (with a savepoint and a committed
// child), signals readiness and then waits to be killed.
func crashHelper(t *testing.T) {
	dir := os.Getenv("CRASH_DIR")
	e, err := Open(dir)
	if err != nil {
		os.Exit(2)
	}
	tx1, _ := e.Begin()
	tx1.Put("committed", []byte("yes"))
	tx1.Savepoint("sp-committed")
	if err := tx1.Commit(); err != nil {
		os.Exit(3)
	}
	tx2, _ := e.Begin()
	tx2.Put("uncommitted", []byte("no"))
	tx2.Savepoint("sp-uncommitted")
	child, _ := tx2.BeginChild()
	child.Put("uncommitted-child", []byte("no"))
	child.Commit()
	if err := os.WriteFile(filepath.Join(dir, "ready"), []byte("1"), 0o644); err != nil {
		os.Exit(4)
	}
	select {} // wait to be killed
}
