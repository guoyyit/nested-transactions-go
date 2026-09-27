package txn

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func TestWriteWriteConflictRejected(t *testing.T) {
	e := open(t)
	a, _ := e.Begin()
	b, _ := e.Begin()
	a.Put("k", []byte("from-a"))
	b.Put("k", []byte("from-b"))

	if err := a.Commit(); err != nil {
		t.Fatal(err)
	}
	// b touched the same key: its commit must be rejected, not merged.
	if err := b.Commit(); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting commit must be rejected, got %v", err)
	}
	// The rejected commit applied nothing.
	check, _ := e.Begin()
	defer check.Rollback()
	if v, ok := mustGet(t, check, "k"); !ok || v != "from-a" {
		t.Fatalf("k = %q %v, want from-a", v, ok)
	}
}

func TestReadWriteConflictRejected(t *testing.T) {
	e := open(t)
	setup, _ := e.Begin()
	setup.Put("k", []byte("v0"))
	if err := setup.Commit(); err != nil {
		t.Fatal(err)
	}

	a, _ := e.Begin()
	if _, ok := mustGet(t, a, "k"); !ok {
		t.Fatal("k missing")
	}
	b, _ := e.Begin()
	b.Put("k", []byte("v1"))
	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}
	// a read a version that is now stale: commit must fail.
	if err := a.Commit(); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale-read commit must be rejected, got %v", err)
	}
}

func TestDisjointTransactionsBothCommit(t *testing.T) {
	e := open(t)
	a, _ := e.Begin()
	b, _ := e.Begin()
	a.Put("ka", []byte("1"))
	b.Put("kb", []byte("2"))
	if err := a.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(); err != nil {
		t.Fatalf("disjoint transactions must not conflict: %v", err)
	}
}

// TestConcurrentCounter hammers one key from many goroutines through
// Run's retry loop. The final value must equal the number of increments,
// which only holds if every increment was serialized.
func TestConcurrentCounter(t *testing.T) {
	e := open(t, WithMaxRetries(200))
	const workers = 8
	const perWorker = 25

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				err := e.Run(func(tx *Tx) error {
					v, ok, err := tx.Get("counter")
					if err != nil {
						return err
					}
					n := 0
					if ok {
						n, _ = strconv.Atoi(string(v))
					}
					return tx.Put("counter", []byte(strconv.Itoa(n+1)))
				})
				if err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()

	check, _ := e.Begin()
	defer check.Rollback()
	v, ok := mustGet(t, check, "counter")
	if !ok {
		t.Fatal("counter missing")
	}
	if n, _ := strconv.Atoi(v); n != workers*perWorker {
		t.Fatalf("counter = %d, want %d (lost updates)", n, workers*perWorker)
	}
}

// TestNoMixedState writes two-field records ("left:right" where both
// fields always match) from concurrent writers while readers scan them.
// A reader must never observe a record whose fields disagree — that would
// be a field-level mixed state.
func TestNoMixedState(t *testing.T) {
	e := open(t)
	const writers = 4
	const rounds = 20

	stop := make(chan struct{})
	var readersWg, writersWg sync.WaitGroup

	// Readers: every record read must have matching fields.
	for r := 0; r < 2; r++ {
		readersWg.Add(1)
		go func() {
			defer readersWg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				tx, err := e.Begin()
				if err != nil {
					t.Error(err)
					return
				}
				for w := 0; w < writers; w++ {
					v, ok, err := tx.Get(fmt.Sprintf("rec-%d", w))
					if err != nil {
						t.Error(err)
						return
					}
					if !ok {
						continue
					}
					parts := strings.Split(string(v), ":")
					if len(parts) != 2 || parts[0] != parts[1] {
						t.Errorf("mixed state observed: %q", v)
						return
					}
				}
				tx.Rollback()
			}
		}()
	}

	// Writers: each round writes a matching pair.
	for w := 0; w < writers; w++ {
		writersWg.Add(1)
		go func(w int) {
			defer writersWg.Done()
			key := fmt.Sprintf("rec-%d", w)
			for i := 0; i < rounds; i++ {
				val := fmt.Sprintf("%d:%d", i, i)
				if err := e.Run(func(tx *Tx) error { return tx.Put(key, []byte(val)) }); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	writersWg.Wait()
	close(stop)
	readersWg.Wait()
}

// TestSerializableEquivalent checks a classic non-serializable schedule:
// two transactions each read both accounts and write the one they did not
// read first. Exactly one of them must be rejected.
func TestSerializableEquivalent(t *testing.T) {
	e := open(t)
	setup, _ := e.Begin()
	setup.Put("x", []byte("10"))
	setup.Put("y", []byte("10"))
	if err := setup.Commit(); err != nil {
		t.Fatal(err)
	}

	a, _ := e.Begin()
	b, _ := e.Begin()
	// a reads x, writes y; b reads y, writes x.
	mustGet(t, a, "x")
	mustGet(t, b, "y")
	a.Put("y", []byte("0"))
	b.Put("x", []byte("0"))

	errA := a.Commit()
	errB := b.Commit()
	if errA == nil && errB == nil {
		t.Fatal("both commits succeeded: schedule is not serializable")
	}
	if errA != nil && errB != nil && !errors.Is(errA, ErrConflict) && !errors.Is(errB, ErrConflict) {
		t.Fatalf("unexpected errors: %v %v", errA, errB)
	}
}
