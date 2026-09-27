package tx

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
)

// TestConflictRejected: two transactions read the same key and both write;
// the second commit must be rejected with ErrConflict.
func TestConflictRejected(t *testing.T) {
	m := openTest(t, nil)

	setup, _ := m.Begin()
	setup.Put("k", []byte("0"))
	setup.Commit()

	t1, _ := m.Begin()
	t2, _ := m.Begin()
	mustGet(t, t1, "k")
	mustGet(t, t2, "k")

	t1.Put("k", []byte("1"))
	t2.Put("k", []byte("2"))

	if err := t1.Commit(); err != nil {
		t.Fatalf("first commit should win: %v", err)
	}
	if err := t2.Commit(); !errors.Is(err, ErrConflict) {
		t.Fatalf("second commit must conflict, got %v", err)
	}
	// The winner's value stands.
	if v, ok := getCommitted(t, m, "k"); !ok || v != "1" {
		t.Fatalf("k = %q,%v want 1,true", v, ok)
	}
}

// TestConcurrentIncrementsSerializable: N goroutines increment one counter
// via Run (read-modify-write with automatic retry). If the final value is N,
// no update was lost: the committed history equals some serial order.
func TestConcurrentIncrementsSerializable(t *testing.T) {
	m := openTest(t, &Options{MaxRetries: 200})
	const n = 40

	setup, _ := m.Begin()
	setup.Put("counter", []byte("0"))
	setup.Commit()

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := m.Run(context.Background(), func(tx *Tx) error {
				v, ok, err := tx.Get("counter")
				if err != nil {
					return err
				}
				if !ok {
					return fmt.Errorf("counter missing")
				}
				n, _ := strconv.Atoi(string(v))
				return tx.Put("counter", []byte(strconv.Itoa(n+1)))
			})
			if err != nil {
				t.Errorf("Run: %v", err)
			}
		}()
	}
	wg.Wait()

	v, ok := getCommitted(t, m, "counter")
	if !ok {
		t.Fatal("counter missing")
	}
	if got, _ := strconv.Atoi(v); got != n {
		t.Fatalf("counter = %d, want %d (lost updates)", got, n)
	}
}

// TestNoMixedState: a writer commits pairs (k1,k2) that are always equal.
// Any reader transaction that successfully commits must have observed a
// consistent pair; a reader racing a commit is rejected and retried.
func TestNoMixedState(t *testing.T) {
	m := openTest(t, &Options{MaxRetries: 500})
	ctx := context.Background()

	setup, _ := m.Begin()
	setup.Put("k1", []byte("0"))
	setup.Put("k2", []byte("0"))
	setup.Commit()

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Writer: commit equal pairs v,v.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; i <= 50; i++ {
			v := []byte(strconv.Itoa(i))
			err := m.Run(ctx, func(tx *Tx) error {
				if err := tx.Put("k1", v); err != nil {
					return err
				}
				return tx.Put("k2", v)
			})
			if err != nil {
				t.Errorf("writer: %v", err)
				return
			}
		}
		close(stop)
	}()

	// Readers: the observation of the *committed* attempt must be equal.
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				var a, b string
				err := m.Run(ctx, func(tx *Tx) error {
					v1, _, err := tx.Get("k1")
					if err != nil {
						return err
					}
					v2, _, err := tx.Get("k2")
					if err != nil {
						return err
					}
					a, b = string(v1), string(v2)
					return nil
				})
				if err != nil {
					t.Errorf("reader: %v", err)
					return
				}
				if a != b {
					t.Errorf("committed reader observed mixed state k1=%q k2=%q", a, b)
					return
				}
			}
		}()
	}
	wg.Wait()
}

// TestBlindWritesLastWriterWins documents the policy for blind writes (no
// prior read): they never conflict; the commit order is the serial order.
func TestBlindWritesLastWriterWins(t *testing.T) {
	m := openTest(t, nil)

	t1, _ := m.Begin()
	t2, _ := m.Begin()
	t1.Put("k", []byte("1"))
	t2.Put("k", []byte("2"))

	if err := t1.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := t2.Commit(); err != nil {
		t.Fatalf("blind write should not conflict: %v", err)
	}
	if v, ok := getCommitted(t, m, "k"); !ok || v != "2" {
		t.Fatalf("k = %q,%v want 2,true", v, ok)
	}
}

// TestRunFnErrorRollsBack: an error from fn aborts the transaction.
func TestRunFnErrorRollsBack(t *testing.T) {
	m := openTest(t, nil)

	boom := errors.New("boom")
	err := m.Run(context.Background(), func(tx *Tx) error {
		tx.Put("a", []byte("1"))
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Run: %v", err)
	}
	if _, ok := getCommitted(t, m, "a"); ok {
		t.Fatal("write from failed fn leaked")
	}
}

func TestManagerClosed(t *testing.T) {
	dir := t.TempDir()
	m, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Begin(); !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("Begin after Close: %v", err)
	}
	// Double close is a no-op.
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
}
