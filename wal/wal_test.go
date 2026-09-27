package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	recs := []Record{
		{Type: RecordBegin, TxnID: 1},
		{Type: RecordSavepoint, TxnID: 1, Name: "sp1"},
		{Type: RecordCommit, TxnID: 1, Writes: []Write{
			{Key: "a", Value: []byte("1")},
			{Key: "b", Delete: true},
			{Key: "c", Value: []byte{}},
		}},
		{Type: RecordRollback, TxnID: 2},
	}
	for _, r := range recs {
		if err := w.Append(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	got := w2.Records()
	if len(got) != len(recs) {
		t.Fatalf("got %d records, want %d", len(got), len(recs))
	}
	for i, r := range recs {
		if got[i].Type != r.Type || got[i].TxnID != r.TxnID || got[i].Name != r.Name {
			t.Fatalf("record %d mismatch: %+v vs %+v", i, got[i], r)
		}
		if len(got[i].Writes) != len(r.Writes) {
			t.Fatalf("record %d writes mismatch", i)
		}
		for j, wr := range r.Writes {
			g := got[i].Writes[j]
			if g.Key != wr.Key || g.Delete != wr.Delete || string(g.Value) != string(wr.Value) {
				t.Fatalf("record %d write %d mismatch: %+v vs %+v", i, j, g, wr)
			}
		}
	}
}

func TestTornTailTruncated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	w.Append(Record{Type: RecordBegin, TxnID: 1})
	w.Append(Record{Type: RecordCommit, TxnID: 1, Writes: []Write{{Key: "a", Value: []byte("1")}}})
	w.Close()

	// Simulate a torn write: append garbage bytes after the valid records.
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	f.Write([]byte{0x10, 0x00, 0x00, 0x00, 0xde, 0xad})
	f.Close()

	w2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	if n := len(w2.Records()); n != 2 {
		t.Fatalf("got %d records, want 2 (torn tail must be dropped)", n)
	}
	// File must be truncated so future appends don't follow the garbage.
	st, _ := os.Stat(path)
	w2.Append(Record{Type: RecordBegin, TxnID: 2})
	w2.Close()
	w3, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w3.Close()
	if n := len(w3.Records()); n != 3 {
		t.Fatalf("got %d records after append, want 3 (size was %d)", n, st.Size())
	}
}

func TestCorruptCRCDropped(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	w.Append(Record{Type: RecordBegin, TxnID: 1})
	w.Append(Record{Type: RecordBegin, TxnID: 2})
	w.Close()

	// Flip a byte inside the second record's payload.
	data, _ := os.ReadFile(path)
	data[len(data)-1] ^= 0xff
	os.WriteFile(path, data, 0o644)

	w2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	recs := w2.Records()
	if len(recs) != 1 || recs[0].TxnID != 1 {
		t.Fatalf("corrupt record not dropped: %+v", recs)
	}
}

func TestCompact(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	w.Append(Record{Type: RecordBegin, TxnID: 1})
	w.Append(Record{Type: RecordCommit, TxnID: 1, Writes: []Write{{Key: "a", Value: []byte("1")}}})
	if err := w.Compact(7, []Write{{Key: "a", Value: []byte("1")}, {Key: "z", Delete: true}}); err != nil {
		t.Fatal(err)
	}
	w.Close()

	w2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	recs := w2.Records()
	if len(recs) != 1 || recs[0].Type != RecordSnapshot || recs[0].TxnID != 7 {
		t.Fatalf("bad records after compact: %+v", recs)
	}
	if len(recs[0].Writes) != 2 {
		t.Fatalf("snapshot writes lost: %+v", recs[0].Writes)
	}
}
