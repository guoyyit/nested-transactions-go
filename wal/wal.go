// Package wal implements a write-ahead log with length-and-CRC framed
// records. Recovery scans valid records and truncates any torn tail
// left behind by a crash, so every record is either fully durable or
// entirely absent.
package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// RecordType identifies the kind of a WAL record.
type RecordType byte

const (
	RecordBegin     RecordType = 1 // a transaction began
	RecordSavepoint RecordType = 2 // a savepoint was created
	RecordCommit    RecordType = 3 // a transaction committed (carries writes)
	RecordRollback  RecordType = 4 // a transaction rolled back
	RecordSnapshot  RecordType = 5 // full snapshot replacing all prior records
)

// Write is a single key mutation carried by commit and snapshot records.
type Write struct {
	Key    string
	Value  []byte
	Delete bool
}

// Record is one decoded WAL entry.
type Record struct {
	Type   RecordType
	TxnID  uint64 // for RecordSnapshot: next txn id at snapshot time
	Name   string // savepoint name, for RecordSavepoint
	Writes []Write
}

const (
	headerSize = 8 // uint32 payload length + uint32 payload CRC32
	maxPayload = 1 << 30
)

// File is an open WAL file positioned for appending.
type File struct {
	mu      sync.Mutex
	f       *os.File
	path    string
	records []Record
}

// Open opens (or creates) the WAL at path, scans all valid records and
// truncates any torn tail left by a crash.
func Open(path string) (*File, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	w := &File{f: f, path: path}
	if err := w.recover(); err != nil {
		f.Close()
		return nil, fmt.Errorf("wal: recover %s: %w", path, err)
	}
	return w, nil
}

// recover scans the file, keeps fully-written CRC-valid records and
// truncates the file at the first invalid or incomplete record.
func (w *File) recover() error {
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	var off int64
	hdr := make([]byte, headerSize)
	for {
		if _, err := io.ReadFull(w.f, hdr); err != nil {
			break // clean EOF or torn header
		}
		length := binary.LittleEndian.Uint32(hdr[0:4])
		crc := binary.LittleEndian.Uint32(hdr[4:8])
		if length == 0 || length > maxPayload {
			break // implausible length: treat as torn write
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(w.f, payload); err != nil {
			break // torn payload
		}
		if crc32.ChecksumIEEE(payload) != crc {
			break // corrupt payload
		}
		rec, err := decode(payload)
		if err != nil {
			break
		}
		w.records = append(w.records, rec)
		off += headerSize + int64(length)
	}
	if err := w.f.Truncate(off); err != nil {
		return err
	}
	_, err := w.f.Seek(off, io.SeekStart)
	return err
}

// Records returns the valid records recovered or appended so far.
func (w *File) Records() []Record {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]Record, len(w.records))
	copy(out, w.records)
	return out
}

// Append encodes rec, writes it and fsyncs before returning, so a record
// is atomically durable once Append returns nil.
func (w *File) Append(rec Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.f.Write(encode(rec)); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	w.records = append(w.records, rec)
	return nil
}

// Compact atomically replaces the whole log with a single snapshot record
// carrying the given writes and the next transaction id.
func (w *File) Compact(nextTxn uint64, writes []Write) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	rec := Record{Type: RecordSnapshot, TxnID: nextTxn, Writes: writes}
	tmp := w.path + ".tmp"
	tf, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := tf.Write(encode(rec)); err != nil {
		tf.Close()
		os.Remove(tmp)
		return err
	}
	if err := tf.Sync(); err != nil {
		tf.Close()
		os.Remove(tmp)
		return err
	}
	if err := tf.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, w.path); err != nil {
		return err
	}
	syncDir(w.path)
	nf, err := os.OpenFile(w.path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	w.f.Close()
	w.f = nf
	w.records = []Record{rec}
	_, err = w.f.Seek(0, io.SeekEnd)
	return err
}

// Close flushes and closes the underlying file.
func (w *File) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Close()
}

func syncDir(path string) {
	d, err := os.Open(dirOf(path))
	if err != nil {
		return
	}
	d.Sync()
	d.Close()
}

func dirOf(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			if i == 0 {
				return "/"
			}
			return path[:i]
		}
	}
	return "."
}

// encode frames a record as [len][crc][payload].
func encode(rec Record) []byte {
	var buf bytes.Buffer
	buf.WriteByte(byte(rec.Type))
	switch rec.Type {
	case RecordBegin, RecordRollback:
		writeU64(&buf, rec.TxnID)
	case RecordSavepoint:
		writeU64(&buf, rec.TxnID)
		writeStr(&buf, rec.Name)
	case RecordCommit, RecordSnapshot:
		writeU64(&buf, rec.TxnID)
		writeWrites(&buf, rec.Writes)
	}
	payload := buf.Bytes()
	out := make([]byte, headerSize+len(payload))
	binary.LittleEndian.PutUint32(out[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(out[4:8], crc32.ChecksumIEEE(payload))
	copy(out[headerSize:], payload)
	return out
}

func decode(payload []byte) (Record, error) {
	r := &reader{buf: payload}
	typ := RecordType(r.u8())
	rec := Record{Type: typ}
	switch typ {
	case RecordBegin, RecordRollback:
		rec.TxnID = r.u64()
	case RecordSavepoint:
		rec.TxnID = r.u64()
		rec.Name = r.str()
	case RecordCommit, RecordSnapshot:
		rec.TxnID = r.u64()
		rec.Writes = r.writes()
	default:
		return rec, fmt.Errorf("wal: unknown record type %d", typ)
	}
	if r.err != nil || r.off != len(r.buf) {
		return rec, fmt.Errorf("wal: malformed record payload")
	}
	return rec, nil
}

func writeU64(buf *bytes.Buffer, v uint64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	buf.Write(b[:])
}

func writeStr(buf *bytes.Buffer, s string) {
	writeU64(buf, uint64(len(s)))
	buf.WriteString(s)
}

func writeWrites(buf *bytes.Buffer, ws []Write) {
	writeU64(buf, uint64(len(ws)))
	for _, w := range ws {
		writeStr(buf, w.Key)
		if w.Delete {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}
		writeU64(buf, uint64(len(w.Value)))
		buf.Write(w.Value)
	}
}

// reader is a cursor over a payload that records the first error.
type reader struct {
	buf []byte
	off int
	err error
}

func (r *reader) take(n int) []byte {
	if r.err != nil {
		return nil
	}
	if n < 0 || len(r.buf)-r.off < n {
		r.err = fmt.Errorf("wal: short record")
		return nil
	}
	b := r.buf[r.off : r.off+n]
	r.off += n
	return b
}

func (r *reader) u8() byte {
	b := r.take(1)
	if b == nil {
		return 0
	}
	return b[0]
}

func (r *reader) u64() uint64 {
	b := r.take(8)
	if b == nil {
		return 0
	}
	return binary.LittleEndian.Uint64(b)
}

func (r *reader) str() string {
	n := r.u64()
	if r.err != nil || n > uint64(len(r.buf)) {
		r.err = fmt.Errorf("wal: bad string length")
		return ""
	}
	return string(r.take(int(n)))
}

func (r *reader) writes() []Write {
	n := r.u64()
	if r.err != nil || n > uint64(len(r.buf)) {
		r.err = fmt.Errorf("wal: bad writes count")
		return nil
	}
	ws := make([]Write, 0, n)
	for i := uint64(0); i < n; i++ {
		key := r.str()
		del := r.u8()
		ln := r.u64()
		if r.err != nil || ln > uint64(len(r.buf)) {
			r.err = fmt.Errorf("wal: bad value length")
			return nil
		}
		val := r.take(int(ln))
		if r.err != nil {
			return nil
		}
		w := Write{Key: key, Delete: del == 1}
		if val != nil {
			w.Value = append([]byte(nil), val...)
		}
		ws = append(ws, w)
	}
	return ws
}
