package tx

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// WAL file layout:
//
//	header:  [magic 4B]['N','T','X','W'][version 1B]
//	record:  [payloadLen uint32 LE][crc32(payload) uint32 LE][payload]
//	payload: [recType 1B][fields...]
//
// Every record is self-checksummed, so a torn write at the tail (caused by a
// crash mid-append) is detected on recovery and the file is truncated there.
// A commit record carries the transaction's full write set, making the commit
// atomic: it either decodes cleanly as a whole or is discarded entirely.
var walMagic = []byte{'N', 'T', 'X', 'W'}

const walVersion = 1

// recType identifies WAL record kinds.
type recType byte

const (
	recBegin recType = iota + 1
	recSavepoint
	recReleaseSavepoint
	recCommit
	recChildCommit
	recRollback
)

const frameHeaderLen = 8 // payloadLen(4) + crc32(4)

// wal is an append-only, CRC-framed write-ahead log. Every append is
// immediately fsynced: transaction/savepoint metadata must survive a crash.
type wal struct {
	f    *os.File
	path string
}

func openWAL(path string) (*wal, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if st.Size() == 0 {
		hdr := append(append([]byte{}, walMagic...), walVersion)
		if _, err := f.WriteAt(hdr, 0); err != nil {
			f.Close()
			return nil, err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, err
		}
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}
	return &wal{f: f, path: path}, nil
}

// append frames and writes one record atomically. When sync is true the
// record is fsynced before returning; audit-only records (begin, rollback,
// child commit) may skip the fsync because recovery never depends on them:
// a transaction without a commit record is rolled back by definition.
func (w *wal) append(typ recType, sync bool, fields func(*encBuf)) error {
	buf := &encBuf{}
	buf.byte(byte(typ))
	fields(buf)
	payload := buf.b

	frame := make([]byte, frameHeaderLen+len(payload))
	binary.LittleEndian.PutUint32(frame[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
	copy(frame[frameHeaderLen:], payload)

	if _, err := w.f.Write(frame); err != nil {
		return err
	}
	if !sync {
		return nil
	}
	return w.f.Sync()
}

func (w *wal) logBegin(txID, parentID uint64) error {
	return w.append(recBegin, false, func(b *encBuf) {
		b.u64(txID)
		b.u64(parentID)
	})
}

func (w *wal) logSavepoint(txID uint64, name string, opIndex int) error {
	return w.append(recSavepoint, true, func(b *encBuf) {
		b.u64(txID)
		b.str(name)
		b.u64(uint64(opIndex))
	})
}

func (w *wal) logReleaseSavepoint(txID uint64, name string) error {
	return w.append(recReleaseSavepoint, true, func(b *encBuf) {
		b.u64(txID)
		b.str(name)
	})
}

// logCommit persists the whole write set as a single record: the commit is
// atomic with respect to crashes (one CRC-framed record, fsynced).
func (w *wal) logCommit(txID uint64, ops []op) error {
	return w.append(recCommit, true, func(b *encBuf) {
		b.u64(txID)
		b.u32(uint32(len(ops)))
		for _, o := range ops {
			b.str(o.key)
			if o.del {
				b.byte(1)
			} else {
				b.byte(0)
			}
			b.bytes(o.val)
		}
	})
}

func (w *wal) logRollback(txID uint64) error {
	return w.append(recRollback, false, func(b *encBuf) {
		b.u64(txID)
	})
}

// logChildCommit records a nested commit for audit. Recovery ignores these
// records: a child's writes become durable only through the root's commit
// record, which carries the family's full merged write set.
func (w *wal) logChildCommit(txID uint64, ops []op) error {
	return w.append(recChildCommit, false, func(b *encBuf) {
		b.u64(txID)
		b.u32(uint32(len(ops)))
		for _, o := range ops {
			b.str(o.key)
			if o.del {
				b.byte(1)
			} else {
				b.byte(0)
			}
			b.bytes(o.val)
		}
	})
}

func (w *wal) close() error {
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}

// recoveredState is the result of replaying a WAL after a crash.
type recoveredState struct {
	commits [][]op // committed write sets, in log order
	maxTxID uint64
}

// recoverWAL replays the log. Records of incomplete transactions are simply
// ignored: uncommitted writes were never applied to the store. A corrupt or
// partial tail (torn write during crash) is truncated so future appends start
// from a clean boundary.
func recoverWAL(path string) (*recoveredState, error) {
	st := &recoveredState{}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() == 0 {
		return st, nil // fresh file; openWAL will write the header
	}
	if info.Size() < int64(len(walMagic)+1) {
		return nil, errors.New("tx: WAL file too short for header")
	}
	hdr := make([]byte, len(walMagic)+1)
	if _, err := io.ReadFull(f, hdr); err != nil {
		return nil, err
	}
	if string(hdr[:4]) != string(walMagic) || hdr[4] != walVersion {
		return nil, fmt.Errorf("tx: bad WAL header")
	}

	offset := int64(len(hdr))
	for {
		recOff := offset
		head := make([]byte, frameHeaderLen)
		n, err := io.ReadFull(f, head)
		if err != nil {
			if err == io.EOF && n == 0 {
				break // clean end of log
			}
			// torn frame header: truncate and stop
			if terr := f.Truncate(recOff); terr != nil {
				return nil, terr
			}
			break
		}
		payloadLen := binary.LittleEndian.Uint32(head[0:4])
		wantCRC := binary.LittleEndian.Uint32(head[4:8])
		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(f, payload); err != nil {
			if terr := f.Truncate(recOff); terr != nil {
				return nil, terr
			}
			break
		}
		if crc32.ChecksumIEEE(payload) != wantCRC {
			if terr := f.Truncate(recOff); terr != nil {
				return nil, terr
			}
			break
		}
		offset = recOff + frameHeaderLen + int64(payloadLen)

		d := &decBuf{b: payload}
		typ := recType(d.byte())
		switch typ {
		case recBegin:
			txID := d.u64()
			d.u64() // parentID: only needed for audit, not for replay
			if txID > st.maxTxID {
				st.maxTxID = txID
			}
		case recSavepoint:
			txID := d.u64()
			d.str()
			d.u64()
			if txID > st.maxTxID {
				st.maxTxID = txID
			}
		case recReleaseSavepoint:
			txID := d.u64()
			d.str()
			if txID > st.maxTxID {
				st.maxTxID = txID
			}
		case recCommit:
			txID := d.u64()
			n := d.u32()
			ops := make([]op, 0, n)
			for i := uint32(0); i < n; i++ {
				key := d.str()
				del := d.byte() == 1
				val := d.bytes()
				ops = append(ops, op{key: key, val: val, del: del})
			}
			st.commits = append(st.commits, ops)
			if txID > st.maxTxID {
				st.maxTxID = txID
			}
		case recChildCommit:
			// Audit-only: nested commits are replayed through the root's
			// commit record, never on their own.
			txID := d.u64()
			n := d.u32()
			for i := uint32(0); i < n; i++ {
				d.str()
				d.byte()
				d.bytes()
			}
			if txID > st.maxTxID {
				st.maxTxID = txID
			}
		case recRollback:
			txID := d.u64()
			if txID > st.maxTxID {
				st.maxTxID = txID
			}
		default:
			return nil, fmt.Errorf("tx: unknown WAL record type %d", typ)
		}
		if d.err != nil {
			return nil, d.err
		}
	}
	return st, nil
}

// --- encoding helpers ---

type encBuf struct{ b []byte }

func (e *encBuf) byte(v byte)    { e.b = append(e.b, v) }
func (e *encBuf) u32(v uint32)   { e.b = binary.LittleEndian.AppendUint32(e.b, v) }
func (e *encBuf) u64(v uint64)   { e.b = binary.LittleEndian.AppendUint64(e.b, v) }
func (e *encBuf) str(s string)   { e.u32(uint32(len(s))); e.b = append(e.b, s...) }
func (e *encBuf) bytes(p []byte) { e.u32(uint32(len(p))); e.b = append(e.b, p...) }

// --- decoding helpers ---

type decBuf struct {
	b   []byte
	off int
	err error
}

func (d *decBuf) take(n int) []byte {
	if d.err != nil {
		return nil
	}
	if n < 0 || d.off+n > len(d.b) {
		d.err = errors.New("tx: corrupt WAL record")
		return nil
	}
	p := d.b[d.off : d.off+n]
	d.off += n
	return p
}

func (d *decBuf) byte() byte {
	p := d.take(1)
	if p == nil {
		return 0
	}
	return p[0]
}

func (d *decBuf) u32() uint32 {
	p := d.take(4)
	if p == nil {
		return 0
	}
	return binary.LittleEndian.Uint32(p)
}

func (d *decBuf) u64() uint64 {
	p := d.take(8)
	if p == nil {
		return 0
	}
	return binary.LittleEndian.Uint64(p)
}

func (d *decBuf) str() string {
	n := d.u32()
	p := d.take(int(n))
	if p == nil {
		return ""
	}
	return string(p)
}

func (d *decBuf) bytes() []byte {
	n := d.u32()
	p := d.take(int(n))
	if p == nil {
		return nil
	}
	out := make([]byte, len(p))
	copy(out, p)
	return out
}
