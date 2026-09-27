package tx

import "sync"

// committedStore holds the globally visible committed state plus per-key
// version counters. Every committed write bumps the key's version; readers
// record the versions they observed so the manager can validate them at
// commit time (optimistic concurrency control).
//
// Uncommitted data never enters this store, so there is nothing to undo on
// crash recovery — recovery only replays committed write sets from the WAL.
type committedStore struct {
	mu       sync.RWMutex
	data     map[string][]byte
	versions map[string]uint64
	seq      uint64
}

func newCommittedStore() *committedStore {
	return &committedStore{
		data:     make(map[string][]byte),
		versions: make(map[string]uint64),
	}
}

// get returns a copy of the value and the current version of key.
// ok=false when the key is absent; ver is still the key's version (0 if it
// was never written) so that "key did not exist" reads can be validated too.
func (s *committedStore) get(key string) (val []byte, ver uint64, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	if !ok {
		return nil, s.versions[key], false
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, s.versions[key], true
}

// validate reports whether every recorded read version still matches the
// committed state. Callers must serialize it against apply (Manager.commitMu).
func (s *committedStore) validate(readSet map[string]uint64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for k, v := range readSet {
		if s.versions[k] != v {
			return false
		}
	}
	return true
}

// apply installs a committed write set atomically and bumps versions.
// Callers must serialize it (Manager.commitMu) and must have persisted the
// write set to the WAL first.
func (s *committedStore) apply(ops []op) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	for _, o := range ops {
		if o.del {
			delete(s.data, o.key)
		} else {
			v := make([]byte, len(o.val))
			copy(v, o.val)
			s.data[o.key] = v
		}
		s.versions[o.key] = s.seq
	}
}
