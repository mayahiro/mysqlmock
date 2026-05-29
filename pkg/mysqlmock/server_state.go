package mysqlmock

import "sync"

type diagnosticsStore struct {
	mu          sync.Mutex
	unsupported []UnsupportedQuery
	queries     []QueryEvent
}

func (s *diagnosticsStore) unsupportedSnapshot() []UnsupportedQuery {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]UnsupportedQuery, len(s.unsupported))
	copy(out, s.unsupported)
	return out
}

func (s *diagnosticsStore) queriesSnapshot() []QueryEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]QueryEvent, len(s.queries))
	copy(out, s.queries)
	return out
}

func (s *diagnosticsStore) recordUnsupported(query UnsupportedQuery) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.unsupported = append(s.unsupported, query)
}

func (s *diagnosticsStore) recordQuery(event QueryEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.queries = append(s.queries, event)
}

func (s *diagnosticsStore) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.unsupported = nil
	s.queries = nil
}

type advisoryLockStore struct {
	mu    sync.Mutex
	locks map[string]uint32
}

func (s *advisoryLockStore) get(name string, connectionID uint32) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.locks == nil {
		s.locks = map[string]uint32{}
	}
	holder, ok := s.locks[name]
	if !ok || holder == connectionID {
		s.locks[name] = connectionID
		return 1
	}
	return 0
}

func (s *advisoryLockStore) release(name string, connectionID uint32) any {
	s.mu.Lock()
	defer s.mu.Unlock()

	holder, ok := s.locks[name]
	if !ok {
		return nil
	}
	if holder != connectionID {
		return 0
	}
	delete(s.locks, name)
	return 1
}

func (s *advisoryLockStore) releaseAll(connectionID uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for name, holder := range s.locks {
		if holder == connectionID {
			delete(s.locks, name)
		}
	}
}
