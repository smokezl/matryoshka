package matryoshka

import (
	"sync"
)

type sourceLock struct {
	m       sync.RWMutex
	content map[string]chan int
}

func (s *sourceLock) lock(key string) (owner bool, ch <-chan int) {
	s.m.RLock()
	ch, ok := s.content[key]
	s.m.RUnlock()
	if ok {
		return
	}
	s.m.Lock()
	ch, ok = s.content[key]
	if ok {
		s.m.Unlock()
		return
	}
	s.content[key] = make(chan int)
	ch = s.content[key]
	owner = true
	s.m.Unlock()
	return
}

func (s *sourceLock) unLock(key string) {
	s.m.Lock()
	ch, ok := s.content[key]
	if ok {
		close(ch)
		delete(s.content, key)
	}
	s.m.Unlock()
}
