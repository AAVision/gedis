package main

import (
	"sync"
	"time"
)

type Value interface{}

type Store struct {
	mu        sync.RWMutex
	data      map[string]Value
	expiry    map[string]time.Time
	stopCh    chan struct{}
	closeOnce sync.Once
}

type ServerError struct {
	Msg string
}

func (e *ServerError) Error() string {
	return e.Msg
}

func NewStore() *Store {
	store := &Store{
		data:   make(map[string]Value),
		expiry: make(map[string]time.Time),
		stopCh: make(chan struct{}),
	}

	go store.cleanupExpiredKeys()

	return store
}

func (s *Store) cleanupExpiredKeys() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.mu.Lock()
			now := time.Now()
			for key, exp := range s.expiry {
				if now.After(exp) {
					delete(s.data, key)
					delete(s.expiry, key)
				}
			}

			s.mu.Unlock()
		case <-s.stopCh:
			return
		}
	}
}

func (s *Store) Close() {
	s.closeOnce.Do(func() {
		close(s.stopCh)
	})
}

func (s *Store) Set(key string, value Value) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
	delete(s.expiry, key)
}

func (s *Store) SetEx(key string, value Value, ttl time.Duration) {
	if ttl == 0 {
		ttl = 3600
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
	s.expiry[key] = time.Now().Add(ttl)
}

func (s *Store) Get(key string) (Value, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if exp, ok := s.expiry[key]; ok && time.Now().After(exp) {
		s.mu.RUnlock()
		s.mu.Lock()
		delete(s.data, key)
		delete(s.expiry, key)
		s.mu.Unlock()
		s.mu.RLock()
		return nil, false
	}

	val, exists := s.data[key]
	return val, exists
}

func (s *Store) Del(keys ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := 0

	for _, key := range keys {
		if _, exists := s.data[key]; exists {
			delete(s.data, key)
			delete(s.expiry, key)
			count++
		}
	}

	return count
}

func (s *Store) Expire(key string, ttl time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.data[key]; !exists {
		return false
	}

	s.expiry[key] = time.Now().Add(ttl)
	return true
}

func (s *Store) TTL(key string) time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()

	exp, exists := s.expiry[key]
	if !exists {
		return -1
	}
	if time.Now().After(exp) {
		return -2
	}
	return time.Until(exp)
}

func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.data))
	for key := range s.data {
		if exp, exists := s.expiry[key]; exists && time.Now().After(exp) {
			continue
		}
		keys = append(keys, key)
	}
	return keys
}

func (s *Store) FlushDB() {
	s.mu.RLock()
	defer s.mu.RUnlock()

	s.data = make(map[string]Value)
	s.expiry = make(map[string]time.Time)
}

var (
	ErrInvalidType = &ServerError{Msg: "ERR invalid type!"}
	ErrKeyNotFound = &ServerError{Msg: "ERR key not found!"}
)
