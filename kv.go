package shardkv

import (
	"fmt"
	"sync"
)

type Store struct {
	mu   sync.Mutex
	data map[string]string
	wal  *WAL
}

func Open(path string) (*Store, error) {
	entries, err := ReadAll(path)
	if err != nil {
		return nil, fmt.Errorf("replay: %w", err)
	}

	data := make(map[string]string, len(entries))
	for _, e := range entries {
		switch e.Op {
		case "SET":
			data[e.Key] = e.Value
		case "DELETE":
			delete(data, e.Key)
		}
	}

	wal, err := OpenWAL(path)
	if err != nil {
		return nil, err
	}

	return &Store{data: data, wal: wal}, nil
}

func (s *Store) Close() error {
	return s.wal.Close()
}

func (s *Store) Set(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.wal.Append(Entry{Op: "SET", Key: key, Value: value}); err != nil {
		return err
	}
	s.data[key] = value
	return nil
}

func (s *Store) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.wal.Append(Entry{Op: "DELETE", Key: key}); err != nil {
		return err
	}
	delete(s.data, key)
	return nil
}

func (s *Store) Get(key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	val, ok := s.data[key]
	if !ok {
		return "", fmt.Errorf("key not found")
	}
	return val, nil
}
