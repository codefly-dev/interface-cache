package cache

import (
	"bytes"
	"container/list"
	"context"
	"sync"
	"time"
)

// Memory is an in-process, size-bounded LRU layer. It is local to one process:
// it grants no leases and hears about other processes' writes only through a
// Notifier below it in a stack.
type Memory struct {
	maxEntries    int
	maxValueBytes int
	now           func() time.Time

	mu    sync.Mutex
	order *list.List // front = most recently used
	items map[string]*list.Element
}

type memoryItem struct {
	key     string
	entry   Entry
	expires time.Time
}

// MemoryOption configures a Memory layer.
type MemoryOption func(*Memory)

// MaxEntries bounds how many keys the layer holds; the least recently used is
// evicted first. Default 10,000.
func MaxEntries(n int) MemoryOption { return func(m *Memory) { m.maxEntries = n } }

// MaxValueBytes caps the size of one cached value; larger values are refused
// with ErrTooLarge. Default 1 MiB.
func MaxValueBytes(n int) MemoryOption { return func(m *Memory) { m.maxValueBytes = n } }

// NewMemory returns an empty Memory layer.
func NewMemory(opts ...MemoryOption) *Memory {
	m := &Memory{
		maxEntries:    10_000,
		maxValueBytes: 1 << 20,
		now:           time.Now,
		order:         list.New(),
		items:         make(map[string]*list.Element),
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Get implements Layer.
func (m *Memory) Get(_ context.Context, key string) (Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	el, ok := m.items[key]
	if !ok {
		return Entry{}, ErrMiss
	}
	item := el.Value.(*memoryItem)
	if !m.now().Before(item.expires) {
		m.removeLocked(el)
		return Entry{}, ErrMiss
	}
	m.order.MoveToFront(el)
	return cloneEntry(item.entry), nil
}

// Set implements Layer.
func (m *Memory) Set(_ context.Context, key string, e Entry, ttl time.Duration) error {
	if len(e.Value) > m.maxValueBytes {
		return ErrTooLarge
	}
	if ttl <= 0 {
		return m.Delete(context.Background(), key)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	item := &memoryItem{key: key, entry: cloneEntry(e), expires: m.now().Add(ttl)}
	if el, ok := m.items[key]; ok {
		el.Value = item
		m.order.MoveToFront(el)
		return nil
	}
	m.items[key] = m.order.PushFront(item)
	for m.order.Len() > m.maxEntries {
		m.removeLocked(m.order.Back())
	}
	return nil
}

// Delete implements Layer.
func (m *Memory) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if el, ok := m.items[key]; ok {
		m.removeLocked(el)
	}
	return nil
}

// Len reports how many keys the layer holds, expired ones included until they
// are next touched.
func (m *Memory) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.order.Len()
}

func (m *Memory) removeLocked(el *list.Element) {
	m.order.Remove(el)
	delete(m.items, el.Value.(*memoryItem).key)
}

// cloneEntry copies Value so a caller mutating its slice cannot change what the
// layer holds, and vice versa.
func cloneEntry(e Entry) Entry {
	e.Value = bytes.Clone(e.Value) // keeps an empty value distinct from nil
	return e
}
