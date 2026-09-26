package cache

import (
	"bytes"
	"container/list"
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"time"
)

// Memory is an in-process, size-bounded LRU layer. It is a complete backend:
// besides Layer it implements Leaser and Notifier, so it can be the shared
// layer under several stacks in one process — Share returns another client of
// the same store, the way a second process would hold a second Redis client.
//
// Notifications are delivered synchronously and are never lost, which makes
// Memory the reference implementation the conformance suite is checked
// against without a server.
type Memory struct {
	store  *memoryStore
	client uint64 // identifies this handle's own writes
}

var (
	_ Leaser   = (*Memory)(nil)
	_ Notifier = (*Memory)(nil)
)

type memoryStore struct {
	maxEntries    int
	maxValueBytes int
	now           func() time.Time
	nextClient    atomic.Uint64

	mu      sync.Mutex
	order   *list.List // front = most recently used
	items   map[string]*list.Element
	leases  map[string]memoryLease
	subs    map[uint64]memorySub
	nextSub uint64
}

type memoryItem struct {
	key     string
	entry   Entry
	expires time.Time
}

type memoryLease struct {
	token   string
	expires time.Time
}

type memorySub struct {
	client uint64
	fn     func(key string)
}

// MemoryOption configures a Memory layer.
type MemoryOption func(*memoryStore)

// MaxEntries bounds how many keys the layer holds; the least recently used is
// evicted first. Default 10,000.
func MaxEntries(n int) MemoryOption { return func(s *memoryStore) { s.maxEntries = n } }

// MaxValueBytes caps the size of one cached value; larger values are refused
// with ErrTooLarge. Default 1 MiB.
func MaxValueBytes(n int) MemoryOption { return func(s *memoryStore) { s.maxValueBytes = n } }

// NewMemory returns a client of a new, empty store.
func NewMemory(opts ...MemoryOption) *Memory {
	s := &memoryStore{
		maxEntries:    10_000,
		maxValueBytes: 1 << 20,
		now:           time.Now,
		order:         list.New(),
		items:         make(map[string]*list.Element),
		leases:        make(map[string]memoryLease),
		subs:          make(map[uint64]memorySub),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s.client()
}

// Share returns another client of the same store. Writes through one client
// are reported to the others' subscribers, never to their own.
func (m *Memory) Share() *Memory { return m.store.client() }

func (s *memoryStore) client() *Memory {
	return &Memory{store: s, client: s.nextClient.Add(1)}
}

// Get implements Layer.
func (m *Memory) Get(_ context.Context, key string) (Entry, error) {
	s := m.store
	s.mu.Lock()
	defer s.mu.Unlock()
	el, ok := s.items[key]
	if !ok {
		return Entry{}, ErrMiss
	}
	item := el.Value.(*memoryItem)
	if !s.now().Before(item.expires) {
		s.removeLocked(el)
		return Entry{}, ErrMiss
	}
	s.order.MoveToFront(el)
	return cloneEntry(item.entry), nil
}

// Set implements Layer. It revokes any fill lease on key.
func (m *Memory) Set(ctx context.Context, key string, e Entry, ttl time.Duration) error {
	if len(e.Value) > m.store.maxValueBytes {
		return ErrTooLarge
	}
	if ttl <= 0 {
		return m.Delete(ctx, key)
	}
	s := m.store
	s.mu.Lock()
	delete(s.leases, key)
	s.putLocked(key, e, ttl)
	notify := s.subscribersLocked(m.client)
	s.mu.Unlock()
	deliver(notify, key)
	return nil
}

// Delete implements Layer. It revokes any fill lease on key.
func (m *Memory) Delete(_ context.Context, key string) error {
	s := m.store
	s.mu.Lock()
	delete(s.leases, key)
	if el, ok := s.items[key]; ok {
		s.removeLocked(el)
	}
	notify := s.subscribersLocked(m.client)
	s.mu.Unlock()
	deliver(notify, key)
	return nil
}

// Acquire implements Leaser.
func (m *Memory) Acquire(_ context.Context, key string, ttl time.Duration) (Lease, bool, error) {
	s := m.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if held, ok := s.leases[key]; ok && s.now().Before(held.expires) {
		return Lease{}, false, nil
	}
	token := memoryToken()
	s.leases[key] = memoryLease{token: token, expires: s.now().Add(ttl)}
	return Lease{Key: key, Token: token}, true, nil
}

// Fill implements Leaser.
func (m *Memory) Fill(ctx context.Context, lease Lease, e Entry, ttl time.Duration) error {
	if ttl <= 0 {
		return m.Release(ctx, lease)
	}
	if len(e.Value) > m.store.maxValueBytes {
		_ = m.Release(ctx, lease)
		return ErrTooLarge
	}
	s := m.store
	s.mu.Lock()
	defer s.mu.Unlock()
	held, ok := s.leases[lease.Key]
	if !ok || held.token != lease.Token || !s.now().Before(held.expires) {
		return ErrLeaseLost
	}
	delete(s.leases, lease.Key)
	s.putLocked(lease.Key, e, ttl)
	return nil
}

// Release implements Leaser. Releasing a lease someone else now holds leaves
// theirs alone.
func (m *Memory) Release(_ context.Context, lease Lease) error {
	s := m.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if held, ok := s.leases[lease.Key]; ok && held.token == lease.Token {
		delete(s.leases, lease.Key)
	}
	return nil
}

// Subscribe implements Notifier: fn hears keys other clients of this store Set
// or Delete, synchronously, after the write.
func (m *Memory) Subscribe(_ context.Context, fn func(key string)) (func(), error) {
	s := m.store
	s.mu.Lock()
	id := s.nextSub
	s.nextSub++
	s.subs[id] = memorySub{client: m.client, fn: fn}
	s.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			s.mu.Lock()
			delete(s.subs, id)
			s.mu.Unlock()
		})
	}, nil
}

// Len reports how many keys the store holds, expired ones included until they
// are next touched.
func (m *Memory) Len() int {
	s := m.store
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.order.Len()
}

func (s *memoryStore) putLocked(key string, e Entry, ttl time.Duration) {
	item := &memoryItem{key: key, entry: cloneEntry(e), expires: s.now().Add(ttl)}
	if el, ok := s.items[key]; ok {
		el.Value = item
		s.order.MoveToFront(el)
		return
	}
	s.items[key] = s.order.PushFront(item)
	for s.order.Len() > s.maxEntries {
		s.removeLocked(s.order.Back())
	}
}

func (s *memoryStore) removeLocked(el *list.Element) {
	s.order.Remove(el)
	delete(s.items, el.Value.(*memoryItem).key)
}

// subscribersLocked returns the callbacks to run for a write by client; they
// run after the lock is released, so a callback may use the store.
func (s *memoryStore) subscribersLocked(client uint64) []func(string) {
	var fns []func(string)
	for _, sub := range s.subs {
		if sub.client != client {
			fns = append(fns, sub.fn)
		}
	}
	return fns
}

func deliver(fns []func(string), key string) {
	for _, fn := range fns {
		fn(key)
	}
}

// cloneEntry copies Value so a caller mutating its slice cannot change what the
// layer holds, and vice versa.
func cloneEntry(e Entry) Entry {
	e.Value = bytes.Clone(e.Value) // keeps an empty value distinct from nil
	return e
}

func memoryToken() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
