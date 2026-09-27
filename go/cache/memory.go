package cache

import (
	"bytes"
	"container/list"
	"context"
	"crypto/rand"
	"encoding/hex"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// Memory is an in-process, size-bounded LRU layer. It is the reference
// implementation: it declares every layer capability a mode needs — Leases,
// Notifies (before the write returns), Flushes, Fences and a ByteBudget — so
// the whole mode matrix is testable without a server. Share returns another
// client of the same store, the way a second process would hold a second
// Redis client, so it can be the shared layer under several stacks.
//
// Notifications are delivered synchronously, in subscription order, and are
// never lost, so Memory needs no resync signal.
//
// # Eviction
//
// The store holds at most MaxEntries keys and MaxBytes bytes, counting each
// key, value and version plus a fixed per-entry overhead. A write that would
// exceed either evicts the least recently used entries (a Get or a write makes
// an entry the most recent) until both hold; an entry expires at its TTL and
// is dropped when next touched. A value over MaxValueBytes, or an entry larger
// than MaxBytes on its own, is refused with ErrTooLarge and stores nothing.
// Eviction is silent: it notifies no subscriber, since a missing copy is a
// miss, never a stale read. Fill leases are not entries and are not evicted.
type Memory struct {
	store  *memoryStore
	client uint64 // identifies this handle's own writes
}

var (
	_ Leaser   = (*Memory)(nil)
	_ Notifier = (*Memory)(nil)
	_ Flusher  = (*Memory)(nil)
	_ Fencer   = (*Memory)(nil)
)

// memoryEntryOverhead approximates what the store spends per entry beyond its
// key, value and version: the list element, the map slot and the item.
const memoryEntryOverhead = 128

type memoryStore struct {
	maxEntries    int
	maxValueBytes int
	maxBytes      int64
	now           func() time.Time
	nextClient    atomic.Uint64

	mu      sync.Mutex
	order   *list.List // front = most recently used
	items   map[string]*list.Element
	bytes   int64
	leases  map[string]memoryLease
	subs    map[uint64]memorySub
	nextSub uint64
}

type memoryItem struct {
	key     string
	entry   Entry
	expires time.Time
	size    int64
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

// MaxBytes is the store's byte budget: the keys, values and versions it holds,
// plus a fixed overhead per entry, never exceed it. The least recently used
// entries are evicted to make room. Default 64 MiB. It is the ByteBudget the
// layer declares.
func MaxBytes(n int64) MemoryOption { return func(s *memoryStore) { s.maxBytes = n } }

// MemoryClock makes the store read time from now, for expiry and leases.
// Default time.Now.
func MemoryClock(now func() time.Time) MemoryOption { return func(s *memoryStore) { s.now = now } }

// NewMemory returns a client of a new, empty store.
func NewMemory(opts ...MemoryOption) *Memory {
	s := &memoryStore{
		maxEntries:    10_000,
		maxValueBytes: 1 << 20,
		maxBytes:      64 << 20,
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

// Capabilities implements Declarer.
func (m *Memory) Capabilities() Capabilities {
	return Capabilities{
		Leases:      true,
		Notifies:    true,
		NoticeBound: NoticesBeforeReturn,
		Flushes:     true,
		Fences:      true,
		ByteBudget:  m.store.maxBytes,
	}
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
	return m.set(ctx, key, e, ttl, false)
}

// SetFenced implements Fencer: it stores e unless key holds an unexpired entry
// of a greater Sequence.
func (m *Memory) SetFenced(ctx context.Context, key string, e Entry, ttl time.Duration) error {
	return m.set(ctx, key, e, ttl, true)
}

func (m *Memory) set(ctx context.Context, key string, e Entry, ttl time.Duration, fenced bool) error {
	if !m.store.fits(key, e) {
		return ErrTooLarge
	}
	if ttl <= 0 {
		return m.Delete(ctx, key)
	}
	s := m.store
	s.mu.Lock()
	if fenced && s.newerLocked(key, e) {
		s.mu.Unlock()
		return ErrFenced
	}
	delete(s.leases, key)
	s.putLocked(key, e, ttl)
	notify := s.subscribersLocked(m.client)
	s.mu.Unlock()
	deliver(notify, key)
	return nil
}

// newerLocked reports whether key holds an unexpired entry of a greater
// sequence than e.
func (s *memoryStore) newerLocked(key string, e Entry) bool {
	el, ok := s.items[key]
	if !ok {
		return false
	}
	item := el.Value.(*memoryItem)
	return s.now().Before(item.expires) && item.entry.Sequence > e.Sequence
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
	if !m.store.fits(lease.Key, e) {
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

// Subscribe implements Notifier: fn hears keys other clients of this store Set,
// Delete or Flush, synchronously, after the write and before it returns. A
// client's own writes are not reported to its own subscribers, which the
// Notifier contract allows.
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

// Flush implements Flusher: it drops every entry and lease in the store, and
// reports each dropped key to other clients' subscribers.
func (m *Memory) Flush(context.Context) error {
	s := m.store
	s.mu.Lock()
	keys := make([]string, 0, len(s.items))
	for key := range s.items {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	s.order.Init()
	clear(s.items)
	clear(s.leases)
	s.bytes = 0
	notify := s.subscribersLocked(m.client)
	s.mu.Unlock()
	for _, key := range keys {
		deliver(notify, key)
	}
	return nil
}

// Len reports how many keys the store holds, expired ones included until they
// are next touched.
func (m *Memory) Len() int {
	s := m.store
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.order.Len()
}

// Bytes reports how much of the byte budget the store uses.
func (m *Memory) Bytes() int64 {
	s := m.store
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bytes
}

func entrySize(key string, e Entry) int64 {
	return int64(len(key)+len(e.Value)+len(e.Version)) + memoryEntryOverhead
}

// fits reports whether e may be stored at all: under the value cap, and not
// larger than the whole budget on its own.
func (s *memoryStore) fits(key string, e Entry) bool {
	return len(e.Value) <= s.maxValueBytes && entrySize(key, e) <= s.maxBytes
}

func (s *memoryStore) putLocked(key string, e Entry, ttl time.Duration) {
	item := &memoryItem{key: key, entry: cloneEntry(e), expires: s.now().Add(ttl), size: entrySize(key, e)}
	if el, ok := s.items[key]; ok {
		s.bytes += item.size - el.Value.(*memoryItem).size
		el.Value = item
		s.order.MoveToFront(el)
	} else {
		s.items[key] = s.order.PushFront(item)
		s.bytes += item.size
	}
	for s.order.Len() > s.maxEntries || s.bytes > s.maxBytes {
		s.removeLocked(s.order.Back())
	}
}

func (s *memoryStore) removeLocked(el *list.Element) {
	item := el.Value.(*memoryItem)
	s.order.Remove(el)
	delete(s.items, item.key)
	s.bytes -= item.size
}

// subscribersLocked returns the callbacks to run for a write by client, in
// subscription order; they run after the lock is released, so a callback may
// use the store.
func (s *memoryStore) subscribersLocked(client uint64) []func(string) {
	ids := make([]uint64, 0, len(s.subs))
	for id, sub := range s.subs {
		if sub.client != client {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	fns := make([]func(string), len(ids))
	for i, id := range ids {
		fns[i] = s.subs[id].fn
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
