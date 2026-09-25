// Package redis is the codefly.dev/cache driver for any server speaking the
// Redis protocol: Redis, Valkey, ElastiCache, Memorystore. It is a Leaser, so a
// miss reaches the origin once across every process sharing the server, and a
// Notifier, so in-process layers above it drop a key another process changed.
//
// Importing it registers the "redis" driver with cache.Open. The connection is
// a redis:// or rediss:// URL, as a provider such as codefly's service-redis
// emits in its "cache" configuration group.
package redis

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/codefly-dev/interface-cache/go/cache"
)

// DriverName is the name the driver registers with cache.Open.
const DriverName = "redis"

func init() {
	cache.Register(DriverName, Open)
}

// Open builds a Layer from a provider's configuration. It does not contact the
// server: an unreachable cache degrades reads, it does not stop a consumer
// from starting.
func Open(_ context.Context, cfg cache.Config) (cache.Layer, error) {
	opts, err := goredis.ParseURL(cfg.Connection)
	if err != nil {
		return nil, fmt.Errorf("redis cache: invalid connection: %w", err)
	}
	return New(goredis.NewClient(opts)), nil
}

// Layer is a cache.Layer over one Redis keyspace.
type Layer struct {
	client goredis.UniversalClient
	prefix string
	origin string // identifies this client's own invalidations
}

var (
	_ cache.Leaser   = (*Layer)(nil)
	_ cache.Notifier = (*Layer)(nil)
)

// Option configures a Layer.
type Option func(*Layer)

// WithPrefix namespaces every key the layer writes, so several caches can
// share one server. Default "cache".
func WithPrefix(prefix string) Option { return func(l *Layer) { l.prefix = prefix } }

// New returns a Layer using client. The caller owns client.
func New(client goredis.UniversalClient, opts ...Option) *Layer {
	l := &Layer{client: client, prefix: "cache", origin: randomToken()}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

// Keys for one cache key share a hash tag, so they live in one slot and the
// scripts below stay valid on Redis Cluster.
func (l *Layer) valueKey(key string) string { return l.prefix + ":v:{" + key + "}" }
func (l *Layer) leaseKey(key string) string { return l.prefix + ":l:{" + key + "}" }
func (l *Layer) channel() string            { return l.prefix + ":invalidations" }

// Get implements cache.Layer.
func (l *Layer) Get(ctx context.Context, key string) (cache.Entry, error) {
	raw, err := l.client.Get(ctx, l.valueKey(key)).Bytes()
	if errors.Is(err, goredis.Nil) {
		return cache.Entry{}, cache.ErrMiss
	}
	if err != nil {
		return cache.Entry{}, err
	}
	return decode(raw)
}

// setScript stores a value, revokes any fill lease on it (a lease taken before
// this write may be about to store an older value) and tells other clients.
var setScript = goredis.NewScript(`
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
redis.call('DEL', KEYS[2])
redis.call('PUBLISH', ARGV[3], ARGV[4])
return 1`)

// Set implements cache.Layer.
func (l *Layer) Set(ctx context.Context, key string, e cache.Entry, ttl time.Duration) error {
	if ttl <= 0 {
		return l.Delete(ctx, key)
	}
	return setScript.Run(ctx, l.client,
		[]string{l.valueKey(key), l.leaseKey(key)},
		encode(e), milliseconds(ttl), l.channel(), l.notice(key)).Err()
}

var deleteScript = goredis.NewScript(`
redis.call('DEL', KEYS[1], KEYS[2])
redis.call('PUBLISH', ARGV[1], ARGV[2])
return 1`)

// Delete implements cache.Layer. It revokes any fill lease on key.
func (l *Layer) Delete(ctx context.Context, key string) error {
	return deleteScript.Run(ctx, l.client,
		[]string{l.valueKey(key), l.leaseKey(key)},
		l.channel(), l.notice(key)).Err()
}

// Acquire implements cache.Leaser.
func (l *Layer) Acquire(ctx context.Context, key string, ttl time.Duration) (cache.Lease, bool, error) {
	token := randomToken()
	ok, err := l.client.SetNX(ctx, l.leaseKey(key), token, ttl).Result()
	if err != nil || !ok {
		return cache.Lease{}, false, err
	}
	return cache.Lease{Key: key, Token: token}, true, nil
}

// fillScript stores a value only while the caller still holds the lease. A
// Set, a Delete or expiry removes the lease key, so a fill that raced any of
// them stores nothing.
var fillScript = goredis.NewScript(`
if redis.call('GET', KEYS[2]) ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
redis.call('DEL', KEYS[2])
return 1`)

// Fill implements cache.Leaser.
func (l *Layer) Fill(ctx context.Context, lease cache.Lease, e cache.Entry, ttl time.Duration) error {
	if ttl <= 0 {
		return l.Release(ctx, lease)
	}
	stored, err := fillScript.Run(ctx, l.client,
		[]string{l.valueKey(lease.Key), l.leaseKey(lease.Key)},
		lease.Token, encode(e), milliseconds(ttl)).Int()
	if err != nil {
		return err
	}
	if stored == 0 {
		return cache.ErrLeaseLost
	}
	return nil
}

var releaseScript = goredis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then redis.call('DEL', KEYS[1]) end
return 1`)

// Release implements cache.Leaser. Releasing a lease someone else now holds
// leaves theirs alone.
func (l *Layer) Release(ctx context.Context, lease cache.Lease) error {
	return releaseScript.Run(ctx, l.client, []string{l.leaseKey(lease.Key)}, lease.Token).Err()
}

// Subscribe implements cache.Notifier. Redis pub/sub is fire-and-forget: a
// notice published while this client is disconnected is lost, and layers
// above then keep that key until their own TTL.
func (l *Layer) Subscribe(ctx context.Context, fn func(key string)) (func(), error) {
	sub := l.client.Subscribe(ctx, l.channel())
	if _, err := sub.Receive(ctx); err != nil {
		_ = sub.Close()
		return nil, err
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for msg := range sub.Channel() {
			origin, key, ok := strings.Cut(msg.Payload, "\x00")
			if ok && origin != l.origin {
				fn(key)
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			_ = sub.Close()
			<-done
		})
	}, nil
}

// notice is the invalidation payload: which client wrote, and the key.
func (l *Layer) notice(key string) string { return l.origin + "\x00" + key }

// Entries are stored as: format byte, flags byte, FreshUntil as Unix
// nanoseconds (0 = none), the version length as a uvarint, the version, then
// the value bytes.
const (
	formatV1    = 1
	flagMissing = 1 << 0
)

func encode(e cache.Entry) []byte {
	buf := make([]byte, 0, 2+8+binary.MaxVarintLen64+len(e.Version)+len(e.Value))
	var flags byte
	if e.Missing {
		flags |= flagMissing
	}
	buf = append(buf, formatV1, flags)
	var fresh int64
	if !e.FreshUntil.IsZero() {
		fresh = e.FreshUntil.UnixNano()
	}
	buf = binary.BigEndian.AppendUint64(buf, uint64(fresh))
	buf = binary.AppendUvarint(buf, uint64(len(e.Version)))
	buf = append(buf, e.Version...)
	return append(buf, e.Value...)
}

func decode(raw []byte) (cache.Entry, error) {
	if len(raw) < 10 || raw[0] != formatV1 {
		return cache.Entry{}, fmt.Errorf("redis cache: entry in an unknown format (%d bytes)", len(raw))
	}
	e := cache.Entry{Missing: raw[1]&flagMissing != 0}
	if fresh := int64(binary.BigEndian.Uint64(raw[2:10])); fresh != 0 {
		e.FreshUntil = time.Unix(0, fresh)
	}
	n, size := binary.Uvarint(raw[10:])
	if size <= 0 || uint64(len(raw)-10-size) < n {
		return cache.Entry{}, errors.New("redis cache: truncated entry")
	}
	rest := raw[10+size:]
	e.Version = string(rest[:n])
	if !e.Missing {
		e.Value = append([]byte{}, rest[n:]...)
	}
	return e, nil
}

func milliseconds(d time.Duration) int64 {
	return max(d.Milliseconds(), 1)
}

func randomToken() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err) // crypto/rand does not fail on supported platforms
	}
	return hex.EncodeToString(b[:])
}
