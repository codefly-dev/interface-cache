package redis_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/codefly-dev/interface-cache/go/cache"
	"github.com/codefly-dev/interface-cache/go/cache/cachetest"
	"github.com/codefly-dev/interface-cache/go/redis"
)

// redisImage is the runtime image codefly's service-redis pins
// (runtime-image.json in codefly-dev/service-redis), so the driver is proven
// against the server a codefly provider actually runs. Move it with that pin.
const redisImage = "docker.io/codeflydev/redis@sha256:0887ab9b5ee268aa087fc201c21be402077b996ae7df9d6b43151aa9f7ccb581"

// infraEnv set to "required" turns a missing Redis from a skip into a failure,
// so CI cannot report green without having talked to a server.
const infraEnv = "CACHE_INFRA_TESTS"

var (
	serverOnce sync.Once
	serverURL  string
	serverErr  error
)

// server returns the connection URL of a real Redis: REDIS_URL when set,
// otherwise a container of redisImage started with a password, the way
// service-redis starts it.
func server(t *testing.T) string {
	t.Helper()
	serverOnce.Do(func() {
		if u := os.Getenv("REDIS_URL"); u != "" {
			serverURL = u
			return
		}
		serverURL, serverErr = startContainer()
	})
	if serverErr != nil {
		if os.Getenv(infraEnv) == "required" {
			t.Fatalf("infrastructure tests are required but no Redis is available: %v", serverErr)
		}
		t.Skipf("no Redis available (set REDIS_URL, or make docker usable): %v", serverErr)
	}
	return serverURL
}

var containerID string

func startContainer() (string, error) {
	if err := exec.Command("docker", "info").Run(); err != nil {
		return "", fmt.Errorf("docker is not usable: %w", err)
	}
	const password = "cachetest-secret"
	out, err := exec.Command("docker", "run", "-d", "--rm", "-p", "127.0.0.1::6379",
		redisImage, "redis-server", "--requirepass", password).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("docker run: %w: %s", err, out)
	}
	containerID = strings.TrimSpace(string(out))
	out, err = exec.Command("docker", "port", containerID, "6379/tcp").Output()
	if err != nil {
		return "", fmt.Errorf("docker port: %w", err)
	}
	address := strings.TrimSpace(strings.Split(string(out), "\n")[0])
	u := (&url.URL{Scheme: "redis", Host: address, User: url.UserPassword("", password)}).String()

	deadline := time.Now().Add(60 * time.Second)
	for {
		opts, _ := goredis.ParseURL(u)
		client := goredis.NewClient(opts)
		err := client.Ping(context.Background()).Err()
		_ = client.Close()
		if err == nil {
			return u, nil
		}
		if time.Now().After(deadline) {
			return "", fmt.Errorf("redis in container %s never answered PING: %w", containerID, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func TestMain(m *testing.M) {
	code := m.Run()
	if containerID != "" {
		_ = exec.Command("docker", "rm", "-f", containerID).Run()
	}
	os.Exit(code)
}

func newLayer(t *testing.T, namespace string) cache.Layer {
	t.Helper()
	opts, err := goredis.ParseURL(server(t))
	if err != nil {
		t.Fatal(err)
	}
	client := goredis.NewClient(opts)
	t.Cleanup(func() { _ = client.Close() })
	return redis.New(client, redis.WithPrefix(namespace))
}

func TestConformance(t *testing.T) {
	cachetest.Run(t, cachetest.Harness{New: newLayer})
}

func TestStackFillOnceAcrossProcesses(t *testing.T) {
	cachetest.RunStack(t, cachetest.Harness{New: newLayer})
}

// Another process's write evicts this process's in-memory copy.
func TestStackEvictsMemoryOnRemoteWrite(t *testing.T) {
	ctx := context.Background()
	ns := cachetest.Namespace(t)
	newStack := func() *cache.Stack {
		s, err := cache.New(ctx,
			cache.WithTier(cache.NewMemory(), time.Hour),
			cache.WithTier(newLayer(t, ns), time.Hour),
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(s.Close)
		return s
	}
	a, b := newStack(), newStack()
	if err := a.Set(ctx, "k", []byte("one")); err != nil {
		t.Fatal(err)
	}
	if v, err := b.Get(ctx, "k"); err != nil || string(v) != "one" {
		t.Fatalf("b.Get = %q, %v", v, err)
	}
	if err := a.Set(ctx, "k", []byte("two")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		v, err := b.Get(ctx, "k")
		if err == nil && string(v) == "two" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("b still serves %q from memory after a's write", v)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// The provider's configuration, as service-redis emits it, opens this driver.
func TestOpenFromProviderConfiguration(t *testing.T) {
	ctx := context.Background()
	lookup := lookup{"cache.driver": redis.DriverName, "cache.connection": server(t)}
	layer, err := cache.Open(ctx, lookup)
	if err != nil {
		t.Fatal(err)
	}
	if err := layer.Set(ctx, cachetest.Namespace(t), cache.Entry{Value: []byte("v")}, time.Minute); err != nil {
		t.Fatalf("layer opened from configuration cannot write: %v", err)
	}
}

// A wrong password is refused by the server, not silently accepted.
func TestWrongPasswordIsRefused(t *testing.T) {
	u, err := url.Parse(server(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, hasPassword := u.User.Password(); !hasPassword {
		t.Skip("REDIS_URL carries no password")
	}
	u.User = url.UserPassword("", "wrong")
	layer, err := redis.Open(context.Background(), cache.Config{Driver: redis.DriverName, Connection: u.String()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := layer.Get(context.Background(), "k"); err == nil || errors.Is(err, cache.ErrMiss) {
		t.Fatalf("Get with a wrong password = %v, want an authentication error", err)
	}
}

// A stack whose Redis is unreachable still serves from the origin.
func TestUnreachableRedisDegrades(t *testing.T) {
	ctx := context.Background()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close() // nothing listens there now

	layer, err := redis.Open(ctx, cache.Config{Driver: redis.DriverName, Connection: "redis://" + address})
	if err != nil {
		t.Fatal(err)
	}
	origin := cache.SourceFunc(func(context.Context, string, string) (cache.Entry, error) {
		return cache.Entry{Value: []byte("from origin")}, nil
	})
	s, err := cache.New(ctx, cache.WithTier(layer, time.Minute), cache.WithOrigin(origin))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for range 3 {
		v, err := s.Get(ctx, "k")
		if err != nil || string(v) != "from origin" {
			t.Fatalf("Get with Redis down = %q, %v; want the origin's value", v, err)
		}
	}
}

type lookup map[string]string

func (l lookup) Configuration(group, key string) (string, error) { return l.get(group, key) }
func (l lookup) Secret(group, key string) (string, error)        { return l.get(group, key) }
func (l lookup) get(group, key string) (string, error) {
	if v, ok := l[group+"."+key]; ok {
		return v, nil
	}
	return "", errors.New("not configured: " + group + "." + key)
}
