package objectstorage_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"

	"github.com/codefly-dev/interface-cache/go/cache"
	"github.com/codefly-dev/interface-cache/go/sources/objectstorage"
)

const gatewayModule = "github.com/codefly-dev/service-object-storage"

// gateway is a real object-storage gateway, built from the module version
// go.mod pins and run on its in-memory backend.
var gateway storagev0.ObjectStorageClient

func TestMain(m *testing.M) {
	stop, err := startGateway()
	if err != nil {
		fmt.Fprintln(os.Stderr, "start object-storage gateway:", err)
		os.Exit(1)
	}
	code := m.Run()
	stop()
	os.Exit(code)
}

func startGateway() (func(), error) {
	dir, err := os.MkdirTemp("", "objectstorage-source")
	if err != nil {
		return nil, err
	}
	// Install the gateway at the version go.mod pins, resolved from its own
	// module: building it inside this one would pull every cloud SDK it links
	// into this module's go.sum, and on to every consumer.
	version, err := exec.Command("go", "list", "-m", "-f", "{{.Version}}", gatewayModule).Output()
	if err != nil {
		return nil, fmt.Errorf("resolve pinned gateway version: %w", err)
	}
	install := exec.Command("go", "install", gatewayModule+"/cmd/service-object-storage@"+strings.TrimSpace(string(version)))
	install.Env = append(os.Environ(), "GOBIN="+dir, "GOFLAGS=")
	if out, err := install.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("go install gateway: %w: %s", err, out)
	}
	bin := filepath.Join(dir, "service-object-storage")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	address := listener.Addr().String()
	_ = listener.Close()

	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(),
		"SOS_BACKEND=mem",
		"SOS_BUCKET=cache-source-test",
		"SOS_ALLOW_ANONYMOUS=true",
		"SOS_LISTEN="+address,
		"SOS_PROBE_INTERVAL=200ms",
	)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	stop := func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		_ = os.RemoveAll(dir)
	}
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		stop()
		return nil, err
	}
	gateway = storagev0.NewObjectStorageClient(conn)
	deadline := time.Now().Add(30 * time.Second)
	for {
		ready, err := gateway.Ready(context.Background(), &storagev0.ReadyRequest{})
		if err == nil && ready.GetReady() {
			return func() { _ = conn.Close(); stop() }, nil
		}
		if time.Now().After(deadline) {
			_ = conn.Close()
			stop()
			return nil, fmt.Errorf("gateway at %s never became ready: %v %v", address, ready, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func key(t *testing.T) string { return t.Name() }

func TestLoadAndConditionalLoad(t *testing.T) {
	ctx := context.Background()
	src := objectstorage.New(gateway)
	etag, err := src.Put(ctx, key(t), []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	e, err := src.Load(ctx, key(t), "")
	if err != nil {
		t.Fatal(err)
	}
	if string(e.Value) != "hello" || e.Version != etag || etag == "" {
		t.Fatalf("Load = %q version %q, want %q version %q", e.Value, e.Version, "hello", etag)
	}
	if _, err := src.Load(ctx, key(t), etag); !errors.Is(err, cache.ErrNotModified) {
		t.Fatalf("Load with the current ETag = %v, want ErrNotModified", err)
	}
	newer, err := src.Put(ctx, key(t), []byte("changed"))
	if err != nil {
		t.Fatal(err)
	}
	e, err = src.Load(ctx, key(t), etag)
	if err != nil || string(e.Value) != "changed" || e.Version != newer {
		t.Fatalf("Load with a stale ETag = %q version %q, %v; want the new object", e.Value, e.Version, err)
	}
}

func TestLoadAbsentIsNotFound(t *testing.T) {
	if _, err := objectstorage.New(gateway).Load(context.Background(), "never-written", ""); !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("Load(absent) = %v, want ErrNotFound", err)
	}
}

func TestLargeObjectIsRefused(t *testing.T) {
	ctx := context.Background()
	src := objectstorage.New(gateway, objectstorage.MaxBytes(1024))
	if _, err := src.Put(ctx, key(t), bytes.Repeat([]byte("x"), 4096)); err != nil {
		t.Fatal(err)
	}
	if _, err := src.Load(ctx, key(t), ""); !errors.Is(err, cache.ErrTooLarge) {
		t.Fatalf("Load over MaxBytes = %v, want ErrTooLarge", err)
	}
}

func TestMultiChunkRoundTrip(t *testing.T) {
	ctx := context.Background()
	src := objectstorage.New(gateway)
	value := make([]byte, 700<<10) // several Put chunks and Get messages
	for i := range value {
		value[i] = byte(i % 251)
	}
	if _, err := src.Put(ctx, key(t), value); err != nil {
		t.Fatal(err)
	}
	e, err := src.Load(ctx, key(t), "")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(e.Value, value) {
		t.Fatalf("round trip changed a %d-byte object", len(value))
	}
}

func TestRemove(t *testing.T) {
	ctx := context.Background()
	src := objectstorage.New(gateway)
	if _, err := src.Put(ctx, key(t), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := src.Remove(ctx, key(t)); err != nil {
		t.Fatal(err)
	}
	if err := src.Remove(ctx, key(t)); err != nil {
		t.Fatalf("Remove(absent) = %v, want nil", err)
	}
	if _, err := src.Load(ctx, key(t), ""); !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("Load after Remove = %v, want ErrNotFound", err)
	}
}

// countingSource counts what reaches the gateway, to prove the stack
// revalidates rather than reloads.
type countingSource struct {
	*objectstorage.Source
	loads, conditional atomic.Int32
}

func (c *countingSource) Load(ctx context.Context, key, ifNot string) (cache.Entry, error) {
	c.loads.Add(1)
	if ifNot != "" {
		c.conditional.Add(1)
	}
	return c.Source.Load(ctx, key, ifNot)
}

func TestStackOverObjectStorage(t *testing.T) {
	ctx := context.Background()
	src := &countingSource{Source: objectstorage.New(gateway)}
	s, err := cache.New(ctx,
		cache.WithTier(cache.NewMemory(), 50*time.Millisecond),
		cache.WithOrigin(src),
		cache.WithStaleWindow(time.Minute),
		cache.WithTTLJitter(0),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	p := cache.NewPartition("tenant")

	if err := s.Set(ctx, p, key(t), []byte("doc v1")); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if v, err := s.Get(ctx, p, key(t)); err != nil || string(v) != "doc v1" {
			t.Fatalf("Get = %q, %v", v, err)
		}
	}
	if n := src.loads.Load(); n != 1 {
		t.Fatalf("gateway loaded %d times for three fresh reads, want 1", n)
	}

	time.Sleep(80 * time.Millisecond) // past freshness, inside the stale window
	if v, err := s.Get(ctx, p, key(t)); err != nil || string(v) != "doc v1" {
		t.Fatalf("Get after expiry = %q, %v", v, err)
	}
	if n := src.conditional.Load(); n != 1 {
		t.Fatalf("expired entry was reloaded unconditionally (%d conditional loads)", n)
	}
}
