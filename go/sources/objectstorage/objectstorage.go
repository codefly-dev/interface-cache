// Package objectstorage makes a codefly object-storage gateway the origin of a
// cache stack. It adapts the gateway's generated gRPC client to cache.Store:
// loads are conditional on the ETag the stack already holds, so an unchanged
// object revalidates without moving its bytes.
//
// The gateway knows nothing about caching; this adapter only uses its
// ordinary, versioned reads and writes.
package objectstorage

import (
	"context"
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"

	"github.com/codefly-dev/interface-cache/go/cache"
)

// Source is a cache.Store over an object-storage gateway.
type Source struct {
	client   storagev0.ObjectStorageClient
	maxBytes int64
	chunk    int
}

var _ cache.Store = (*Source)(nil)

// Option configures a Source.
type Option func(*Source)

// MaxBytes caps the size of an object the source loads into a cache; a larger
// one is ErrTooLarge, and the caller should read it from the gateway directly
// (or by presigned URL). Default 8 MiB.
func MaxBytes(n int64) Option { return func(s *Source) { s.maxBytes = n } }

// New returns a Source using client. Authentication (the gateway's
// x-codefly-token) belongs to the client's connection, not to the source.
func New(client storagev0.ObjectStorageClient, opts ...Option) *Source {
	s := &Source{client: client, maxBytes: 8 << 20, chunk: 256 << 10}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Load implements cache.Source. The object's ETag is the entry's version.
func (s *Source) Load(ctx context.Context, key string, ifNotVersion string) (cache.Entry, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // abandons the stream when the object is refused part-way
	stream, err := s.client.Get(ctx, &storagev0.GetRequest{Key: key, IfNoneMatch: ifNotVersion})
	if err != nil {
		return cache.Entry{}, translate(err)
	}
	first, err := stream.Recv()
	if err != nil {
		return cache.Entry{}, translate(err)
	}
	header := first.GetHeader()
	if header == nil {
		return cache.Entry{}, errors.New("objectstorage: gateway sent data before the header")
	}
	if header.GetNotModified() {
		return cache.Entry{}, cache.ErrNotModified
	}
	info := header.GetInfo()
	if info.GetSize() > s.maxBytes {
		return cache.Entry{}, fmt.Errorf("%w: object %q is %d bytes, over the source's %d", cache.ErrTooLarge, key, info.GetSize(), s.maxBytes)
	}
	value := make([]byte, 0, max(info.GetSize(), 0))
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return cache.Entry{}, translate(err)
		}
		value = append(value, msg.GetData()...)
		if int64(len(value)) > s.maxBytes {
			return cache.Entry{}, fmt.Errorf("%w: object %q exceeds the source's %d bytes", cache.ErrTooLarge, key, s.maxBytes)
		}
	}
	return cache.Entry{Value: value, Version: info.GetEtag()}, nil
}

// Put implements cache.Store. It returns the stored object's ETag.
func (s *Source) Put(ctx context.Context, key string, value []byte) (string, error) {
	stream, err := s.client.Put(ctx)
	if err != nil {
		return "", translate(err)
	}
	header := &storagev0.PutRequest{Kind: &storagev0.PutRequest_Header{Header: &storagev0.PutHeader{
		Key: key, TotalSize: int64(len(value)),
	}}}
	if err := stream.Send(header); err != nil {
		return "", sendError(stream, err)
	}
	for off := 0; off < len(value); off += s.chunk {
		end := min(off+s.chunk, len(value))
		if err := stream.Send(&storagev0.PutRequest{Kind: &storagev0.PutRequest_Data{Data: value[off:end]}}); err != nil {
			return "", sendError(stream, err)
		}
	}
	res, err := stream.CloseAndRecv()
	if err != nil {
		return "", translate(err)
	}
	return res.GetEtag(), nil
}

// Remove implements cache.Store. Removing an absent object is not an error.
func (s *Source) Remove(ctx context.Context, key string) error {
	_, err := s.client.Delete(ctx, &storagev0.DeleteRequest{Key: key})
	if err != nil && status.Code(err) != codes.NotFound {
		return translate(err)
	}
	return nil
}

// sendError surfaces the server's status: a failed Send reports only io.EOF,
// and the reason arrives on CloseAndRecv.
func sendError(stream storagev0.ObjectStorage_PutClient, err error) error {
	if errors.Is(err, io.EOF) {
		_, err = stream.CloseAndRecv()
	}
	return translate(err)
}

func translate(err error) error {
	if status.Code(err) == codes.NotFound {
		return fmt.Errorf("%w: %v", cache.ErrNotFound, err)
	}
	return err
}
