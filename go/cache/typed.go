package cache

import (
	"context"
	"encoding/json"
)

// Codec converts a value to and from the bytes a stack stores. A stack holds
// one kind of value, so it takes one codec.
type Codec[T any] interface {
	Marshal(v T) ([]byte, error)
	Unmarshal(data []byte) (T, error)
}

// JSON is a Codec using encoding/json.
func JSON[T any]() Codec[T] { return jsonCodec[T]{} }

type jsonCodec[T any] struct{}

func (jsonCodec[T]) Marshal(v T) ([]byte, error) { return json.Marshal(v) }

func (jsonCodec[T]) Unmarshal(data []byte) (T, error) {
	var v T
	err := json.Unmarshal(data, &v)
	return v, err
}

// Typed is a Stack that stores values of type T through a Codec.
type Typed[T any] struct {
	stack *Stack
	codec Codec[T]
}

// NewTyped wraps stack with codec.
func NewTyped[T any](stack *Stack, codec Codec[T]) *Typed[T] {
	return &Typed[T]{stack: stack, codec: codec}
}

// Get returns the decoded value for key, with Stack.Get's errors.
func (t *Typed[T]) Get(ctx context.Context, key string) (T, error) {
	data, err := t.stack.Get(ctx, key)
	if err != nil {
		var zero T
		return zero, err
	}
	return t.codec.Unmarshal(data)
}

// Set encodes v and writes it as Stack.Set does.
func (t *Typed[T]) Set(ctx context.Context, key string, v T) error {
	data, err := t.codec.Marshal(v)
	if err != nil {
		return err
	}
	return t.stack.Set(ctx, key, data)
}

// Delete removes key as Stack.Delete does.
func (t *Typed[T]) Delete(ctx context.Context, key string) error {
	return t.stack.Delete(ctx, key)
}

// Stack returns the underlying stack.
func (t *Typed[T]) Stack() *Stack { return t.stack }
