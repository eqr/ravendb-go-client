package ravenlease

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/eqr/ravendb-go-client"
)

// CompareExchangeStore keeps the lease as a cluster-wide compare-exchange value under one key.
type CompareExchangeStore[T any] struct {
	docs *ravendb.DocumentStore
	key  string
}

// NewCompareExchangeStore returns a store on docs' default database. The caller owns docs.
func NewCompareExchangeStore[T any](docs *ravendb.DocumentStore, key string) *CompareExchangeStore[T] {
	return &CompareExchangeStore[T]{docs: docs, key: key}
}

func (s *CompareExchangeStore[T]) Get(ctx context.Context) (*Lease[T], int64, error) {
	op, err := ravendb.NewGetCompareExchangeValueOperation(reflect.TypeOf(&Lease[T]{}), s.key)
	if err != nil {
		return nil, 0, err
	}
	if err := s.send(ctx, op); err != nil {
		return nil, 0, err
	}
	if op.Command.Result == nil || op.Command.Result.Value == nil {
		return nil, 0, nil
	}
	b, err := json.Marshal(op.Command.Result.Value)
	if err != nil {
		return nil, 0, err
	}
	var lease Lease[T]
	if err := json.Unmarshal(b, &lease); err != nil {
		return nil, 0, fmt.Errorf("decode lease %q: %w", s.key, err)
	}
	return &lease, op.Command.Result.Index, nil
}

func (s *CompareExchangeStore[T]) Put(ctx context.Context, lease Lease[T], index int64) (int64, bool, error) {
	op, err := ravendb.NewPutCompareExchangeValueOperation(s.key, &lease, index)
	if err != nil {
		return 0, false, err
	}
	if err := s.send(ctx, op); err != nil {
		return 0, false, err
	}
	if op.Command.Result == nil {
		return 0, false, errors.New("RavenDB returned no compare-exchange result")
	}
	return op.Command.Result.Index, op.Command.Result.IsSuccessful, nil
}

func (s *CompareExchangeStore[T]) Delete(ctx context.Context, index int64) (bool, error) {
	op, err := ravendb.NewDeleteCompareExchangeValueOperation(reflect.TypeOf(&Lease[T]{}), s.key, index)
	if err != nil {
		return false, err
	}
	if err := s.send(ctx, op); err != nil {
		return false, err
	}
	if op.Command.Result == nil {
		return false, errors.New("RavenDB returned no compare-exchange result")
	}
	return op.Command.Result.IsSuccessful, nil
}

func (s *CompareExchangeStore[T]) send(ctx context.Context, op ravendb.IOperation) error {
	return s.docs.Operations().ForDatabase(s.docs.GetDatabase()).SendContext(ctx, op, nil)
}
