package kv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/eqr/ravendb-go-client"
)

const (
	defaultCollection = "kv"
	listPageSize      = 1024
)

// ErrNotFound is returned by Get when the key does not exist.
var ErrNotFound = errors.New("kv: key not found")

// Store is a JSON key-value store kept as RavenDB documents.
// Each key is one document with ID "<collection>/<appName>/<key>".
// RavenDB IDs are case-insensitive, so keys that differ only by case collide.
type Store struct {
	docs       *ravendb.DocumentStore
	collection string
	appName    string
}

// NewStore returns a store for the given collection and app name.
// An empty collection defaults to "kv".
func NewStore(docs *ravendb.DocumentStore, collection string, appName string) Store {
	collection = strings.TrimSpace(collection)
	if collection == "" {
		collection = defaultCollection
	}
	return Store{docs: docs, collection: collection, appName: strings.TrimSpace(appName)}
}

// DocumentID returns the RavenDB document ID used for key.
func (s Store) DocumentID(key string) string {
	return s.idPrefix() + strings.TrimSpace(key)
}

// Set inserts or overwrites the value for key.
func (s Store) Set(ctx context.Context, key string, value any) error {
	id, err := s.id(key)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal value: %w", err)
	}
	doc := map[string]any{
		"key":     strings.TrimSpace(key),
		"appName": s.appName,
		"value":   json.RawMessage(encoded),
		ravendb.MetadataKey: map[string]any{
			ravendb.MetadataCollection: s.collection,
		},
	}
	return s.execute(ctx, ravendb.NewPutDocumentCommand(id, nil, doc))
}

// Get returns the raw JSON value for key, or ErrNotFound.
func (s Store) Get(ctx context.Context, key string) (json.RawMessage, error) {
	id, err := s.id(key)
	if err != nil {
		return nil, err
	}
	cmd, err := ravendb.NewGetDocumentsCommand([]string{id}, nil, false)
	if err != nil {
		return nil, err
	}
	if err := s.execute(ctx, cmd); err != nil {
		return nil, err
	}
	if cmd.Result == nil || len(cmd.Result.Results) == 0 || cmd.Result.Results[0] == nil {
		return nil, ErrNotFound
	}
	value, err := json.Marshal(cmd.Result.Results[0]["value"])
	if err != nil {
		return nil, fmt.Errorf("encode value: %w", err)
	}
	return value, nil
}

// Delete removes key. Deleting a missing key is not an error.
func (s Store) Delete(ctx context.Context, key string) error {
	id, err := s.id(key)
	if err != nil {
		return err
	}
	return s.execute(ctx, ravendb.NewDeleteDocumentCommand(id, nil))
}

// Exists reports whether key exists.
func (s Store) Exists(ctx context.Context, key string) (bool, error) {
	id, err := s.id(key)
	if err != nil {
		return false, err
	}
	cmd := ravendb.NewHeadDocumentCommand(id, nil)
	if err := s.execute(ctx, cmd); err != nil {
		return false, err
	}
	return cmd.Exists(), nil
}

// List returns all keys of this app that start with prefix.
// Prefix matching is case-insensitive.
func (s Store) List(ctx context.Context, prefix string) ([]string, error) {
	if s.docs == nil {
		return nil, errors.New("kv document store is nil")
	}
	startWith := s.idPrefix() + strings.TrimSpace(prefix)
	keys := make([]string, 0)
	for start := 0; ; {
		cmd, err := ravendb.NewGetDocumentsCommandFull(startWith, "", "", "", start, listPageSize, false)
		if err != nil {
			return nil, err
		}
		if err := s.execute(ctx, cmd); err != nil {
			return nil, err
		}
		if cmd.Result == nil {
			return keys, nil
		}
		for _, doc := range cmd.Result.Results {
			// An appName containing "/" can share an ID prefix with another app.
			if appName, _ := doc["appName"].(string); appName != s.appName {
				continue
			}
			if key, ok := doc["key"].(string); ok {
				keys = append(keys, key)
			}
		}
		if len(cmd.Result.Results) < listPageSize {
			return keys, nil
		}
		start += len(cmd.Result.Results)
	}
}

func (s Store) idPrefix() string {
	return s.collection + "/" + s.appName + "/"
}

func (s Store) id(key string) (string, error) {
	if s.docs == nil {
		return "", errors.New("kv document store is nil")
	}
	if strings.TrimSpace(key) == "" {
		return "", errors.New("key is required")
	}
	return s.DocumentID(key), nil
}

func (s Store) execute(ctx context.Context, cmd ravendb.RavenCommand) error {
	return s.docs.GetRequestExecutor("").ExecuteCommandContext(ctx, cmd, nil)
}

// TypedStore wraps Store with typed values.
type TypedStore[T any] struct {
	store Store
}

// NewTypedStore returns a typed store for the given collection and app name.
func NewTypedStore[T any](docs *ravendb.DocumentStore, collection string, appName string) TypedStore[T] {
	return TypedStore[T]{store: NewStore(docs, collection, appName)}
}

// Set inserts or overwrites the value for key.
func (s TypedStore[T]) Set(ctx context.Context, key string, value T) error {
	return s.store.Set(ctx, key, value)
}

// Get returns the value for key, or ErrNotFound.
func (s TypedStore[T]) Get(ctx context.Context, key string) (T, error) {
	var out T
	raw, err := s.store.Get(ctx, key)
	if err != nil {
		return out, err
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		var zero T
		return zero, err
	}
	return out, nil
}

// Delete removes key. Deleting a missing key is not an error.
func (s TypedStore[T]) Delete(ctx context.Context, key string) error {
	return s.store.Delete(ctx, key)
}

// Exists reports whether key exists.
func (s TypedStore[T]) Exists(ctx context.Context, key string) (bool, error) {
	return s.store.Exists(ctx, key)
}

// List returns all keys of this app that start with prefix.
func (s TypedStore[T]) List(ctx context.Context, prefix string) ([]string, error) {
	return s.store.List(ctx, prefix)
}
