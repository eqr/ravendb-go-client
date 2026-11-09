package ravenkv

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/eqr/ravendb-go-client"
)

const listPageSize = 1024

// ErrNotFound is returned by Get when the key does not exist.
var ErrNotFound = errors.New("ravenkv: key not found")

// KVEntry is the stored document, in collection "KVEntries". Value holds the raw JSON value.
type KVEntry struct {
	Key       string          `json:"Key"`
	Value     json.RawMessage `json:"Value"`
	UpdatedAt time.Time       `json:"UpdatedAt"`
}

// Store is a JSON key-value store kept as RavenDB documents.
// Each key is one document with ID "KV/{app}/{collection}/{hex(key)}".
// Writes run in cluster-wide transactions.
type Store struct {
	docs       *ravendb.DocumentStore
	app        string
	collection string
}

// NewStore returns a store for collection and app, in pbclient.NewKVStore order.
// Neither may contain "/". app may be empty.
func NewStore(docs *ravendb.DocumentStore, collection string, app string) Store {
	return Store{docs: docs, app: strings.TrimSpace(app), collection: strings.TrimSpace(collection)}
}

// DocumentID returns the RavenDB document ID used for key.
func (s Store) DocumentID(key string) string {
	return s.idPrefix() + hex.EncodeToString([]byte(strings.TrimSpace(key)))
}

// Set inserts or overwrites the value for key.
func (s Store) Set(ctx context.Context, key string, value any) error {
	return s.Update(ctx, func(tx *Tx) error { return tx.Set(key, value) })
}

// Delete removes key. Deleting a missing key is not an error.
func (s Store) Delete(ctx context.Context, key string) error {
	return s.Update(ctx, func(tx *Tx) error { return tx.Delete(key) })
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
	value, err := json.Marshal(cmd.Result.Results[0]["Value"])
	if err != nil {
		return nil, fmt.Errorf("encode value: %w", err)
	}
	return value, nil
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

// List returns all keys that start with prefix. Matching is case-sensitive.
func (s Store) List(ctx context.Context, prefix string) ([]string, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	startWith := s.idPrefix() + hex.EncodeToString([]byte(strings.TrimSpace(prefix)))
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
			if key, ok := doc["Key"].(string); ok {
				keys = append(keys, key)
			}
		}
		if len(cmd.Result.Results) < listPageSize {
			return keys, nil
		}
		start += len(cmd.Result.Results)
	}
}

// Update runs fn in one cluster-wide transaction. Changes made through tx are
// saved together when fn returns nil, and discarded when it returns an error.
// A concurrent write to a key that fn loaded makes the save fail.
func (s Store) Update(ctx context.Context, fn func(tx *Tx) error) error {
	if err := s.validate(); err != nil {
		return err
	}
	session, err := s.docs.OpenSessionWithOptions(&ravendb.SessionOptions{
		Database:        s.docs.GetDatabase(),
		TransactionMode: ravendb.TransactionMode_ClusterWide,
	})
	if err != nil {
		return err
	}
	defer session.Close()
	if err := fn(&Tx{ctx: ctx, store: s, session: session}); err != nil {
		return err
	}
	return session.SaveChangesContext(ctx)
}

func (s Store) idPrefix() string {
	return "KV/" + s.app + "/" + s.collection + "/"
}

func (s Store) validate() error {
	if s.docs == nil {
		return errors.New("ravenkv document store is nil")
	}
	if s.collection == "" {
		return errors.New("ravenkv collection is required")
	}
	if strings.Contains(s.app, "/") || strings.Contains(s.collection, "/") {
		return errors.New("ravenkv app and collection must not contain \"/\"")
	}
	return nil
}

func (s Store) id(key string) (string, error) {
	if err := s.validate(); err != nil {
		return "", err
	}
	if strings.TrimSpace(key) == "" {
		return "", errors.New("key is required")
	}
	return s.DocumentID(key), nil
}

func (s Store) execute(ctx context.Context, cmd ravendb.RavenCommand) error {
	return s.docs.GetRequestExecutor("").ExecuteCommandContext(ctx, cmd, nil)
}

// Tx reads and writes keys of one Store inside Update.
type Tx struct {
	ctx     context.Context
	store   Store
	session *ravendb.DocumentSession
}

// Get returns the raw JSON value for key, or ErrNotFound.
func (tx *Tx) Get(key string) (json.RawMessage, error) {
	entry, err := tx.load(key)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, ErrNotFound
	}
	return entry.Value, nil
}

// Exists reports whether key exists.
func (tx *Tx) Exists(key string) (bool, error) {
	entry, err := tx.load(key)
	return entry != nil, err
}

// Set inserts or overwrites the value for key.
func (tx *Tx) Set(key string, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal value: %w", err)
	}
	entry, err := tx.load(key)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if entry != nil {
		entry.Value = encoded
		entry.UpdatedAt = now
		return nil
	}
	entry = &KVEntry{Key: strings.TrimSpace(key), Value: encoded, UpdatedAt: now}
	return tx.session.StoreWithID(entry, tx.store.DocumentID(key))
}

// Delete removes key. Deleting a missing key is not an error.
func (tx *Tx) Delete(key string) error {
	entry, err := tx.load(key)
	if err != nil || entry == nil {
		return err
	}
	return tx.session.Delete(entry)
}

func (tx *Tx) load(key string) (*KVEntry, error) {
	id, err := tx.store.id(key)
	if err != nil {
		return nil, err
	}
	var entry *KVEntry
	if err := tx.session.LoadContext(tx.ctx, &entry, id); err != nil {
		return nil, err
	}
	return entry, nil
}

// TxGet returns the value for key decoded into T, or ErrNotFound.
func TxGet[T any](tx *Tx, key string) (T, error) {
	var out T
	raw, err := tx.Get(key)
	if err != nil {
		return out, err
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		var zero T
		return zero, err
	}
	return out, nil
}

// TypedStore wraps Store with typed values.
type TypedStore[T any] struct {
	store Store
}

// NewTypedStore returns a typed store for collection and app.
func NewTypedStore[T any](docs *ravendb.DocumentStore, collection string, app string) TypedStore[T] {
	return TypedStore[T]{store: NewStore(docs, collection, app)}
}

// Store returns the untyped store.
func (s TypedStore[T]) Store() Store {
	return s.store
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

// List returns all keys that start with prefix.
func (s TypedStore[T]) List(ctx context.Context, prefix string) ([]string, error) {
	return s.store.List(ctx, prefix)
}

// Update runs fn in one cluster-wide transaction; see Store.Update.
func (s TypedStore[T]) Update(ctx context.Context, fn func(tx *Tx) error) error {
	return s.store.Update(ctx, fn)
}
