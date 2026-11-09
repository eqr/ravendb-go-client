package kv_test

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"testing"

	"github.com/eqr/ravendb-go-client/internal/ravtest"
	"github.com/eqr/ravendb-go-client/kv"
)

type settings struct {
	Name    string `json:"name"`
	Enabled bool   `json:"enabled"`
	UserID  int64  `json:"user_id"`
}

func TestStore(t *testing.T) {
	url := ravtest.Start(t, "kvtest")
	docs := ravtest.Open(t, url, "kvtest")
	ctx := context.Background()

	t.Run("typed round trip", func(t *testing.T) {
		store := kv.NewTypedStore[settings](docs, "kv", "guide")
		want := settings{Name: "beta", Enabled: true, UserID: 9007199254740993}
		if err := store.Set(ctx, "feature_beta", want); err != nil {
			t.Fatal(err)
		}
		got, err := store.Get(ctx, "feature_beta")
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("got %+v, want %+v", got, want)
		}
	})

	t.Run("scalar int64 keeps precision", func(t *testing.T) {
		store := kv.NewTypedStore[int64](docs, "kv", "guide")
		if err := store.Set(ctx, "telegram_user", 9007199254740993); err != nil {
			t.Fatal(err)
		}
		got, err := store.Get(ctx, "telegram_user")
		if err != nil {
			t.Fatal(err)
		}
		if got != 9007199254740993 {
			t.Fatalf("got %d", got)
		}
	})

	t.Run("overwrite", func(t *testing.T) {
		store := kv.NewStore(docs, "kv", "guide")
		if err := store.Set(ctx, "counter", 1); err != nil {
			t.Fatal(err)
		}
		if err := store.Set(ctx, "counter", 2); err != nil {
			t.Fatal(err)
		}
		got, err := store.Get(ctx, "counter")
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "2" {
			t.Fatalf("got %s", got)
		}
	})

	t.Run("missing key", func(t *testing.T) {
		store := kv.NewStore(docs, "kv", "guide")
		if _, err := store.Get(ctx, "nope"); !errors.Is(err, kv.ErrNotFound) {
			t.Fatalf("got %v, want ErrNotFound", err)
		}
		exists, err := store.Exists(ctx, "nope")
		if err != nil || exists {
			t.Fatalf("exists=%v err=%v", exists, err)
		}
		if err := store.Delete(ctx, "nope"); err != nil {
			t.Fatalf("delete missing: %v", err)
		}
	})

	t.Run("delete and exists", func(t *testing.T) {
		store := kv.NewStore(docs, "kv", "guide")
		if err := store.Set(ctx, "temp", "x"); err != nil {
			t.Fatal(err)
		}
		if exists, err := store.Exists(ctx, "temp"); err != nil || !exists {
			t.Fatalf("exists=%v err=%v", exists, err)
		}
		if err := store.Delete(ctx, "temp"); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Get(ctx, "temp"); !errors.Is(err, kv.ErrNotFound) {
			t.Fatalf("got %v, want ErrNotFound", err)
		}
	})

	t.Run("list scopes by app and prefix", func(t *testing.T) {
		mapper := kv.NewStore(docs, "mapper_db", "restaurant")
		other := kv.NewStore(docs, "mapper_db", "credentials")
		empty := kv.NewStore(docs, "library_kv", "")
		for _, key := range []string{"backfill_a", "backfill_b", "other"} {
			if err := mapper.Set(ctx, key, key); err != nil {
				t.Fatal(err)
			}
		}
		if err := other.Set(ctx, "backfill_c", 1); err != nil {
			t.Fatal(err)
		}
		if err := empty.Set(ctx, "state", map[string]any{"done": true}); err != nil {
			t.Fatal(err)
		}

		keys, err := mapper.List(ctx, "backfill_")
		if err != nil {
			t.Fatal(err)
		}
		slices.Sort(keys)
		if !slices.Equal(keys, []string{"backfill_a", "backfill_b"}) {
			t.Fatalf("got %v", keys)
		}

		all, err := mapper.List(ctx, "")
		if err != nil {
			t.Fatal(err)
		}
		if len(all) != 3 {
			t.Fatalf("got %v", all)
		}

		emptyKeys, err := empty.List(ctx, "")
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(emptyKeys, []string{"state"}) {
			t.Fatalf("got %v", emptyKeys)
		}
		if id := empty.DocumentID("state"); id != "library_kv//state" {
			t.Fatalf("id %q", id)
		}
	})

	t.Run("list pages past one batch", func(t *testing.T) {
		store := kv.NewStore(docs, "kv", "paging")
		for i := range 1100 {
			if err := store.Set(ctx, "k"+strconv.Itoa(i), i); err != nil {
				t.Fatal(err)
			}
		}
		keys, err := store.List(ctx, "")
		if err != nil {
			t.Fatal(err)
		}
		if len(keys) != 1100 {
			t.Fatalf("got %d keys", len(keys))
		}
	})
}
