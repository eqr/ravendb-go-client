package ravenkv_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"testing"

	"github.com/eqr/ravendb-go-client/internal/ravtest"
	"github.com/eqr/ravendb-go-client/ravenkv"
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

	t.Run("document layout", func(t *testing.T) {
		store := ravenkv.NewStore(docs, "kv", "guide")
		if id := store.DocumentID("Ab/c"); id != "KV/guide/kv/41622f63" {
			t.Fatalf("id %q", id)
		}
		if id := ravenkv.NewStore(docs, "library_kv", "").DocumentID("s"); id != "KV//library_kv/73" {
			t.Fatalf("id %q", id)
		}
		if err := store.Set(ctx, "layout", map[string]int{"a": 1}); err != nil {
			t.Fatal(err)
		}
		session, err := docs.OpenSession("")
		if err != nil {
			t.Fatal(err)
		}
		defer session.Close()
		var entry *ravenkv.KVEntry
		if err := session.Load(&entry, store.DocumentID("layout")); err != nil {
			t.Fatal(err)
		}
		if entry == nil || entry.Key != "layout" || string(entry.Value) != `{"a":1}` || entry.UpdatedAt.IsZero() {
			t.Fatalf("entry %+v", entry)
		}
		metadata, err := session.Advanced().GetMetadataFor(entry)
		if err != nil {
			t.Fatal(err)
		}
		if collection, _ := metadata.Get("@collection"); collection != "KVEntries" {
			t.Fatalf("collection %v", collection)
		}
	})

	t.Run("typed round trip", func(t *testing.T) {
		store := ravenkv.NewTypedStore[settings](docs, "kv", "guide")
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
		store := ravenkv.NewTypedStore[int64](docs, "kv", "guide")
		if err := store.Set(ctx, "telegram_user", 9007199254740993); err != nil {
			t.Fatal(err)
		}
		got, err := store.Get(ctx, "telegram_user")
		if err != nil || got != 9007199254740993 {
			t.Fatalf("got %d err %v", got, err)
		}
	})

	t.Run("keys differing by case stay separate", func(t *testing.T) {
		store := ravenkv.NewStore(docs, "kv", "guide")
		if err := store.Set(ctx, "Token", 1); err != nil {
			t.Fatal(err)
		}
		if err := store.Set(ctx, "token", 2); err != nil {
			t.Fatal(err)
		}
		upper, err := store.Get(ctx, "Token")
		if err != nil || string(upper) != "1" {
			t.Fatalf("Token=%s err=%v", upper, err)
		}
		lower, err := store.Get(ctx, "token")
		if err != nil || string(lower) != "2" {
			t.Fatalf("token=%s err=%v", lower, err)
		}
	})

	t.Run("overwrite", func(t *testing.T) {
		store := ravenkv.NewStore(docs, "kv", "guide")
		for _, v := range []int{1, 2} {
			if err := store.Set(ctx, "counter", v); err != nil {
				t.Fatal(err)
			}
		}
		got, err := store.Get(ctx, "counter")
		if err != nil || string(got) != "2" {
			t.Fatalf("got %s err %v", got, err)
		}
	})

	t.Run("missing key", func(t *testing.T) {
		store := ravenkv.NewStore(docs, "kv", "guide")
		if _, err := store.Get(ctx, "nope"); !errors.Is(err, ravenkv.ErrNotFound) {
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

	t.Run("delete then recreate", func(t *testing.T) {
		store := ravenkv.NewStore(docs, "kv", "guide")
		if err := store.Set(ctx, "temp", "x"); err != nil {
			t.Fatal(err)
		}
		if exists, err := store.Exists(ctx, "temp"); err != nil || !exists {
			t.Fatalf("exists=%v err=%v", exists, err)
		}
		if err := store.Delete(ctx, "temp"); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Get(ctx, "temp"); !errors.Is(err, ravenkv.ErrNotFound) {
			t.Fatalf("got %v, want ErrNotFound", err)
		}
		if err := store.Set(ctx, "temp", "y"); err != nil {
			t.Fatalf("recreate: %v", err)
		}
	})

	t.Run("list scopes by app collection and prefix", func(t *testing.T) {
		restaurant := ravenkv.NewStore(docs, "mapper_db", "restaurant")
		credentials := ravenkv.NewStore(docs, "mapper_db", "credentials")
		library := ravenkv.NewStore(docs, "library_kv", "")
		for _, key := range []string{"backfill_a", "backfill_b", "Backfill_c", "other"} {
			if err := restaurant.Set(ctx, key, key); err != nil {
				t.Fatal(err)
			}
		}
		if err := credentials.Set(ctx, "backfill_x", 1); err != nil {
			t.Fatal(err)
		}
		if err := library.Set(ctx, "state", map[string]any{"done": true}); err != nil {
			t.Fatal(err)
		}

		keys, err := restaurant.List(ctx, "backfill_")
		if err != nil {
			t.Fatal(err)
		}
		slices.Sort(keys)
		if !slices.Equal(keys, []string{"backfill_a", "backfill_b"}) {
			t.Fatalf("got %v", keys)
		}
		all, err := restaurant.List(ctx, "")
		if err != nil || len(all) != 4 {
			t.Fatalf("got %v err %v", all, err)
		}
		libraryKeys, err := library.List(ctx, "")
		if err != nil || !slices.Equal(libraryKeys, []string{"state"}) {
			t.Fatalf("got %v err %v", libraryKeys, err)
		}
	})

	t.Run("list pages past one batch", func(t *testing.T) {
		store := ravenkv.NewStore(docs, "kv", "paging")
		for i := range 1100 {
			if err := store.Set(ctx, "k"+strconv.Itoa(i), i); err != nil {
				t.Fatal(err)
			}
		}
		keys, err := store.List(ctx, "")
		if err != nil || len(keys) != 1100 {
			t.Fatalf("got %d keys err %v", len(keys), err)
		}
	})

	t.Run("update is atomic across keys", func(t *testing.T) {
		store := ravenkv.NewStore(docs, "kv", "guide")
		if err := store.Set(ctx, "code:123", map[string]string{"user": "u1"}); err != nil {
			t.Fatal(err)
		}
		err := store.Update(ctx, func(tx *ravenkv.Tx) error {
			code, err := ravenkv.TxGet[map[string]string](tx, "code:123")
			if err != nil {
				return err
			}
			if err := tx.Set("user:"+code["user"], int64(42)); err != nil {
				return err
			}
			return tx.Delete("code:123")
		})
		if err != nil {
			t.Fatal(err)
		}
		if exists, _ := store.Exists(ctx, "code:123"); exists {
			t.Fatal("code still exists")
		}
		got, err := store.Get(ctx, "user:u1")
		if err != nil || string(got) != "42" {
			t.Fatalf("got %s err %v", got, err)
		}
	})

	t.Run("failed update writes nothing", func(t *testing.T) {
		store := ravenkv.NewStore(docs, "kv", "guide")
		boom := errors.New("boom")
		err := store.Update(ctx, func(tx *ravenkv.Tx) error {
			if err := tx.Set("half", 1); err != nil {
				return err
			}
			return boom
		})
		if !errors.Is(err, boom) {
			t.Fatalf("got %v", err)
		}
		if exists, _ := store.Exists(ctx, "half"); exists {
			t.Fatal("partial write saved")
		}
	})

	t.Run("concurrent update loses", func(t *testing.T) {
		store := ravenkv.NewStore(docs, "kv", "guide")
		if err := store.Set(ctx, "claim", "free"); err != nil {
			t.Fatal(err)
		}
		err := store.Update(ctx, func(tx *ravenkv.Tx) error {
			if _, err := tx.Get("claim"); err != nil {
				return err
			}
			if err := store.Set(ctx, "claim", "winner"); err != nil {
				t.Fatalf("winner write: %v", err)
			}
			return tx.Set("claim", "loser")
		})
		if err == nil {
			t.Fatal("stale update saved")
		}
		got, err := store.Get(ctx, "claim")
		if err != nil {
			t.Fatal(err)
		}
		var value string
		if err := json.Unmarshal(got, &value); err != nil || value != "winner" {
			t.Fatalf("got %s", got)
		}
	})

	t.Run("rejects slash in app or collection", func(t *testing.T) {
		if err := ravenkv.NewStore(docs, "kv", "a/b").Set(ctx, "k", 1); err == nil {
			t.Fatal("want error")
		}
	})
}
