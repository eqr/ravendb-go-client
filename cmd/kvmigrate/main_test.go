package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/eqr/ravendb-go-client/internal/ravtest"
	"github.com/eqr/ravendb-go-client/ravenkv"
)

type fakeItem struct {
	ID      string          `json:"id"`
	Key     string          `json:"key"`
	AppName string          `json:"appname,omitempty"`
	Value   json.RawMessage `json:"value"`
}

func fakePocketBase(t *testing.T, collections map[string][]fakeItem) *httptest.Server {
	t.Helper()
	return fakePocketBaseFunc(t, func(name string, _ int64) ([]fakeItem, bool) {
		items, ok := collections[name]
		return items, ok
	})
}

// fakePocketBaseFunc serves collections from load, which also gets the number of list requests so far.
func fakePocketBaseFunc(t *testing.T, load func(name string, request int64) ([]fakeItem, bool)) *httptest.Server {
	t.Helper()
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/collections/users/auth-with-password" {
			var body struct{ Identity, Password string }
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body.Identity != "admin@example.com" || body.Password != "secret" {
				http.Error(w, `{"message":"bad credentials"}`, http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"token": "tok"})
			return
		}
		if r.Header.Get("Authorization") != "Bearer tok" {
			http.Error(w, `{"message":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		name := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/collections/"), "/records")
		items, ok := load(name, requests.Add(1))
		if !ok {
			http.NotFound(w, r)
			return
		}
		if filter := r.URL.Query().Get("filter"); filter != "" {
			app, err := strconv.Unquote(strings.TrimPrefix(filter, "appname = "))
			if err != nil {
				http.Error(w, `{"message":"bad filter"}`, http.StatusBadRequest)
				return
			}
			var kept []fakeItem
			for _, item := range items {
				if item.AppName == app {
					kept = append(kept, item)
				}
			}
			items = kept
		}
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		perPage, _ := strconv.Atoi(r.URL.Query().Get("perPage"))
		start := min((page-1)*perPage, len(items))
		end := min(start+perPage, len(items))
		for i := range items {
			if items[i].ID == "" {
				items[i].ID = "id" + strconv.Itoa(100000+i)
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"page": page, "perPage": perPage, "totalItems": len(items), "items": items[start:end]})
	}))
	t.Cleanup(server.Close)
	return server
}

func credentialFiles(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	identity := filepath.Join(dir, "identity")
	password := filepath.Join(dir, "password")
	if err := os.WriteFile(identity, []byte("admin@example.com\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(password, []byte("secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return identity, password
}

func TestDryRunReportsCounts(t *testing.T) {
	items := make([]fakeItem, 0, 1200)
	for i := range 1200 {
		items = append(items, fakeItem{Key: "k" + strconv.Itoa(i), AppName: "guide", Value: json.RawMessage(`1`)})
	}
	pb := fakePocketBase(t, map[string][]fakeItem{
		"kv":         items,
		"library_kv": {{Key: "state", Value: json.RawMessage(`{"done":true}`)}},
	})
	identity, password := credentialFiles(t)

	var out bytes.Buffer
	err := run(context.Background(), []string{
		"-mode", "dry-run", "-source-url", pb.URL,
		"-source-identity-file", identity, "-source-password-file", password,
		"-scopes", "kv:guide,library_kv:",
	}, &out)
	if err != nil {
		t.Fatal(err)
	}
	var rep report
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatal(err)
	}
	if rep.Counts["kv"]["guide"] != 1200 || rep.Counts["library_kv"][""] != 1 {
		t.Fatalf("counts %+v", rep.Counts)
	}
}

func TestDryRunFailsOnAppNameCaseCollision(t *testing.T) {
	pb := fakePocketBase(t, map[string][]fakeItem{
		"kv": {
			{Key: "token", AppName: "Guide", Value: json.RawMessage(`1`)},
			{Key: "token", AppName: "guide", Value: json.RawMessage(`2`)},
			{Key: "Token", AppName: "guide", Value: json.RawMessage(`3`)},
		},
	})
	identity, password := credentialFiles(t)
	err := run(context.Background(), []string{
		"-mode", "dry-run", "-source-url", pb.URL,
		"-source-identity-file", identity, "-source-password-file", password,
		"-scopes", "kv:Guide,kv:guide",
	}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "collisions") {
		t.Fatalf("got %v, want collision error", err)
	}
}

func TestRejectsUnprotectedCredentialFile(t *testing.T) {
	identity, password := credentialFiles(t)
	if err := os.Chmod(password, 0o644); err != nil {
		t.Fatal(err)
	}
	err := run(context.Background(), []string{
		"-mode", "dry-run", "-source-url", "http://unused",
		"-source-identity-file", identity, "-source-password-file", password,
		"-scopes", "kv:guide",
	}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "readable only by its owner") {
		t.Fatalf("got %v", err)
	}
}

func TestImportAndVerify(t *testing.T) {
	pb := fakePocketBase(t, map[string][]fakeItem{
		"kv": {
			{Key: "global_settings", AppName: "guide", Value: json.RawMessage(`{"model":"x","limits":[1,2]}`)},
			{Key: "tg_user", AppName: "guide", Value: json.RawMessage(`9007199254740993`)},
			{Key: "session", AppName: "telegram", Value: json.RawMessage(`"not in scope"`)},
		},
		"mapper_db": {
			{Key: "google_token", AppName: "credentials", Value: json.RawMessage(`"abc"`)},
			{Key: "backfill", AppName: "restaurant", Value: json.RawMessage(`{"offset":10}`)},
		},
		"library_kv": {{Key: "artifact_backfill", Value: json.RawMessage(`{"done":false}`)}},
	})
	identity, password := credentialFiles(t)
	url := ravtest.Start(t, "kvmigrate")
	args := func(mode string) []string {
		return []string{
			"-mode", mode, "-source-url", pb.URL,
			"-source-identity-file", identity, "-source-password-file", password,
			"-scopes", "kv:guide,mapper_db:credentials,mapper_db:restaurant,library_kv:",
			"-confirm-source-writers-stopped",
			"-ravendb-urls", url, "-ravendb-database", "kvmigrate",
		}
	}
	ctx := context.Background()

	for range 2 {
		var out bytes.Buffer
		if err := run(ctx, args("import"), &out); err != nil {
			t.Fatal(err)
		}
		var rep report
		if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
			t.Fatal(err)
		}
		if rep.Written != 5 {
			t.Fatalf("written %d", rep.Written)
		}
	}

	var out bytes.Buffer
	if err := run(ctx, args("verify"), &out); err != nil {
		t.Fatalf("verify: %v\n%s", err, out.String())
	}

	docs := ravtest.Open(t, url, "kvmigrate")
	if exists, err := ravenkv.NewStore(docs, "kv", "telegram").Exists(ctx, "session"); err != nil || exists {
		t.Fatalf("telegram row imported outside scope: exists=%v err=%v", exists, err)
	}
	userID, err := ravenkv.NewTypedStore[int64](docs, "kv", "guide").Get(ctx, "tg_user")
	if err != nil || userID != 9007199254740993 {
		t.Fatalf("tg_user=%d err=%v", userID, err)
	}

	if err := ravenkv.NewStore(docs, "mapper_db", "restaurant").Set(ctx, "stray", 1); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	err = run(ctx, args("verify"), &out)
	if err == nil || !strings.Contains(out.String(), ravenkv.NewStore(nil, "mapper_db", "restaurant").DocumentID("stray")) {
		t.Fatalf("verify should report extra key: err=%v out=%s", err, out.String())
	}
}

func TestImportRequiresWriterFence(t *testing.T) {
	identity, password := credentialFiles(t)
	err := run(context.Background(), []string{
		"-mode", "import", "-source-url", "http://unused",
		"-source-identity-file", identity, "-source-password-file", password,
		"-scopes", "kv:guide", "-ravendb-urls", "http://unused", "-ravendb-database", "kv-shared",
	}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "-confirm-source-writers-stopped") {
		t.Fatalf("got %v", err)
	}
}

func TestFailsWhenSourceChangesBetweenReads(t *testing.T) {
	pb := fakePocketBaseFunc(t, func(name string, request int64) ([]fakeItem, bool) {
		value := json.RawMessage(`1`)
		if request > 1 {
			value = json.RawMessage(`2`)
		}
		return []fakeItem{{ID: "a1", Key: "k", AppName: "guide", Value: value}}, name == "kv"
	})
	identity, password := credentialFiles(t)
	err := run(context.Background(), []string{
		"-mode", "dry-run", "-source-url", pb.URL,
		"-source-identity-file", identity, "-source-password-file", password,
		"-scopes", "kv:guide",
	}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "source changed") || !strings.Contains(err.Error(), `changed kv/a1 key "k" fields value`) {
		t.Fatalf("got %v", err)
	}
	if strings.Contains(err.Error(), "2") {
		t.Fatalf("error leaks a value: %v", err)
	}
}

func TestEmptyAppScopeRejectsNamedAppRows(t *testing.T) {
	pb := fakePocketBase(t, map[string][]fakeItem{
		"kv": {{Key: "k", AppName: "guide", Value: json.RawMessage(`1`)}},
	})
	identity, password := credentialFiles(t)
	err := run(context.Background(), []string{
		"-mode", "dry-run", "-source-url", pb.URL,
		"-source-identity-file", identity, "-source-password-file", password,
		"-scopes", "kv:",
	}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), `belongs to appname "guide"`) {
		t.Fatalf("got %v", err)
	}
}

func TestParseScopes(t *testing.T) {
	scopes, err := parseScopes("kv:guide, library_kv:")
	if err != nil || len(scopes) != 2 || scopes[1] != (scope{Collection: "library_kv"}) {
		t.Fatalf("scopes %+v err %v", scopes, err)
	}
	for _, bad := range []string{"kv", ":guide", "kv:a/b", "kv:guide,kv:guide", `kv:"x"`} {
		if _, err := parseScopes(bad); err == nil {
			t.Fatalf("%q: want error", bad)
		}
	}
}
