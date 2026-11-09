package ravendb

import (
	"context"
	"fmt"
	"testing"
	"time"

	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type ctDoc struct {
	ID string `json:"Id,omitempty"`
}

func TestCountersAndTimeSeriesTestcontainers(t *testing.T) {
	ctx := context.Background()

	req := tc.ContainerRequest{
		Image:        "ravendb/ravendb:7.2.7",
		ExposedPorts: []string{"8080/tcp"},
		WaitingFor:   wait.ForHTTP("/").WithPort("8080/tcp").WithStartupTimeout(120 * time.Second),
	}
	container, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start container: %v", err)
	}
	defer container.Terminate(ctx)

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := container.MappedPort(ctx, "8080")
	if err != nil {
		t.Fatal(err)
	}
	url := fmt.Sprintf("http://%s:%s", host, port.Port())

	dbRecord := &DatabaseRecord{DatabaseName: "testdb"}
	storeManager := NewDocumentStore([]string{url}, "")
	if err := storeManager.Initialize(); err != nil {
		t.Fatal(err)
	}
	op := NewCreateDatabaseOperation(dbRecord, 1)
	if err := storeManager.Maintenance().Server().Send(op); err != nil {
		t.Logf("create db: %v", err)
	}
	storeManager.Close()

	store := NewDocumentStore([]string{url}, "testdb")
	if err := store.Initialize(); err != nil {
		t.Fatalf("initialize store: %v", err)
	}
	defer store.Close()

	session, err := store.OpenSession("")
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Store(&ctDoc{ID: "docs/1"}); err != nil {
		t.Fatal(err)
	}
	if err := session.SaveChanges(); err != nil {
		t.Fatalf("save doc: %v", err)
	}
	session.Close()

	session, _ = store.OpenSession("")
	if err := session.CountersFor("docs/1").Increment("produced", 5); err != nil {
		t.Fatal(err)
	}
	if err := session.CountersFor("docs/1").Increment("produced", 2); err != nil {
		t.Fatal(err)
	}
	if err := session.SaveChanges(); err != nil {
		t.Fatalf("save counters: %v", err)
	}
	session.Close()

	session, _ = store.OpenSession("")
	got, err := session.CountersFor("docs/1").Get("produced")
	if err != nil {
		t.Fatal(err)
	}
	if got != 7 {
		t.Fatalf("counter got %d want 7", got)
	}
	session.Close()

	m, err := store.Operations().GetCounters("docs/1")
	if err != nil {
		t.Fatal(err)
	}
	if m["produced"] != 7 {
		t.Fatalf("direct counters %v", m)
	}

	// delete counter
	session, _ = store.OpenSession("")
	if err := session.CountersFor("docs/1").Delete("produced"); err != nil {
		t.Fatal(err)
	}
	if err := session.SaveChanges(); err != nil {
		t.Fatalf("delete counter: %v", err)
	}
	session.Close()
	session, _ = store.OpenSession("")
	got2, err := session.CountersFor("docs/1").Get("produced")
	if err != nil {
		t.Fatal(err)
	}
	if got2 != 0 {
		t.Fatalf("counter after delete got %d want 0", got2)
	}
	session.Close()

	now := time.Now().UTC().Truncate(time.Second)
	session, _ = store.OpenSession("")
	if err := session.TimeSeriesFor("docs/1", "produced").Append(now, 1, 2); err != nil {
		t.Fatal(err)
	}
	if err := session.SaveChanges(); err != nil {
		t.Fatalf("save ts: %v", err)
	}
	session.Close()

	session, _ = store.OpenSession("")
	entries, err := session.TimeSeriesFor("docs/1", "produced").Get(now.Add(-time.Hour), now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || len(entries[0].Values) != 2 {
		t.Fatalf("ts entries %v", entries)
	}
	session.Close()

	dentries, err := store.Operations().GetTimeSeries("docs/1", "produced", now.Add(-time.Hour), now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(dentries) != 1 {
		t.Fatalf("direct ts %v", dentries)
	}

	if err := store.Operations().DeleteTimeSeriesRange("docs/1", "produced", now.Add(-time.Hour), now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
}
