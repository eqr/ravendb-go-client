package ravtest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/eqr/ravendb-go-client"
	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Start runs a RavenDB container, creates database, and returns the server URL.
func Start(t *testing.T, database string) string {
	t.Helper()
	ctx := context.Background()
	container, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{
		ContainerRequest: tc.ContainerRequest{
			Image:        "ravendb/ravendb:7.2.7",
			ExposedPorts: []string{"8080/tcp"},
			Env: map[string]string{
				"RAVEN_Setup_Mode":                      "None",
				"RAVEN_License_Eula_Accepted":           "true",
				"RAVEN_Security_UnsecuredAccessAllowed": "PublicNetwork",
			},
			WaitingFor: wait.ForHTTP("/").WithPort("8080/tcp").WithStartupTimeout(120 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start ravendb container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := container.MappedPort(ctx, "8080")
	if err != nil {
		t.Fatal(err)
	}
	url := fmt.Sprintf("http://%s:%s", host, port.Port())

	admin := ravendb.NewDocumentStore([]string{url}, "")
	if err := admin.Initialize(); err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	op := ravendb.NewCreateDatabaseOperation(&ravendb.DatabaseRecord{DatabaseName: database}, 1)
	if err := admin.Maintenance().Server().Send(op); err != nil {
		t.Fatalf("create database: %v", err)
	}
	return url
}

// Open returns an initialized document store for database.
func Open(t *testing.T, url, database string) *ravendb.DocumentStore {
	t.Helper()
	docs := ravendb.NewDocumentStore([]string{url}, database)
	if err := docs.Initialize(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(docs.Close)
	return docs
}
