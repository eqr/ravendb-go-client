package ravenlease_test

import (
	"context"
	"testing"
	"time"

	"github.com/eqr/ravendb-go-client/internal/ravtest"
	"github.com/eqr/ravendb-go-client/ravenlease"
)

type recording struct {
	Address string `json:"address"`
	File    string `json:"file"`
}

func TestCompareExchangeStore(t *testing.T) {
	url := ravtest.Start(t, "leasetest")
	docs := ravtest.Open(t, url, "leasetest")
	ctx := context.Background()
	store := ravenlease.NewCompareExchangeStore[recording](docs, "test/leader")

	lease, index, err := store.Get(ctx)
	if err != nil || lease != nil || index != 0 {
		t.Fatalf("missing lease: %+v %d %v", lease, index, err)
	}

	first := ravenlease.Lease[recording]{Holder: "a", Data: recording{Address: "a:1", File: "one.mp3"}}
	index, ok, err := store.Put(ctx, first, 0)
	if err != nil || !ok || index <= 0 {
		t.Fatalf("create: %d %v %v", index, ok, err)
	}
	if _, ok, err := store.Put(ctx, ravenlease.Lease[recording]{Holder: "b"}, 0); err != nil || ok {
		t.Fatalf("create over existing: %v %v", ok, err)
	}

	lease, got, err := store.Get(ctx)
	if err != nil || lease == nil || *lease != first || got != index {
		t.Fatalf("read back: %+v %d %v", lease, got, err)
	}

	if ok, err := store.Delete(ctx, index-1); err != nil || ok {
		t.Fatalf("delete with stale index: %v %v", ok, err)
	}
	if ok, err := store.Delete(ctx, index); err != nil || !ok {
		t.Fatalf("delete: %v %v", ok, err)
	}
	if lease, _, err := store.Get(ctx); err != nil || lease != nil {
		t.Fatalf("after delete: %+v %v", lease, err)
	}
}

func TestManagersShareCompareExchangeLease(t *testing.T) {
	url := ravtest.Start(t, "leasemanagers")
	docs := ravtest.Open(t, url, "leasemanagers")
	store := ravenlease.NewCompareExchangeStore[recording](docs, "test/leader")

	a := ravenlease.NewManager[recording](store, "a", ravenlease.Options{})
	a.SetData(recording{Address: "a:1", File: "live.mp3"})
	aCtx, aCancel := context.WithCancel(context.Background())
	aDone := make(chan struct{})
	go func() { defer close(aDone); a.Run(aCtx) }()
	waitLeader(t, a)

	b := ravenlease.NewManager[recording](store, "b", ravenlease.Options{})
	bCtx, bCancel := context.WithCancel(context.Background())
	bDone := make(chan struct{})
	go func() { defer close(bDone); b.Run(bCtx) }()
	defer func() { bCancel(); <-bDone }()

	if lease, ok := waitLease(t, b); !ok || lease.Holder != "a" || lease.Data.File != "live.mp3" {
		t.Fatalf("follower view: %+v %v", lease, ok)
	}
	if b.IsLeader() {
		t.Fatal("second manager took an active lease")
	}

	aCancel()
	<-aDone
	if err := a.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitLeader(t, b)
}

func waitLeader(t *testing.T, m *ravenlease.Manager[recording]) {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for !m.IsLeader() {
		select {
		case <-m.Changes():
		case <-time.After(100 * time.Millisecond):
		case <-deadline:
			t.Fatal("manager did not become leader")
		}
	}
}

func waitLease(t *testing.T, m *ravenlease.Manager[recording]) (ravenlease.Lease[recording], bool) {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		if lease, ok := m.CurrentLease(); ok {
			return lease, true
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-deadline:
			return ravenlease.Lease[recording]{}, false
		}
	}
}
