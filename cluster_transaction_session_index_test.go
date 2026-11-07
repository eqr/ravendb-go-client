package ravendb

import "testing"

func TestNewSessionsCaptureDatabaseClusterTransactionIndex(t *testing.T) {
	store := &DocumentStore{}
	executor := &RequestExecutor{conventions: NewDocumentConventions()}
	store.updateLastClusterTransactionIndex("pq-test-db", 41)

	first := newInMemoryDocumentSessionOperations("pq-test-db", store, executor, "first", TransactionMode_ClusterWide, nil)
	stale := newInMemoryDocumentSessionOperations("PQ-TEST-DB", store, executor, "stale", TransactionMode_ClusterWide, nil)
	for name, session := range map[string]*InMemoryDocumentSessionOperations{"first": first, "stale": stale} {
		if session.sessionInfo.lastClusterTransactionIndex == nil {
			t.Fatalf("%s session did not inherit the store's cluster transaction index", name)
		}
		if got := *session.sessionInfo.lastClusterTransactionIndex; got != 41 {
			t.Fatalf("%s session index = %d, want 41", name, got)
		}
	}

	store.updateLastClusterTransactionIndex("PQ-Test-Db", 43)
	store.updateLastClusterTransactionIndex("pq-test-db", 42)
	if got := *stale.sessionInfo.lastClusterTransactionIndex; got != 41 {
		t.Fatalf("existing session index changed to %d after another commit, want snapshot 41", got)
	}

	fresh := newInMemoryDocumentSessionOperations("pq-test-db", store, executor, "fresh", TransactionMode_ClusterWide, nil)
	if fresh.sessionInfo.lastClusterTransactionIndex == nil {
		t.Fatal("new session did not inherit the updated cluster transaction index")
	}
	if got := *fresh.sessionInfo.lastClusterTransactionIndex; got != 43 {
		t.Fatalf("new session index = %d, want max committed index 43", got)
	}
}
