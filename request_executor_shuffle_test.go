package ravendb

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

func TestShuffleURLsReordersCopy(t *testing.T) {
	urls := []string{"http://a", "http://b", "http://c"}
	shuffled := shuffleURLs(urls, 1)

	if reflect.DeepEqual(shuffled, urls) {
		t.Fatalf("shuffleURLs returned original order: %v", shuffled)
	}
	if !reflect.DeepEqual(urls, []string{"http://a", "http://b", "http://c"}) {
		t.Fatalf("shuffleURLs modified input: %v", urls)
	}
	if len(shuffled) != len(urls) {
		t.Fatalf("shuffled URL count = %d, want %d", len(shuffled), len(urls))
	}
	seen := make(map[string]bool, len(shuffled))
	for _, url := range shuffled {
		seen[url] = true
	}
	for _, url := range urls {
		if !seen[url] {
			t.Errorf("shuffled URLs omitted %q: %v", url, shuffled)
		}
	}
}

func TestBootstrapUsesOneShuffledSeedAndLoadsTopology(t *testing.T) {
	topologyBody := []byte(`{}`)
	requests := make(chan string, 2)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.Host
		_, _ = w.Write(topologyBody)
	})
	first := httptest.NewServer(handler)
	defer first.Close()
	second := httptest.NewServer(handler)
	defer second.Close()

	topologyBody, _ = json.Marshal(Topology{
		Etag: 1,
		Nodes: []*ServerNode{
			{URL: first.URL, Database: "db", ClusterTag: "A", ServerRole: ServerNodeRoleMember},
			{URL: second.URL, Database: "db", ClusterTag: "B", ServerRole: ServerNodeRoleMember},
		},
	})
	executor := RequestExecutorCreate([]string{first.URL, second.URL}, "db", nil, nil, NewDocumentConventions())
	defer executor.Close()

	if _, err := executor.firstTopologyUpdateFuture.Get(); err != nil {
		t.Fatalf("initial topology update failed: %v", err)
	}
	select {
	case <-requests:
	case <-time.After(2 * time.Second):
		t.Fatal("bootstrap did not contact a seed URL")
	}
	if got := len(executor.GetTopology().Nodes); got != 2 {
		t.Fatalf("loaded topology has %d nodes, want 2", got)
	}
	command := NewGetDatabaseTopologyCommand()
	firstChoice, err := executor.chooseNodeForRequest(context.Background(), command, &SessionInfo{SessionID: 0})
	if err != nil {
		t.Fatalf("choose first read node: %v", err)
	}
	secondChoice, err := executor.chooseNodeForRequest(context.Background(), command, &SessionInfo{SessionID: 1})
	if err != nil {
		t.Fatalf("choose second read node: %v", err)
	}
	if firstChoice.currentNode.URL == secondChoice.currentNode.URL {
		t.Fatalf("round-robin sessions selected the same node: %s", firstChoice.currentNode.URL)
	}
}
