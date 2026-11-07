package ravendb

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestQueryHashIncludesParametersAfterNilStructAndTime(t *testing.T) {
	base := time.Date(2026, time.November, 7, 12, 30, 45, 0, time.UTC)
	cases := []struct {
		name  string
		first interface{}
	}{
		{name: "nil", first: nil},
		{name: "struct", first: struct{ Value string }{Value: "same"}},
		{name: "time", first: base},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			first := queryHashWithParameters(Parameters{"p0": tc.first, "p1": int64(1)})
			second := queryHashWithParameters(Parameters{"p0": tc.first, "p1": int64(2)})
			if first == second {
				t.Fatalf("query hashes ignore later parameter for %s: %s", tc.name, first)
			}
		})
	}
}

func TestQueryHashDistinguishesSubsecondTimesAndMapOrder(t *testing.T) {
	base := time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC)
	next := base.Add(time.Nanosecond)
	first := queryHashWithParameters(Parameters{"p0": base})
	second := queryHashWithParameters(Parameters{"p0": next})
	if first == second {
		t.Fatalf("subsecond query times share hash %s", first)
	}
	nestedFirst := queryHashWithParameters(Parameters{"p0": []interface{}{base}})
	nestedSecond := queryHashWithParameters(Parameters{"p0": []interface{}{next}})
	if nestedFirst == nestedSecond {
		t.Fatalf("nested subsecond query times share hash %s", nestedFirst)
	}

	left := queryHashWithParameters(Parameters{"p0": int64(1), "p1": base, "p2": "cursor"})
	right := queryHashWithParameters(Parameters{"p2": "cursor", "p1": base, "p0": int64(1)})
	if left != right {
		t.Fatalf("parameter insertion order changed hash: %s != %s", left, right)
	}
}

func TestQueryCacheDoesNotReuseResponseForChangedCursor(t *testing.T) {
	type queryRequest struct {
		Query           string                 `json:"Query"`
		QueryParameters map[string]interface{} `json:"QueryParameters"`
	}
	type observedRequest struct {
		hash        string
		cursor      int64
		ifNoneMatch string
	}
	var observed []observedRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request queryRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		cursor, ok := request.QueryParameters["p1"].(float64)
		if !ok {
			http.Error(w, "missing cursor parameter", http.StatusBadRequest)
			return
		}
		observed = append(observed, observedRequest{
			hash:        r.URL.Query().Get("queryHash"),
			cursor:      int64(cursor),
			ifNoneMatch: r.Header.Get("If-None-Match"),
		})
		if r.Header.Get("If-None-Match") != "" {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", fmt.Sprintf("\"cursor-%d\"", int64(cursor)))
		_, _ = fmt.Fprintf(w, `{"Results":[{"Cursor":%d}],"IndexName":"Test/ByCursor","IsStale":false}`, int64(cursor))
	}))
	defer server.Close()

	executor := NewRequestExecutor("test", nil, nil, nil, []string{server.URL})
	executor.httpClient = server.Client()
	executor.disableTopologyUpdates = true
	executor.disableClientConfigurationUpdates = true
	defer executor.Close()
	node := &ServerNode{URL: server.URL, Database: "test", ClusterTag: "A", ServerRole: ServerNodeRoleMember}

	for _, cursor := range []int64{1, 2} {
		query := NewIndexQuery("from index 'Test/ByCursor' where ChangedAt < $p0 and Id > $p1")
		query.queryParameters = Parameters{
			"p0": time.Date(2026, time.November, 7, 12, 30, 45, 123, time.UTC),
			"p1": cursor,
		}
		command, err := NewQueryCommand(NewDocumentConventions(), query, false, false)
		if err != nil {
			t.Fatal(err)
		}
		if err := executor.ExecuteContext(context.Background(), node, -1, command, false, nil); err != nil {
			t.Fatal(err)
		}
		got := command.Result.Results[0]["Cursor"]
		if got != float64(cursor) {
			t.Fatalf("cursor %d query returned cached result %v", cursor, got)
		}
	}

	if len(observed) != 2 {
		t.Fatalf("server observed %d requests, want 2", len(observed))
	}
	if observed[0].hash == observed[1].hash {
		t.Fatalf("different cursor bodies shared queryHash %q", observed[0].hash)
	}
	if observed[0].cursor != 1 || observed[1].cursor != 2 {
		t.Fatalf("server request cursors = %d, %d; want 1, 2", observed[0].cursor, observed[1].cursor)
	}
	if observed[1].ifNoneMatch != "" {
		t.Fatalf("second cursor request reused cached ETag %q", observed[1].ifNoneMatch)
	}
}

func queryHashWithParameters(parameters Parameters) string {
	query := NewIndexQuery("from index 'Test/ByCursor' where Value = $p0")
	query.queryParameters = parameters
	return query.GetQueryHash()
}
