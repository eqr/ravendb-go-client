package ravendb

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"
)

func TestClusterTransactionBatchRequestAdvertisesAtomicGuardSupport(t *testing.T) {
	automaticGuards := false
	originalChangeVector := "RAFT:29-node,TRXN:76-node"
	request := newSyntheticBatchRequest(t, TransactionMode_ClusterWide, &automaticGuards, &originalChangeVector)
	defer closeSyntheticRequestBody(t, request)

	var body struct {
		TransactionMode string `json:"TransactionMode"`
		Commands        []struct {
			ChangeVector         *string `json:"ChangeVector"`
			OriginalChangeVector *string `json:"OriginalChangeVector"`
		} `json:"Commands"`
	}
	encodedBody, err := io.ReadAll(request.Body)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encodedBody, &body); err != nil {
		t.Fatalf("decode synthetic request body %q: %v", encodedBody, err)
	}
	if body.TransactionMode != "ClusterWide" {
		t.Fatalf("JSON TransactionMode = %q, want ClusterWide; body=%s", body.TransactionMode, encodedBody)
	}
	if len(body.Commands) != 1 {
		t.Fatalf("JSON command count = %d, want one; body=%s", len(body.Commands), encodedBody)
	}
	if body.Commands[0].ChangeVector != nil {
		t.Fatalf("ChangeVector = %q, want nil when optimistic concurrency is disabled", *body.Commands[0].ChangeVector)
	}
	if got := body.Commands[0].OriginalChangeVector; got == nil || *got != originalChangeVector {
		t.Fatalf("OriginalChangeVector = %v, want %q", got, originalChangeVector)
	}

	query := request.URL.Query()
	if query.Get("disableAtomicDocumentWrites") != "false" {
		t.Fatalf("disableAtomicDocumentWrites query value = %q, want false; URL=%s", query.Get("disableAtomicDocumentWrites"), request.URL)
	}
	if query.Get("TransactionMode") != "" {
		t.Fatalf("TransactionMode must be in JSON body, not query: %s", request.URL)
	}
	if got := request.Header.Get(headersClientVersion); got != clusterTransactionAtomicGuardClientVersion {
		t.Fatalf("Raven-Client-Version = %q, want %q", got, clusterTransactionAtomicGuardClientVersion)
	}
}

func TestClusterTransactionDefaultAdvertisesAtomicGuardSupport(t *testing.T) {
	request := newSyntheticBatchRequest(t, TransactionMode_ClusterWide, nil, nil)
	defer closeSyntheticRequestBody(t, request)
	if got := request.Header.Get(headersClientVersion); got != clusterTransactionAtomicGuardClientVersion {
		t.Fatalf("Raven-Client-Version = %q, want %q for default cluster transaction guards", got, clusterTransactionAtomicGuardClientVersion)
	}
	if got := request.URL.Query().Get("disableAtomicDocumentWrites"); got != "" {
		t.Fatalf("default disableAtomicDocumentWrites query value = %q, want omitted; URL=%s", got, request.URL)
	}
}

func TestDeleteCommandPreservesOriginalChangeVectorWithoutOCC(t *testing.T) {
	originalChangeVector := "RAFT:29-node,TRXN:76-node"
	command := newDeleteCommandData("pq/test/delete-guard", "", &originalChangeVector)
	serialized, err := command.serialize(NewDocumentConventions())
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(serialized)
	if err != nil {
		t.Fatal(err)
	}
	var body struct {
		ChangeVector         *string `json:"ChangeVector"`
		OriginalChangeVector *string `json:"OriginalChangeVector"`
	}
	if err := json.Unmarshal(encoded, &body); err != nil {
		t.Fatal(err)
	}
	if body.ChangeVector != nil {
		t.Fatalf("ChangeVector = %q, want nil when optimistic concurrency is disabled", *body.ChangeVector)
	}
	if body.OriginalChangeVector == nil || *body.OriginalChangeVector != originalChangeVector {
		t.Fatalf("OriginalChangeVector = %v, want %q", body.OriginalChangeVector, originalChangeVector)
	}

	publicCommand := NewDeleteCommandData("pq/test/public-delete", originalChangeVector)
	publicSerialized, err := publicCommand.serialize(NewDocumentConventions())
	if err != nil {
		t.Fatal(err)
	}
	publicEncoded, err := json.Marshal(publicSerialized)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(publicEncoded, &body); err != nil {
		t.Fatal(err)
	}
	if body.ChangeVector == nil || *body.ChangeVector != originalChangeVector {
		t.Fatalf("public NewDeleteCommandData ChangeVector = %v, want %q", body.ChangeVector, originalChangeVector)
	}
	if body.OriginalChangeVector == nil || *body.OriginalChangeVector != originalChangeVector {
		t.Fatalf("public NewDeleteCommandData OriginalChangeVector = %v, want %q", body.OriginalChangeVector, originalChangeVector)
	}
}

func TestBatchRequestUsesBaseClientVersionWhenAutomaticGuardsAreDisabled(t *testing.T) {
	disabled := true
	automaticGuards := false
	tests := []struct {
		name            string
		transactionMode int
		disableGuards   *bool
	}{
		{name: "explicitly disabled cluster transaction", transactionMode: TransactionMode_ClusterWide, disableGuards: &disabled},
		{name: "single-node transaction", transactionMode: TransactionMode_SingleNode, disableGuards: &automaticGuards},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := newSyntheticBatchRequest(t, test.transactionMode, test.disableGuards, nil)
			defer closeSyntheticRequestBody(t, request)
			if got := request.Header.Get(headersClientVersion); got != goClientVersion {
				t.Fatalf("Raven-Client-Version = %q, want %q", got, goClientVersion)
			}
		})
	}
}

func newSyntheticBatchRequest(t *testing.T, transactionMode int, disableGuards *bool, originalChangeVector *string) *http.Request {
	t.Helper()
	command, err := newBatchCommand(
		NewDocumentConventions(),
		[]ICommandData{
			newPutCommandDataWithJSON("pq/test/atomic-guard", nil, originalChangeVector, map[string]interface{}{"value": "synthetic"}),
		},
		nil,
		transactionMode,
		disableGuards,
	)
	if err != nil {
		t.Fatal(err)
	}
	node := NewServerNode()
	node.URL = "https://raven.invalid"
	node.Database = "synthetic"
	request, err := (&RequestExecutor{}).createRequest(nil, node, command)
	if err != nil {
		t.Fatal(err)
	}
	return request
}

func closeSyntheticRequestBody(t *testing.T, request *http.Request) {
	t.Helper()
	if err := request.Body.Close(); err != nil {
		t.Errorf("close synthetic request body: %v", err)
	}
}
