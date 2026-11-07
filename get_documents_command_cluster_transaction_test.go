package ravendb

import "testing"

func TestClusterWideGetDocumentsCommandAdvertisesTransactionMode(t *testing.T) {
	command, err := NewGetDocumentsCommand([]string{"docs/1"}, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	command.setTransactionMode(TransactionMode_ClusterWide)
	node := NewServerNode()
	node.URL = "https://raven.invalid"
	node.Database = "synthetic"
	request, err := command.CreateRequest(node)
	if err != nil {
		t.Fatal(err)
	}
	if got := request.URL.Query().Get("txMode"); got != "ClusterWide" {
		t.Fatalf("txMode query value = %q, want ClusterWide; URL=%s", got, request.URL)
	}
}

func TestLoadOperationTracksAutomaticGuardForNewDocument(t *testing.T) {
	store := &DocumentStore{}
	executor := &RequestExecutor{conventions: NewDocumentConventions()}
	session := newInMemoryDocumentSessionOperations("pq-test-db", store, executor, "test", TransactionMode_ClusterWide, nil)
	operation := NewLoadOperation(session)
	const documentID = "docs/1"
	const guardChangeVector = "A:1-guard"
	result := &GetDocumentsResult{
		Results: []map[string]interface{}{},
		CompareExchangeValueIncludes: map[string]interface{}{
			"rvn-atomic/docs/1": map[string]interface{}{
				"Key":          "rvn-atomic/docs/1",
				"Index":        int64(106),
				"Value":        map[string]interface{}{"Object": map[string]interface{}{"Id": documentID}},
				"ChangeVector": guardChangeVector,
			},
		},
	}
	if err := operation.setResult(result); err != nil {
		t.Fatal(err)
	}

	entity := &struct{ Value string }{Value: "recreated"}
	session.storeEntityInUnitOfWork(documentID, entity, nil, map[string]interface{}{}, ConcurrencyCheckAuto)
	documentInfo := getDocumentInfoByEntity(session.documentsByEntity, entity)
	if documentInfo == nil || documentInfo.changeVector == nil {
		t.Fatal("new document did not inherit the automatic guard change vector")
	}
	if got := *documentInfo.changeVector; got != guardChangeVector {
		t.Fatalf("new document change vector = %q, want automatic guard vector %q", got, guardChangeVector)
	}
}
