package ravendb

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type contextTestCommand struct {
	RavenCommandBase
	path          string
	method        string
	response      []byte
	responseError error
}

func newContextTestCommand(method, path string) *contextTestCommand {
	return &contextTestCommand{
		RavenCommandBase: NewRavenCommandBase(),
		method:           method,
		path:             path,
	}
}

func (c *contextTestCommand) CreateRequest(node *ServerNode) (*http.Request, error) {
	var body io.Reader
	if c.method == http.MethodPost {
		body = strings.NewReader("payload")
	}
	return http.NewRequest(c.method, node.URL+c.path, body)
}

func (c *contextTestCommand) SetResponse(response []byte, _ bool) error {
	if c.responseError != nil {
		return c.responseError
	}
	c.response = append(c.response[:0], response...)
	return nil
}

type responseBodyTracker struct {
	io.ReadCloser
	closed     chan struct{}
	closeOnce  sync.Once
	closeCount int32
}

func (b *responseBodyTracker) Close() error {
	atomic.AddInt32(&b.closeCount, 1)
	b.closeOnce.Do(func() { close(b.closed) })
	return b.ReadCloser.Close()
}

type responseTrackingRoundTripper struct {
	base   http.RoundTripper
	bodies chan *responseBodyTracker
}

func (rt *responseTrackingRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := rt.base.RoundTrip(request)
	if response != nil && response.Body != nil {
		body := &responseBodyTracker{
			ReadCloser: response.Body,
			closed:     make(chan struct{}),
		}
		response.Body = body
		rt.bodies <- body
	}
	return response, err
}

func trackResponseBodies(executor *RequestExecutor, server *httptest.Server) <-chan *responseBodyTracker {
	client := *server.Client()
	tracker := &responseTrackingRoundTripper{
		base:   client.Transport,
		bodies: make(chan *responseBodyTracker, 8),
	}
	client.Transport = tracker
	executor.httpClient = &client
	return tracker.bodies
}

func requireBodyClosed(t *testing.T, body *responseBodyTracker) {
	t.Helper()
	select {
	case <-body.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("response body was not closed")
	}
	if got := atomic.LoadInt32(&body.closeCount); got != 1 {
		t.Fatalf("response body was closed %d times, want exactly once", got)
	}
}

type preferredResponseTransport struct {
	preferredHost    string
	preferredEntered chan struct{}
	releasePreferred chan struct{}
	body             *responseBodyTracker
}

func (rt *preferredResponseTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.URL.Host == rt.preferredHost {
		rt.preferredEntered <- struct{}{}
		<-rt.releasePreferred
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       rt.body,
			Request:    request,
		}, nil
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(`{}`)),
		Request:    request,
	}, nil
}

type contextTestOperation struct {
	command RavenCommand
}

func (o contextTestOperation) GetCommand(_ *DocumentStore, _ *DocumentConventions, _ *httpCache) (RavenCommand, error) {
	return o.command, nil
}

type contextTestMaintenanceOperation struct {
	command RavenCommand
}

func (o contextTestMaintenanceOperation) GetCommand(_ *DocumentConventions) (RavenCommand, error) {
	return o.command, nil
}

func newContextTestExecutor(t *testing.T, servers ...*httptest.Server) *RequestExecutor {
	t.Helper()
	urls := make([]string, len(servers))
	nodes := make([]*ServerNode, len(servers))
	for i, server := range servers {
		urls[i] = server.URL
		nodes[i] = &ServerNode{URL: server.URL, Database: "test", ClusterTag: string(rune('A' + i)), ServerRole: ServerNodeRoleMember}
	}
	executor := NewRequestExecutor("test", nil, nil, nil, urls)
	executor.httpClient = servers[0].Client()
	executor.setNodeSelector(NewNodeSelector(&Topology{Nodes: nodes, Etag: 1}))
	executor.disableTopologyUpdates = true
	executor.disableClientConfigurationUpdates = true
	t.Cleanup(executor.Close)
	return executor
}

type contextTestEntity struct {
	Name string `json:"Name"`
}

func newContextTestStore(t *testing.T, server *httptest.Server) (*DocumentStore, *DocumentSession) {
	t.Helper()
	conventions := NewDocumentConventions()
	conventions.SetDisableTopologyUpdates(true)
	store := NewDocumentStore([]string{server.URL}, "test")
	store.SetConventions(conventions)
	if err := store.Initialize(); err != nil {
		t.Fatal(err)
	}
	store.GetRequestExecutor("").httpClient = server.Client()
	session, err := store.OpenSession("")
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	return store, session
}

func TestDocumentSessionLoadContextCancelsRequest(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)
	_, session := newContextTestStore(t, server)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var result *contextTestEntity
	done := make(chan error, 1)
	go func() { done <- session.LoadContext(ctx, &result, "docs/1") }()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("LoadContext did not dispatch request")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("LoadContext did not stop after cancellation")
	}
}

func TestDocumentSessionSaveChangesContextCancelsRequest(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)
	_, session := newContextTestStore(t, server)
	if err := session.StoreWithID(&contextTestEntity{Name: "sample"}, "docs/1"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- session.SaveChangesContext(ctx) }()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("SaveChangesContext did not dispatch request")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SaveChangesContext did not stop after cancellation")
	}
}

func TestDocumentQueryContextCancelsRequest(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)
	_, session := newContextTestStore(t, server)
	query := session.QueryCollection("ContextTestEntity")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var results []*contextTestEntity
	done := make(chan error, 1)
	go func() { done <- query.GetResultsContext(ctx, &results) }()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("query did not dispatch request")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("query did not stop after cancellation")
	}
}
func TestMaintenanceSendContextCancelsRequest(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)
	store, _ := newContextTestStore(t, server)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	operation := contextTestMaintenanceOperation{command: newContextTestCommand(http.MethodGet, "/maintenance")}
	done := make(chan error, 1)
	go func() { done <- store.Maintenance().SendContext(ctx, operation) }()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("maintenance operation did not dispatch request")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("maintenance operation did not stop after cancellation")
	}
}

func TestCompareExchangeContextCancelsRequest(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)
	store, _ := newContextTestStore(t, server)
	session, err := store.OpenSessionWithOptions(&SessionOptions{TransactionMode: TransactionMode_ClusterWide})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := session.Advanced().ClusterTransaction().GetCompareExchangeValueContext(ctx, reflect.TypeOf(int(0)), "counter")
		done <- err
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("compare-exchange operation did not dispatch request")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("compare-exchange operation did not stop after cancellation")
	}
}

func TestRequestExecutorExecuteContextPreCanceled(t *testing.T) {
	var requests int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&requests, 1)
		_, _ = io.WriteString(w, `{}`)
	}))
	defer server.Close()
	executor := newContextTestExecutor(t, server)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := executor.ExecuteCommandContext(ctx, newContextTestCommand(http.MethodGet, "/"), nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if got := atomic.LoadInt32(&requests); got != 0 {
		t.Fatalf("pre-canceled context dispatched %d requests", got)
	}
}

func TestRequestExecutorExecuteContextCancelsBlockedRequest(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)
	executor := newContextTestExecutor(t, server)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- executor.ExecuteCommandContext(ctx, newContextTestCommand(http.MethodGet, "/"), nil) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not receive request")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("request did not stop after cancellation")
	}
}

func TestRequestExecutorExecuteContextCancelsBlockedBodyRead(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		_, _ = io.WriteString(w, `{"partial":`)
		w.(http.Flusher).Flush()
		entered <- struct{}{}
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)
	executor := newContextTestExecutor(t, server)
	bodies := trackResponseBodies(executor, server)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- executor.ExecuteCommandContext(ctx, newContextTestCommand(http.MethodGet, "/"), nil) }()
	body := <-bodies
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not begin streaming response")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("body read did not stop after cancellation")
	}
	requireBodyClosed(t, body)
}

func TestExecuteContextClosesAutomaticBodyAfterSuccessfulProcessing(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{}`)
	}))
	defer server.Close()
	executor := newContextTestExecutor(t, server)
	bodies := trackResponseBodies(executor, server)
	command := newContextTestCommand(http.MethodGet, "/")

	if err := executor.ExecuteCommandContext(context.Background(), command, nil); err != nil {
		t.Fatal(err)
	}
	body := <-bodies
	requireBodyClosed(t, body)
	if string(command.response) != `{}` {
		t.Fatalf("response body was not processed: %q", command.response)
	}
}

func TestExecuteContextClosesAutomaticBodyAfterProcessingError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{}`)
	}))
	defer server.Close()
	executor := newContextTestExecutor(t, server)
	bodies := trackResponseBodies(executor, server)
	expectedErr := errors.New("reject response")
	command := newContextTestCommand(http.MethodGet, "/")
	command.responseError = expectedErr

	if err := executor.ExecuteCommandContext(context.Background(), command, nil); !errors.Is(err, expectedErr) {
		t.Fatalf("expected response processing error, got %v", err)
	}
	requireBodyClosed(t, <-bodies)
}

func TestExecuteContextHandsOffStreamingBodyOnlyAfterSuccess(t *testing.T) {
	t.Run("success preserves caller ownership", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, "stream-data")
		}))
		defer server.Close()
		executor := newContextTestExecutor(t, server)
		bodies := trackResponseBodies(executor, server)
		command := NewStreamCommand("stream")

		if err := executor.ExecuteCommandContext(context.Background(), command, nil); err != nil {
			t.Fatal(err)
		}
		body := <-bodies
		select {
		case <-body.closed:
			t.Fatal("successful stream handoff closed the caller-owned body")
		default:
		}
		if err := command.Result.Response.Body.Close(); err != nil {
			t.Fatal(err)
		}
		requireBodyClosed(t, body)
	})

	t.Run("canceled refresh closes unreturned stream", func(t *testing.T) {
		topologyEntered := make(chan struct{}, 1)
		releaseTopology := make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasSuffix(r.URL.Path, "/topology") {
				topologyEntered <- struct{}{}
				<-releaseTopology
				_, _ = io.WriteString(w, `{}`)
				return
			}
			w.Header().Set(headersRefreshTopology, "true")
			_, _ = io.WriteString(w, "stream-data")
		}))
		defer server.Close()
		defer close(releaseTopology)
		executor := newContextTestExecutor(t, server)
		bodies := trackResponseBodies(executor, server)
		command := NewStreamCommand("stream")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- executor.ExecuteCommandContext(ctx, command, nil) }()
		body := <-bodies
		select {
		case <-topologyEntered:
		case <-time.After(2 * time.Second):
			t.Fatal("topology refresh did not begin")
		}
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("expected context.Canceled, got %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("stream operation did not stop after cancellation")
		}
		requireBodyClosed(t, body)
	})
}

func TestExecuteOnAllClosesPreferredResponseAfterCancellation(t *testing.T) {
	preferred := httptest.NewServer(http.NotFoundHandler())
	defer preferred.Close()
	other := httptest.NewServer(http.NotFoundHandler())
	defer other.Close()
	executor := newContextTestExecutor(t, preferred, other)
	body := &responseBodyTracker{
		ReadCloser: io.NopCloser(strings.NewReader("late response")),
		closed:     make(chan struct{}),
	}
	transport := &preferredResponseTransport{
		preferredHost:    preferred.Listener.Addr().String(),
		preferredEntered: make(chan struct{}, 1),
		releasePreferred: make(chan struct{}),
		body:             body,
	}
	executor.httpClient = &http.Client{Transport: transport}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	chosenNode := executor.getNodeSelector().getTopology().Nodes[0]
	done := make(chan error, 1)
	go func() {
		_, err := executor.executeOnAllToFigureOutTheFastest(ctx, chosenNode, newContextTestCommand(http.MethodGet, "/"))
		done <- err
	}()
	select {
	case <-transport.preferredEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("preferred request did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("fanout did not return after cancellation")
	}
	close(transport.releasePreferred)
	requireBodyClosed(t, body)
}

func TestCompletableFutureCancellationDoesNotCancelOtherWaiters(t *testing.T) {
	future := newCompletableFuture()
	waiting := make(chan int, 3)
	canceledResult := make(chan error, 1)
	healthyResults := make(chan interface{}, 2)
	canceledCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		waiting <- 0
		_, err := future.GetContext(canceledCtx)
		canceledResult <- err
	}()
	for i := 0; i < 2; i++ {
		go func() {
			waiting <- 1
			result, err := future.GetContext(context.Background())
			if err != nil {
				healthyResults <- err
				return
			}
			healthyResults <- result
		}()
	}
	for i := 0; i < 3; i++ {
		<-waiting
	}
	cancel()
	select {
	case err := <-canceledResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled waiter returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled waiter did not return")
	}
	future.complete("ready")
	for i := 0; i < 2; i++ {
		select {
		case result := <-healthyResults:
			if result != "ready" {
				t.Fatalf("healthy waiter returned %#v, want ready", result)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("future completion did not wake a healthy waiter")
		}
	}
}

func TestRequestExecutorExecuteContextHonorsDeadline(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)
	executor := newContextTestExecutor(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := executor.ExecuteCommandContext(ctx, newContextTestCommand(http.MethodGet, "/"), nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	select {
	case <-entered:
	default:
		t.Fatal("deadline test did not dispatch request")
	}
}

func TestCanceledWriteIsNotRetriedOnAnotherNode(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	first := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer first.Close()
	defer close(release)
	var secondRequests int32
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&secondRequests, 1)
		_, _ = io.WriteString(w, `{}`)
	}))
	defer second.Close()
	executor := newContextTestExecutor(t, first, second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- executor.ExecuteCommandContext(ctx, newContextTestCommand(http.MethodPost, "/write"), nil)
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first node did not receive write")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("write did not stop after cancellation")
	}
	if got := atomic.LoadInt32(&secondRequests); got != 0 {
		t.Fatalf("canceled write was retried on another node %d times", got)
	}
}

func TestCancellationDuringRetryHandlingDoesNotFailOver(t *testing.T) {
	bodyRead := make(chan struct{}, 1)
	release := make(chan struct{})
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.(http.Flusher).Flush()
		_, _ = io.WriteString(w, `{"Error":`)
		w.(http.Flusher).Flush()
		bodyRead <- struct{}{}
		<-release
	}))
	defer first.Close()
	defer close(release)
	var secondRequests int32
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&secondRequests, 1)
		_, _ = io.WriteString(w, `{}`)
	}))
	defer second.Close()
	executor := newContextTestExecutor(t, first, second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- executor.ExecuteCommandContext(ctx, newContextTestCommand(http.MethodPost, "/write"), nil)
	}()
	select {
	case <-bodyRead:
	case <-time.After(2 * time.Second):
		t.Fatal("retry handling did not begin reading the error body")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("retry handling did not stop after cancellation")
	}
	if got := atomic.LoadInt32(&secondRequests); got != 0 {
		t.Fatalf("canceled operation failed over to another node %d times", got)
	}
}

func TestCancelingOneOperationLeavesSharedStoreUsable(t *testing.T) {
	blockedEntered := make(chan struct{}, 1)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/blocked" {
			blockedEntered <- struct{}{}
			select {
			case <-r.Context().Done():
			case <-release:
			}
			return
		}
		_, _ = io.WriteString(w, `{}`)
	}))
	defer server.Close()
	defer close(release)

	conventions := NewDocumentConventions()
	conventions.SetDisableTopologyUpdates(true)
	store := NewDocumentStore([]string{server.URL}, "test")
	store.SetConventions(conventions)
	if err := store.Initialize(); err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	store.GetRequestExecutor("").httpClient = server.Client()

	ctx, cancel := context.WithCancel(context.Background())
	blockedDone := make(chan error, 1)
	go func() {
		op := contextTestOperation{command: newContextTestCommand(http.MethodGet, "/blocked")}
		blockedDone <- store.Operations().SendContext(ctx, op, nil)
	}()
	select {
	case <-blockedEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked operation was not dispatched")
	}
	cancel()
	if err := <-blockedDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled operation, got %v", err)
	}
	op := contextTestOperation{command: newContextTestCommand(http.MethodGet, "/ok")}
	if err := store.Operations().SendContext(context.Background(), op, nil); err != nil {
		t.Fatalf("unrelated operation failed after cancellation: %v", err)
	}
}

func TestTopologyWaitCanBeCanceledWithoutCancelingSharedFuture(t *testing.T) {
	executor := NewRequestExecutor("test", nil, nil, nil, []string{"http://127.0.0.1:1"})
	defer executor.Close()
	future := newCompletableFuture()
	executor.firstTopologyUpdateFuture = future
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := executor.ExecuteCommandContext(ctx, newContextTestCommand(http.MethodGet, "/"), nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	if executor.firstTopologyUpdateFuture != future || future.IsDone() {
		t.Fatal("canceling one caller changed the shared topology update")
	}
}

func TestPublicContextEntryPointsReturnPreCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	session := &DocumentSession{}
	if err := session.LoadContext(ctx, nil, "id"); !errors.Is(err, context.Canceled) {
		t.Fatalf("LoadContext returned %v", err)
	}
	if err := session.SaveChangesContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("SaveChangesContext returned %v", err)
	}
	if _, err := session.ExistsContext(ctx, "id"); !errors.Is(err, context.Canceled) {
		t.Fatalf("ExistsContext returned %v", err)
	}

	query := &abstractDocumentQuery{}
	if err := query.GetResultsContext(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetResultsContext returned %v", err)
	}
	if _, err := query.CountContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("CountContext returned %v", err)
	}
	if err := (&OperationExecutor{}).SendContext(ctx, nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("OperationExecutor.SendContext returned %v", err)
	}
	if err := (&MaintenanceOperationExecutor{}).SendContext(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("MaintenanceOperationExecutor.SendContext returned %v", err)
	}
	if err := (&ServerOperationExecutor{}).SendContext(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("ServerOperationExecutor.SendContext returned %v", err)
	}
	if err := (&Operation{}).WaitForCompletionContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Operation.WaitForCompletionContext returned %v", err)
	}
	attachments := &DocumentSessionAttachments{}
	if _, err := attachments.ExistsContext(ctx, "id", "name"); !errors.Is(err, context.Canceled) {
		t.Fatalf("DocumentSessionAttachments.ExistsContext returned %v", err)
	}
	revisions := &DocumentSessionRevisions{}
	if err := revisions.GetForPagedContext(ctx, nil, "id", 0, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("DocumentSessionRevisions.GetForPagedContext returned %v", err)
	}
	if _, err := (&SuggestionDocumentQuery{}).ExecuteContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("SuggestionDocumentQuery.ExecuteContext returned %v", err)
	}
	clusterTransaction := &ClusterTransactionOperations{}
	if _, err := clusterTransaction.GetCompareExchangeValueContext(ctx, nil, "key"); !errors.Is(err, context.Canceled) {
		t.Fatalf("ClusterTransactionOperations.GetCompareExchangeValueContext returned %v", err)
	}
}
