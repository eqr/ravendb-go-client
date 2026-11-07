package ravendb

import "context"

type ServerOperationExecutor struct {
	requestExecutor *ClusterRequestExecutor
}

func NewServerOperationExecutor(store *DocumentStore) *ServerOperationExecutor {
	res := &ServerOperationExecutor{}
	urls := store.GetUrls()
	cert := store.Certificate
	trustStore := store.TrustStore
	conv := store.GetConventions()
	if conv.IsDisableTopologyUpdates() {
		res.requestExecutor = ClusterRequestExecutorCreateForSingleNode(urls[0], cert, trustStore, conv)
	} else {
		res.requestExecutor = ClusterRequestExecutorCreate(urls, cert, trustStore, conv)
	}
	fn := func(store *DocumentStore) {
		res.requestExecutor.Close()
	}
	store.AddAfterCloseListener(fn)
	return res
}

func (e *ServerOperationExecutor) Send(operation IServerOperation) error {
	return e.SendContext(context.Background(), operation)
}

func (e *ServerOperationExecutor) SendContext(ctx context.Context, operation IServerOperation) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	command, err := operation.GetCommand(e.requestExecutor.GetConventions())
	if err != nil {
		return err
	}
	return e.requestExecutor.ExecuteCommandContext(ctx, command, nil)
}

func (e *ServerOperationExecutor) SendAsync(operation IServerOperation) (*Operation, error) {
	return e.SendAsyncContext(context.Background(), operation)
}

func (e *ServerOperationExecutor) SendAsyncContext(ctx context.Context, operation IServerOperation) (*Operation, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	requestExecutor := e.requestExecutor
	command, err := operation.GetCommand(requestExecutor.GetConventions())
	if err != nil {
		return nil, err
	}
	if err = requestExecutor.ExecuteCommandContext(ctx, command, nil); err != nil {
		return nil, err
	}
	result := getCommandOperationIDResult(command)
	return NewServerWideOperation(requestExecutor, requestExecutor.GetConventions(), result.OperationID), nil
}

func (e *ServerOperationExecutor) Close() {
	e.requestExecutor.Close()
	e.requestExecutor = nil
}
