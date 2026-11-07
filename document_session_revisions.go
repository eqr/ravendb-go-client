package ravendb

import "context"

// Note: Java's IRevisionsSessionOperations is DocumentSessionRevisions

// TODO: write a unique wrapper type
type RevisionsSessionOperations = DocumentSessionRevisions

// DocumentSessionRevisions represents revisions operations
type DocumentSessionRevisions struct {
	*AdvancedSessionExtensionBase
}

func newDocumentSessionRevisions(session *InMemoryDocumentSessionOperations) *DocumentSessionRevisions {
	return &DocumentSessionRevisions{
		AdvancedSessionExtensionBase: newAdvancedSessionExtensionBase(session),
	}
}

func (r *DocumentSessionRevisions) GetFor(results interface{}, id string) error {
	return r.GetForContext(context.Background(), results, id)
}

func (r *DocumentSessionRevisions) GetForContext(ctx context.Context, results interface{}, id string) error {
	return r.GetForPagedContext(ctx, results, id, 0, 25)
}

func (r *DocumentSessionRevisions) GetForStartAt(results interface{}, id string, start int) error {
	return r.GetForStartAtContext(context.Background(), results, id, start)
}

func (r *DocumentSessionRevisions) GetForStartAtContext(ctx context.Context, results interface{}, id string, start int) error {
	return r.GetForPagedContext(ctx, results, id, start, 25)
}

func (r *DocumentSessionRevisions) GetForPaged(results interface{}, id string, start int, pageSize int) error {
	return r.GetForPagedContext(context.Background(), results, id, start, pageSize)
}

func (r *DocumentSessionRevisions) GetForPagedContext(ctx context.Context, results interface{}, id string, start int, pageSize int) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	operation, err := NewGetRevisionOperationRange(r.session, id, start, pageSize, false)
	if err != nil {
		return err
	}

	command, err := operation.createRequest()
	if err != nil {
		return err
	}
	if err = r.requestExecutor.ExecuteCommandContext(ctx, command, r.sessionInfo); err != nil {
		return err
	}
	operation.setResult(command.Result)
	return operation.GetRevisionsFor(results)
}

func (r *DocumentSessionRevisions) GetMetadataFor(id string) ([]*MetadataAsDictionary, error) {
	return r.GetMetadataForContext(context.Background(), id)
}

func (r *DocumentSessionRevisions) GetMetadataForContext(ctx context.Context, id string) ([]*MetadataAsDictionary, error) {
	return r.GetMetadataForPagedContext(ctx, id, 0, 25)
}

func (r *DocumentSessionRevisions) GetMetadataForStartAt(id string, start int) ([]*MetadataAsDictionary, error) {
	return r.GetMetadataForStartAtContext(context.Background(), id, start)
}

func (r *DocumentSessionRevisions) GetMetadataForStartAtContext(ctx context.Context, id string, start int) ([]*MetadataAsDictionary, error) {
	return r.GetMetadataForPagedContext(ctx, id, start, 25)
}

func (r *DocumentSessionRevisions) GetMetadataForPaged(id string, start int, pageSize int) ([]*MetadataAsDictionary, error) {
	return r.GetMetadataForPagedContext(context.Background(), id, start, pageSize)
}

func (r *DocumentSessionRevisions) GetMetadataForPagedContext(ctx context.Context, id string, start int, pageSize int) ([]*MetadataAsDictionary, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	operation, err := NewGetRevisionOperationRange(r.session, id, start, pageSize, true)
	if err != nil {
		return nil, err
	}
	command, err := operation.createRequest()
	if err != nil {
		return nil, err
	}
	if err = r.requestExecutor.ExecuteCommandContext(ctx, command, r.sessionInfo); err != nil {
		return nil, err
	}
	operation.setResult(command.Result)
	return operation.GetRevisionsMetadataFor(), nil
}

func (r *DocumentSessionRevisions) Get(result interface{}, changeVector string) error {
	return r.GetContext(context.Background(), result, changeVector)
}

func (r *DocumentSessionRevisions) GetContext(ctx context.Context, result interface{}, changeVector string) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	operation := NewGetRevisionOperationWithChangeVectors(r.session, []string{changeVector})
	command, err := operation.createRequest()
	if err != nil {
		return err
	}
	if err = r.requestExecutor.ExecuteCommandContext(ctx, command, r.sessionInfo); err != nil {
		return err
	}
	operation.setResult(command.Result)
	return operation.GetRevision(result)
}

func (r *DocumentSessionRevisions) GetRevisions(results interface{}, changeVectors []string) error {
	return r.GetRevisionsContext(context.Background(), results, changeVectors)
}

func (r *DocumentSessionRevisions) GetRevisionsContext(ctx context.Context, results interface{}, changeVectors []string) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	operation := NewGetRevisionOperationWithChangeVectors(r.session, changeVectors)
	command, err := operation.createRequest()
	if err != nil {
		return err
	}
	if err = r.requestExecutor.ExecuteCommandContext(ctx, command, r.sessionInfo); err != nil {
		return err
	}
	operation.setResult(command.Result)
	return operation.GetRevisions(results)
}
