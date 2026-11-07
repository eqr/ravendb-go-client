package ravendb

import "context"

// Note: Java's IAttachmentsSessionOperations is DocumentSessionAttachments

// TODO: make a unique wrapper type
type AttachmentsSessionOperations = DocumentSessionAttachments

type DocumentSessionAttachments struct {
	*DocumentSessionAttachmentsBase
}

func NewDocumentSessionAttachments(session *InMemoryDocumentSessionOperations) *DocumentSessionAttachments {
	res := &DocumentSessionAttachments{}
	res.DocumentSessionAttachmentsBase = NewDocumentSessionAttachmentsBase(session)
	return res
}

func (s *DocumentSessionAttachments) Exists(documentID string, name string) (bool, error) {
	return s.ExistsContext(context.Background(), documentID, name)
}

func (s *DocumentSessionAttachments) ExistsContext(ctx context.Context, documentID string, name string) (bool, error) {
	if err := validateContext(ctx); err != nil {
		return false, err
	}
	command, err := NewHeadAttachmentCommand(documentID, name, nil)
	if err != nil {
		return false, err
	}
	if err = s.requestExecutor.ExecuteCommandContext(ctx, command, s.sessionInfo); err != nil {
		return false, err
	}
	return command.Result != "", nil
}

func (s *DocumentSessionAttachments) GetByID(documentID string, name string) (*AttachmentResult, error) {
	return s.GetByIDContext(context.Background(), documentID, name)
}

func (s *DocumentSessionAttachments) GetByIDContext(ctx context.Context, documentID string, name string) (*AttachmentResult, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	operation := NewGetAttachmentOperation(documentID, name, AttachmentDocument, "", nil)
	if err := s.session.GetOperations().SendContext(ctx, operation, s.sessionInfo); err != nil {
		return nil, err
	}
	return operation.Command.Result, nil
}

func (s *DocumentSessionAttachments) Get(entity interface{}, name string) (*AttachmentResult, error) {
	return s.GetContext(context.Background(), entity, name)
}

func (s *DocumentSessionAttachments) GetContext(ctx context.Context, entity interface{}, name string) (*AttachmentResult, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	document := getDocumentInfoByEntity(s.documents, entity)
	if document == nil {
		return nil, throwEntityNotInSession(entity)
	}
	return s.GetByIDContext(ctx, document.id, name)
}

func (s *DocumentSessionAttachments) GetRevision(documentID string, name string, changeVector *string) (*AttachmentResult, error) {
	return s.GetRevisionContext(context.Background(), documentID, name, changeVector)
}

func (s *DocumentSessionAttachments) GetRevisionContext(ctx context.Context, documentID string, name string, changeVector *string) (*AttachmentResult, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	operation := NewGetAttachmentOperation(documentID, name, AttachmentRevision, "", changeVector)
	if err := s.session.GetOperations().SendContext(ctx, operation, s.sessionInfo); err != nil {
		return nil, err
	}
	return operation.Command.Result, nil
}
