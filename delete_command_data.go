package ravendb

// DeleteCommandData represents data for delete command
type DeleteCommandData struct {
	CommandData
	originalChangeVector *string
}

// NewDeleteCommandData creates ICommandData for Delete command.
func NewDeleteCommandData(id string, changeVector string) ICommandData {
	var originalChangeVector *string
	if changeVector != "" {
		originalChangeVector = &changeVector
	}
	return newDeleteCommandData(id, changeVector, originalChangeVector)
}

func newDeleteCommandData(id string, changeVector string, originalChangeVector *string) *DeleteCommandData {
	var changeVectorPtr *string
	if changeVector != "" {
		changeVectorPtr = &changeVector
	}
	return &DeleteCommandData{
		CommandData: CommandData{
			Type:         CommandDelete,
			ID:           id,
			ChangeVector: changeVectorPtr,
		},
		originalChangeVector: originalChangeVector,
	}
}

func (d *DeleteCommandData) serialize(conventions *DocumentConventions) (interface{}, error) {
	js := d.baseJSON()
	if d.originalChangeVector != nil {
		js["OriginalChangeVector"] = *d.originalChangeVector
	}
	return js, nil
}
