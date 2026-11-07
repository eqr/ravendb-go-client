package ravendb

// PutCommandDataWithJSON represents data for put command with json
type PutCommandDataWithJSON struct {
	*CommandData
	document             map[string]interface{}
	originalChangeVector *string
}

var _ ICommandData = &PutCommandDataWithJSON{} // verify interface match

// newPutCommandDataWithJSON returns new PutCommandDataWithJSON
func newPutCommandDataWithJSON(id string, changeVector *string, originalChangeVector *string, document map[string]interface{}) *PutCommandDataWithJSON {
	panicIf(document == nil, "Document cannot be nil")

	res := &PutCommandDataWithJSON{
		CommandData: &CommandData{
			Type:         CommandPut,
			ID:           id,
			ChangeVector: changeVector,
		},
		document:             document,
		originalChangeVector: originalChangeVector,
	}
	return res
}

func (d *PutCommandDataWithJSON) serialize(conventions *DocumentConventions) (interface{}, error) {
	js := d.baseJSON()
	js["Document"] = d.document
	if d.originalChangeVector != nil {
		js["OriginalChangeVector"] = *d.originalChangeVector
	}
	return js, nil
}
