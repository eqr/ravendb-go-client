package ravendb

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"time"
)

type TimeSeriesEntry struct {
	Timestamp time.Time `json:"Timestamp"`
	Tag       string    `json:"Tag,omitempty"`
	Values    []float64 `json:"Values"`
}

type TimeSeriesOperation struct {
	Type  string           `json:"Type"`
	Entry *TimeSeriesEntry `json:"Entry,omitempty"`
	From  time.Time        `json:"From,omitempty"`
	To    time.Time        `json:"To,omitempty"`
}

type TimeSeriesCommandData struct {
	*CommandData
	Name       string                `json:"Name"`
	Appends    []*TimeSeriesEntry    `json:"-"`
	Deletes    [][2]time.Time        `json:"-"`
	Operations []TimeSeriesOperation `json:"-"`
}

var _ ICommandData = &TimeSeriesCommandData{}

func NewTimeSeriesCommandData(documentID, seriesName string, operations []TimeSeriesOperation, changeVector *string) *TimeSeriesCommandData {
	res := &TimeSeriesCommandData{
		CommandData: &CommandData{
			Type:         CommandTimeSeries,
			ID:           documentID,
			ChangeVector: changeVector,
		},
		Name:       seriesName,
		Operations: operations,
	}
	return res
}

func (d *TimeSeriesCommandData) serialize(conventions *DocumentConventions) (interface{}, error) {
	res := d.baseJSON()
	res["Id"] = d.ID
	res["Type"] = "TimeSeries"
	var appends []*TimeSeriesEntry
	var deletes []map[string]interface{}
	for _, op := range d.Operations {
		switch op.Type {
		case "Append":
			if op.Entry != nil {
				appends = append(appends, op.Entry)
			}
		case "Delete":
			deletes = append(deletes, map[string]interface{}{
				"From": op.From,
				"To":   op.To,
			})
		}
	}
	res["TimeSeries"] = map[string]interface{}{
		"DocumentId": d.ID,
		"Name":       d.Name,
		"Appends":    appends,
		"Deletes":    deletes,
	}
	return res, nil
}

type TimeSeriesSessionOperations struct {
	session    *InMemoryDocumentSessionOperations
	documentID string
	seriesName string
}

func (s *DocumentSession) TimeSeriesFor(documentID, seriesName string) *TimeSeriesSessionOperations {
	return &TimeSeriesSessionOperations{
		session:    s.InMemoryDocumentSessionOperations,
		documentID: documentID,
		seriesName: seriesName,
	}
}

func (ts *TimeSeriesSessionOperations) Append(timestamp time.Time, values ...float64) error {
	return ts.AppendContext(context.Background(), timestamp, values...)
}

func (ts *TimeSeriesSessionOperations) AppendContext(ctx context.Context, timestamp time.Time, values ...float64) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	entry := &TimeSeriesEntry{Timestamp: timestamp, Values: values}
	op := TimeSeriesOperation{Type: "Append", Entry: entry}
	return ts.appendOperation(op)
}

func (ts *TimeSeriesSessionOperations) AppendTagged(timestamp time.Time, tag string, values ...float64) error {
	return ts.AppendTaggedContext(context.Background(), timestamp, tag, values...)
}

func (ts *TimeSeriesSessionOperations) AppendTaggedContext(ctx context.Context, timestamp time.Time, tag string, values ...float64) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	entry := &TimeSeriesEntry{Timestamp: timestamp, Tag: tag, Values: values}
	op := TimeSeriesOperation{Type: "Append", Entry: entry}
	return ts.appendOperation(op)
}

func (ts *TimeSeriesSessionOperations) AppendBulk(entries []*TimeSeriesEntry) error {
	return ts.AppendBulkContext(context.Background(), entries)
}

func (ts *TimeSeriesSessionOperations) AppendBulkContext(ctx context.Context, entries []*TimeSeriesEntry) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	for _, e := range entries {
		op := TimeSeriesOperation{Type: "Append", Entry: e}
		if err := ts.appendOperation(op); err != nil {
			return err
		}
	}
	return nil
}

func (ts *TimeSeriesSessionOperations) appendOperation(op TimeSeriesOperation) error {
	key := newIDTypeAndName(ts.documentID, CommandTimeSeries, ts.seriesName)
	if existing, ok := ts.session.deferredCommandsMap[key]; ok {
		if cc, ok := existing.(*TimeSeriesCommandData); ok {
			cc.Operations = append(cc.Operations, op)
			return nil
		}
	}
	cv := ts.getChangeVector()
	cc := NewTimeSeriesCommandData(ts.documentID, ts.seriesName, []TimeSeriesOperation{op}, cv)
	ts.session.Defer(cc)
	return nil
}

func (ts *TimeSeriesSessionOperations) getChangeVector() *string {
	for _, di := range ts.session.documentsByEntity {
		if di != nil && di.id == ts.documentID {
			cv := di.changeVector
			if cv != nil && *cv != "" {
				return cv
			}
		}
	}
	return nil
}

func (ts *TimeSeriesSessionOperations) Get(from, to time.Time) ([]*TimeSeriesEntry, error) {
	return ts.GetContext(context.Background(), from, to)
}

func (ts *TimeSeriesSessionOperations) GetContext(ctx context.Context, from, to time.Time) ([]*TimeSeriesEntry, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	cmd := NewGetTimeSeriesCommand(ts.documentID, ts.seriesName, from, to)
	if err := ts.session.requestExecutor.ExecuteCommandContext(ctx, cmd, ts.session.sessionInfo); err != nil {
		return nil, err
	}
	return cmd.Result.Entries, nil
}

func (ts *TimeSeriesSessionOperations) Delete(from, to time.Time) error {
	return ts.DeleteContext(context.Background(), from, to)
}

func (ts *TimeSeriesSessionOperations) DeleteContext(ctx context.Context, from, to time.Time) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	op := TimeSeriesOperation{Type: "Delete", From: from, To: to}
	return ts.appendOperation(op)
}

type TimeSeriesPolicy struct {
	Name        string
	Pattern     string
	Aggregation interface{}
	Retention   time.Duration
}

type GetTimeSeriesResult struct {
	Entries []*TimeSeriesEntry `json:"Entries"`
}

type GetTimeSeriesCommand struct {
	RavenCommandBase
	documentID string
	seriesName string
	from       time.Time
	to         time.Time
	Result     *GetTimeSeriesResult
}

func NewGetTimeSeriesCommand(documentID, seriesName string, from, to time.Time) *GetTimeSeriesCommand {
	cmd := &GetTimeSeriesCommand{documentID: documentID, seriesName: seriesName, from: from, to: to}
	cmd.RavenCommandBase.ResponseType = RavenCommandResponseTypeObject
	cmd.Result = &GetTimeSeriesResult{}
	return cmd
}
func (c *GetTimeSeriesCommand) CreateRequest(node *ServerNode) (*http.Request, error) {
	urlStr := node.URL + "/databases/" + node.Database + "/timeseries?docId=" + url.QueryEscape(c.documentID) +
		"&name=" + url.QueryEscape(c.seriesName) +
		"&from=" + url.QueryEscape(c.from.UTC().Format(time.RFC3339Nano)) +
		"&to=" + url.QueryEscape(c.to.UTC().Format(time.RFC3339Nano))
	return newHttpGet(urlStr)
}

func (c *GetTimeSeriesCommand) SetResponse(response []byte, fromCache bool) error {
	if len(response) == 0 {
		return nil
	}
	return json.Unmarshal(response, c.Result)
}

var _ = http.MethodGet
var _ = url.Values{}
