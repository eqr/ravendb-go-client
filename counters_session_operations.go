package ravendb

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
)

type CounterOperation struct {
	Type        string `json:"Type"`
	CounterName string `json:"CounterName"`
	Delta       int64  `json:"Delta,omitempty"`
}

type CountersCommandData struct {
	*CommandData
	Operations []CounterOperation `json:"-"`
}

var _ ICommandData = &CountersCommandData{}

func NewCountersCommandData(documentID string, operations []CounterOperation, changeVector *string) *CountersCommandData {
	res := &CountersCommandData{
		CommandData: &CommandData{
			Type:         CommandCounters,
			ID:           documentID,
			ChangeVector: changeVector,
		},
		Operations: operations,
	}
	return res
}

func (d *CountersCommandData) serialize(conventions *DocumentConventions) (interface{}, error) {
	res := d.baseJSON()
	delete(res, "Id")
	res["Type"] = "Counters"
	opList := make([]map[string]interface{}, 0, len(d.Operations))
	for _, op := range d.Operations {
		m := map[string]interface{}{
			"Type":        op.Type,
			"CounterName": op.CounterName,
		}
		if op.Type == "Increment" {
			m["Delta"] = op.Delta
		}
		opList = append(opList, m)
	}
	res["Counters"] = map[string]interface{}{
		"DocumentId": d.ID,
		"Operations": opList,
	}
	return res, nil
}

type CountersSessionOperations struct {
	session    *InMemoryDocumentSessionOperations
	documentID string
}

func (s *DocumentSession) CountersFor(documentID string) *CountersSessionOperations {
	return &CountersSessionOperations{
		session:    s.InMemoryDocumentSessionOperations,
		documentID: documentID,
	}
}

func (c *CountersSessionOperations) Increment(counterName string, delta int64) error {
	return c.IncrementContext(context.Background(), counterName, delta)
}

func (c *CountersSessionOperations) IncrementContext(ctx context.Context, counterName string, delta int64) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if counterName == "" {
		return newIllegalArgumentError("counterName cannot be null or empty")
	}
	op := CounterOperation{Type: "Increment", CounterName: counterName, Delta: delta}
	return c.appendOperation(op)
}

func (c *CountersSessionOperations) Delete(counterName string) error {
	return c.DeleteContext(context.Background(), counterName)
}

func (c *CountersSessionOperations) DeleteContext(ctx context.Context, counterName string) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if counterName == "" {
		return newIllegalArgumentError("counterName cannot be null or empty")
	}
	op := CounterOperation{Type: "Delete", CounterName: counterName}
	return c.appendOperation(op)
}

func (c *CountersSessionOperations) appendOperation(op CounterOperation) error {
	key := newIDTypeAndName(c.documentID, CommandCounters, "")
	if existing, ok := c.session.deferredCommandsMap[key]; ok {
		if cc, ok := existing.(*CountersCommandData); ok {
			cc.Operations = append(cc.Operations, op)
			return nil
		}
	}
	cv := c.getChangeVector()
	cc := NewCountersCommandData(c.documentID, []CounterOperation{op}, cv)
	c.session.Defer(cc)
	return nil
}

func (c *CountersSessionOperations) getChangeVector() *string {
	// best effort: look up tracked document
	for _, di := range c.session.documentsByEntity {
		if di != nil && di.id == c.documentID {
			cv := di.changeVector
			if cv != nil && *cv != "" {
				return cv
			}
		}
	}
	return nil
}

func (c *CountersSessionOperations) Get(counterName string) (int64, error) {
	return c.GetContext(context.Background(), counterName)
}

func (c *CountersSessionOperations) GetContext(ctx context.Context, counterName string) (int64, error) {
	if err := validateContext(ctx); err != nil {
		return 0, err
	}
	cmd := NewGetCountersCommand(c.documentID, counterName)
	if err := c.session.requestExecutor.ExecuteCommandContext(ctx, cmd, c.session.sessionInfo); err != nil {
		return 0, err
	}
	for _, cr := range cmd.Result.Counters {
		if cr.CounterName == counterName {
			return cr.TotalValue, nil
		}
	}
	return 0, nil
}

func (c *CountersSessionOperations) GetAll() (map[string]int64, error) {
	return c.GetAllContext(context.Background())
}

func (c *CountersSessionOperations) GetAllContext(ctx context.Context) (map[string]int64, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	cmd := NewGetCountersCommand(c.documentID, "")
	if err := c.session.requestExecutor.ExecuteCommandContext(ctx, cmd, c.session.sessionInfo); err != nil {
		return nil, err
	}
	res := map[string]int64{}
	for _, cr := range cmd.Result.Counters {
		res[cr.CounterName] = cr.TotalValue
	}
	return res, nil
}

type CounterValue struct {
	CounterName string `json:"CounterName"`
	TotalValue  int64  `json:"TotalValue"`
}

type GetCountersResult struct {
	Counters []CounterValue `json:"Counters"`
}

type GetCountersCommand struct {
	RavenCommandBase
	documentID string
	counter    string
	Result     *GetCountersResult
}

func NewGetCountersCommand(documentID, counter string) *GetCountersCommand {
	cmd := &GetCountersCommand{documentID: documentID, counter: counter}
	cmd.RavenCommandBase.ResponseType = RavenCommandResponseTypeObject
	cmd.Result = &GetCountersResult{}
	return cmd
}

func (c *GetCountersCommand) CreateRequest(node *ServerNode) (*http.Request, error) {
	urlStr := node.URL + "/databases/" + node.Database + "/counters?docId=" + url.QueryEscape(c.documentID)
	if c.counter != "" {
		urlStr += "&counter=" + url.QueryEscape(c.counter)
	}
	return newHttpGet(urlStr)
}

func (c *GetCountersCommand) SetResponse(response []byte, fromCache bool) error {
	if len(response) == 0 {
		return nil
	}
	return json.Unmarshal(response, c.Result)
}
