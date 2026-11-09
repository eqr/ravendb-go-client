package ravendb

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"time"
)

func (o *OperationExecutor) GetCounters(documentID string) (map[string]int64, error) {
	return o.GetCountersContext(context.Background(), documentID)
}

func (o *OperationExecutor) GetCountersContext(ctx context.Context, documentID string) (map[string]int64, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	cmd := NewGetCountersCommand(documentID, "")
	if err := o.requestExecutor.ExecuteCommandContext(ctx, cmd, nil); err != nil {
		return nil, err
	}
	res := map[string]int64{}
	for _, c := range cmd.Result.Counters {
		res[c.CounterName] = c.TotalValue
	}
	return res, nil
}

func (o *OperationExecutor) GetTimeSeries(documentID, seriesName string, from, to time.Time) ([]*TimeSeriesEntry, error) {
	return o.GetTimeSeriesContext(context.Background(), documentID, seriesName, from, to)
}

func (o *OperationExecutor) GetTimeSeriesContext(ctx context.Context, documentID, seriesName string, from, to time.Time) ([]*TimeSeriesEntry, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	cmd := NewGetTimeSeriesCommand(documentID, seriesName, from, to)
	if err := o.requestExecutor.ExecuteCommandContext(ctx, cmd, nil); err != nil {
		return nil, err
	}
	return cmd.Result.Entries, nil
}

func (o *OperationExecutor) DeleteTimeSeriesRange(documentID, seriesName string, from, to time.Time) error {
	return o.DeleteTimeSeriesRangeContext(context.Background(), documentID, seriesName, from, to)
}

func (o *OperationExecutor) DeleteTimeSeriesRangeContext(ctx context.Context, documentID, seriesName string, from, to time.Time) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	cmd := NewDeleteTimeSeriesRangeCommand(documentID, seriesName, from, to)
	if err := o.requestExecutor.ExecuteCommandContext(ctx, cmd, nil); err != nil {
		return err
	}
	return nil
}

type DeleteTimeSeriesRangeCommand struct {
	RavenCommandBase
	documentID string
	seriesName string
	from       time.Time
	to         time.Time
}

func NewDeleteTimeSeriesRangeCommand(documentID, seriesName string, from, to time.Time) *DeleteTimeSeriesRangeCommand {
	cmd := &DeleteTimeSeriesRangeCommand{documentID: documentID, seriesName: seriesName, from: from, to: to}
	cmd.RavenCommandBase.ResponseType = RavenCommandResponseTypeEmpty
	return cmd
}

func (c *DeleteTimeSeriesRangeCommand) CreateRequest(node *ServerNode) (*http.Request, error) {
	body := map[string]interface{}{
		"DocumentId": c.documentID,
		"Name":       c.seriesName,
		"Deletes": []map[string]interface{}{
			{
				"From": c.from.UTC().Format(time.RFC3339Nano),
				"To":   c.to.UTC().Format(time.RFC3339Nano),
			},
		},
	}
	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	urlStr := node.URL + "/databases/" + node.Database + "/timeseries?docId=" + url.QueryEscape(c.documentID)
	return NewHttpPost(urlStr, b)
}

func (c *DeleteTimeSeriesRangeCommand) SetResponse(response []byte, fromCache bool) error {
	return nil
}

func (o *OperationExecutor) ConfigureTimeSeries(collection string, policy TimeSeriesPolicy) error {
	return o.ConfigureTimeSeriesContext(context.Background(), collection, policy)
}

func (o *OperationExecutor) ConfigureTimeSeriesContext(ctx context.Context, collection string, policy TimeSeriesPolicy) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	cmd := NewConfigureTimeSeriesCommand(collection, policy)
	if err := o.requestExecutor.ExecuteCommandContext(ctx, cmd, nil); err != nil {
		return err
	}
	return nil
}

type ConfigureTimeSeriesCommand struct {
	RavenCommandBase
	collection string
	policy     TimeSeriesPolicy
}

func NewConfigureTimeSeriesCommand(collection string, policy TimeSeriesPolicy) *ConfigureTimeSeriesCommand {
	cmd := &ConfigureTimeSeriesCommand{collection: collection, policy: policy}
	cmd.RavenCommandBase.ResponseType = RavenCommandResponseTypeEmpty
	return cmd
}

func (c *ConfigureTimeSeriesCommand) CreateRequest(node *ServerNode) (*http.Request, error) {
	body := map[string]interface{}{
		"Collection": c.collection,
	}
	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	urlStr := node.URL + "/databases/" + node.Database + "/timeseries/configuration"
	return NewHttpPost(urlStr, b)
}

func (c *ConfigureTimeSeriesCommand) SetResponse(response []byte, fromCache bool) error {
	return nil
}
