package ravendb

import (
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type exactNumberDocument struct {
	Minimum int64                  `json:"Minimum"`
	Maximum int64                  `json:"Maximum"`
	Values  []interface{}          `json:"Values"`
	Nested  map[string]interface{} `json:"Nested"`
}

func TestJSONUnmarshalPreservesExactIntegersInNestedValues(t *testing.T) {
	const document = `{"Values":[-9223372036854775808,-9007199254740993,-9007199254740992,9007199254740992,9007199254740993,9223372036854775807,1.25,1.0,9007199254740993e0,18446744073709551615],"Nested":{"records":[{"value":9223372036854775807}]}}`
	var decoded map[string]interface{}
	require.NoError(t, jsonUnmarshal([]byte(document), &decoded))

	values := decoded["Values"].([]interface{})
	require.Equal(t, int64(-1<<63), values[0])
	require.Equal(t, int64(-(1<<53)-1), values[1])
	require.Equal(t, float64(-(1 << 53)), values[2])
	require.Equal(t, float64(1<<53), values[3])
	require.Equal(t, int64(1<<53+1), values[4])
	require.Equal(t, int64(1<<63-1), values[5])
	require.Equal(t, float64(1.25), values[6])
	require.Equal(t, float64(1), values[7])
	require.Equal(t, int64(9007199254740993), values[8])
	require.Equal(t, ^uint64(0), values[9])

	nested := decoded["Nested"].(map[string]interface{})
	records := nested["records"].([]interface{})
	require.Equal(t, int64(1<<63-1), records[0].(map[string]interface{})["value"])

	encoded, err := jsonMarshal(decoded)
	require.NoError(t, err)
	for _, boundary := range []string{"-9223372036854775808", "-9007199254740993", "9007199254740993", "9223372036854775807", "18446744073709551615"} {
		require.Contains(t, string(encoded), boundary)
	}
}

func TestEntityMapRoundTripPreservesNestedInt64AndFloatValues(t *testing.T) {
	document := &exactNumberDocument{
		Minimum: -1 << 63,
		Maximum: 1<<63 - 1,
		Values:  []interface{}{int64(-9007199254740993), float64(0.125), map[string]interface{}{"n": int64(9007199254740993)}},
		Nested:  map[string]interface{}{"array": []interface{}{int64(1<<63 - 1), float64(2.5)}},
	}

	jsonMap := structToJSONMap(document)
	require.Equal(t, document.Minimum, jsonMap["Minimum"])
	require.Equal(t, document.Maximum, jsonMap["Maximum"])
	require.Equal(t, int64(-9007199254740993), jsonMap["Values"].([]interface{})[0])
	require.Equal(t, float64(0.125), jsonMap["Values"].([]interface{})[1])

	decoded, err := makeStructFromJSONMap(reflect.TypeOf(document), jsonMap)
	require.NoError(t, err)
	require.Equal(t, document, decoded)
}

func TestJSONIntegerHelpersRejectLossyConversions(t *testing.T) {
	for _, test := range []struct {
		value interface{}
		want  int64
	}{
		{int64(-1 << 63), -1 << 63},
		{int64(1<<63 - 1), 1<<63 - 1},
		{float64(-1 << 63), -1 << 63},
		{json.Number("9223372036854775807"), 1<<63 - 1},
		{json.Number("9007199254740993e0"), 9007199254740993},
	} {
		got, ok := jsonValueAsInt64(test.value)
		require.True(t, ok)
		require.Equal(t, test.want, got)
	}

	for _, value := range []interface{}{float64(1.5), float64(1 << 63), uint64(1 << 63)} {
		_, ok := jsonValueAsInt64(value)
		require.False(t, ok, "value %v (%T) must not truncate or overflow", value, value)
	}
	_, ok := jsonValueAsInt64(float64(math.NaN()))
	require.False(t, ok)
	_, ok = jsonValueAsInt64(float64(math.Inf(1)))
	require.False(t, ok)

	got, ok := jsonGetAsInt(map[string]interface{}{"n": int64(42)}, "n")
	require.True(t, ok)
	require.Equal(t, 42, got)
	_, ok = jsonGetAsInt(map[string]interface{}{"n": float64(42.5)}, "n")
	require.False(t, ok)
}

func TestQueryResponseKeepsExactProjectedInt64(t *testing.T) {
	command, err := NewQueryCommand(NewDocumentConventions(), &IndexQuery{}, false, false)
	require.NoError(t, err)
	err = command.SetResponse([]byte(`{"Results":[{"Value":9223372036854775807,"@metadata":{"@projection":true}}]}`), false)
	require.NoError(t, err)

	document := command.Result.Results[0]
	metadata := document[MetadataKey].(map[string]interface{})
	var got int64
	fields := newFieldsToFetchToken([]string{"Value"}, []string{"Value"}, false, "")
	require.NoError(t, queryOperationDeserialize(&got, "", document, metadata, fields, true, nil))
	require.Equal(t, int64(1<<63-1), got)
}

func TestQueryResponseExtremeExponentsDoNotPanic(t *testing.T) {
	for _, test := range []struct {
		name           string
		number         string
		wantRangeError bool
		wantFloat64    bool
	}{
		{name: "maximum positive exponent", number: "1e9223372036854775807", wantRangeError: true},
		{name: "near maximum positive exponent", number: "1e9223372036854775806", wantRangeError: true},
		{name: "fraction and maximum exponent", number: "1.000e9223372036854775807", wantRangeError: true},
		{name: "minimum negative exponent", number: "1e-9223372036854775808"},
		{name: "near minimum negative exponent", number: "1e-9223372036854775807"},
		{name: "fraction at minimum exponent", number: "1.000e-9223372036854775808"},
		{name: "fraction scale boundary", number: "1.000e-9223372036854775805"},
		{name: "below int64 range", number: "-9223372036854775809", wantFloat64: true},
		{name: "above uint64 range", number: "18446744073709551616", wantFloat64: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			command, err := NewQueryCommand(NewDocumentConventions(), &IndexQuery{}, false, false)
			require.NoError(t, err)

			response := []byte(`{"Results":[{"Value":` + test.number + `}]}`)
			var responseErr error
			require.NotPanics(t, func() {
				responseErr = command.SetResponse(response, false)
			})
			if test.wantRangeError {
				require.True(t, errors.Is(responseErr, strconv.ErrRange), "expected range error, got %v", responseErr)
				return
			}
			if responseErr != nil {
				require.True(t, errors.Is(responseErr, strconv.ErrRange), "expected range error, got %v", responseErr)
				require.False(t, test.wantFloat64, "expected float64 fallback, got %v", responseErr)
				return
			}
			value := command.Result.Results[0]["Value"]
			require.IsType(t, float64(0), value)
			if test.wantFloat64 {
				require.NoError(t, responseErr)
			}
		})
	}
}

func TestCompareExchangePreservesExactInt64(t *testing.T) {
	const response = `{"Index":9007199254740993,"Successful":true,"Value":{"Object":-9223372036854775808}}`
	result, err := parseCompareExchangeResultFromString(reflect.TypeOf(int64(0)), []byte(response), nil)
	require.NoError(t, err)
	require.Equal(t, int64(9007199254740993), result.Index)
	require.Equal(t, int64(-1<<63), result.Value)

	values, err := compareExchangeValueResultParserGetValues(reflect.TypeOf(int64(0)), []byte(`{"Results":[{"Key":"k","Index":9223372036854775807,"Value":{"Object":9223372036854775807}}]}`), nil)
	require.NoError(t, err)
	require.Equal(t, int64(1<<63-1), values["k"].Index)
	require.Equal(t, int64(1<<63-1), values["k"].Value)

	serialized, err := jsonMarshal((&CompareExchangeSessionValue{}).compareExchangeValueToJson(&exactNumberDocument{Maximum: 1<<63 - 1}, nil))
	require.NoError(t, err)
	require.True(t, strings.Contains(string(serialized), `"Maximum":9223372036854775807`))
}

func TestJSONChangeTrackingComparesExactIntegerValues(t *testing.T) {
	var changes []*DocumentsChanges
	original := map[string]interface{}{"n": int64(1<<63 - 1), "nested": []interface{}{int64(9007199254740993)}}
	unchanged := map[string]interface{}{"n": int64(1<<63 - 1), "nested": []interface{}{int64(9007199254740993)}}
	require.False(t, jsonOperationCompareJson("id", original, unchanged, nil, &changes))

	changed := map[string]interface{}{"n": int64(1<<63 - 2), "nested": []interface{}{int64(9007199254740993)}}
	require.True(t, jsonOperationCompareJson("id", original, changed, nil, &changes))
}
