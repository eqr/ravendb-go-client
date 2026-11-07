package ravendb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

// Note: com.fasterxml.jackson.databind.JsonNode represents decoded JSON value i.e. interface{}

// Note: Java's com.fasterxml.jackson.databind.node.JsonNodeType represents type of JSON value, which in Go
// is the same as value itself, and therefore is interface{}

// Note: Java's com.fasterxml.jackson.databind.node.ObjectNode is map[string]interface{}
// It represents parsed json document

// Note: Java's com.fasterxml.jackson.databind.TreeNode represents a decoded JSON value that combines
// value and type. In Go it's interface{}

// Note: Java's com.fasterxml.jackson.databind.node.ArrayNode  represents array of JSON objects
// It's []map[string]interface{} in Go

// we should use jsonMarshal instead of jsonMarshal so that it's easy
// to change json marshalling in all code base (e.g. to use a faster
// json library or ensure that values are marshaled correctly)
func jsonMarshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func jsonUnmarshal(d []byte, v interface{}) error {
	if !jsonTypeHasInterfaceValues(reflect.TypeOf(v)) {
		return json.Unmarshal(d, v)
	}
	decoder := json.NewDecoder(bytes.NewReader(d))
	decoder.UseNumber()
	if err := decoder.Decode(v); err != nil {
		return err
	}
	var extra interface{}
	if err := decoder.Decode(&extra); err != io.EOF {
		if err != nil {
			return err
		}
		return fmt.Errorf("unexpected additional JSON value")
	}
	return normalizeJSONNumbers(reflect.ValueOf(v))
}

func jsonTypeHasInterfaceValues(typ reflect.Type) bool {
	if typ == nil {
		return false
	}
	if cached, ok := jsonTypeInterfaceValues.Load(typ); ok {
		return cached.(bool)
	}
	result := jsonTypeContainsInterfaceValue(typ, make(map[reflect.Type]bool))
	jsonTypeInterfaceValues.Store(typ, result)
	return result
}

func jsonTypeContainsInterfaceValue(typ reflect.Type, visited map[reflect.Type]bool) bool {
	if typ.Kind() == reflect.Interface {
		return true
	}
	if visited[typ] {
		return false
	}
	visited[typ] = true
	switch typ.Kind() {
	case reflect.Ptr, reflect.Slice, reflect.Array, reflect.Map:
		return jsonTypeContainsInterfaceValue(typ.Elem(), visited)
	case reflect.Struct:
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			if field.PkgPath != "" && !field.Anonymous {
				continue
			}
			if jsonTypeContainsInterfaceValue(field.Type, visited) {
				return true
			}
		}
	}
	return false
}

const maxInt64Value = int64(1<<63 - 1)

var jsonNumberType = reflect.TypeOf(json.Number(""))
var jsonTypeInterfaceValues sync.Map

func normalizeJSONNumbers(value reflect.Value) error {
	if !value.IsValid() || value.Type() == jsonNumberType {
		return nil
	}
	switch value.Kind() {
	case reflect.Ptr:
		if value.IsNil() {
			return nil
		}
		return normalizeJSONNumbers(value.Elem())
	case reflect.Interface:
		if value.IsNil() {
			return nil
		}
		element := value.Elem()
		if element.Type() == jsonNumberType {
			number, err := decodeJSONNumber(element.Interface().(json.Number))
			if err != nil {
				return err
			}
			value.Set(reflect.ValueOf(number))
			return nil
		}
		switch element.Kind() {
		case reflect.Map, reflect.Slice, reflect.Ptr:
			return normalizeJSONNumbers(element)
		case reflect.Struct, reflect.Array:
			if !value.CanSet() {
				return nil
			}
			copy := reflect.New(element.Type()).Elem()
			copy.Set(element)
			if err := normalizeJSONNumbers(copy); err != nil {
				return err
			}
			value.Set(copy)
		}
	case reflect.Map:
		iter := value.MapRange()
		for iter.Next() {
			entry := reflect.New(value.Type().Elem()).Elem()
			entry.Set(iter.Value())
			if err := normalizeJSONNumbers(entry); err != nil {
				return err
			}
			value.SetMapIndex(iter.Key(), entry)
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < value.Len(); i++ {
			if value.Index(i).CanSet() {
				if err := normalizeJSONNumbers(value.Index(i)); err != nil {
					return err
				}
			}
		}
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			field := value.Field(i)
			if field.CanSet() {
				if err := normalizeJSONNumbers(field); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
func normalizeJSONNumberMap(value map[string]interface{}) error {
	if value == nil {
		return nil
	}
	return normalizeJSONNumbers(reflect.ValueOf(value))
}
func decodeJSONNumber(number json.Number) (interface{}, error) {
	text := number.String()
	const maxSafeInteger = int64(1 << 53)
	if integer, ok := exactJSONInteger(text); ok {
		if signed, err := strconv.ParseInt(integer, 10, 64); err == nil {
			if signed >= -maxSafeInteger && signed <= maxSafeInteger {
				return float64(signed), nil
			}
			return signed, nil
		}
		if unsigned, err := strconv.ParseUint(integer, 10, 64); err == nil {
			if unsigned <= uint64(maxSafeInteger) {
				return float64(unsigned), nil
			}
			return unsigned, nil
		}
	}
	return number.Float64()
}

func exactJSONInteger(text string) (string, bool) {
	if text == "" {
		return "", false
	}
	negative := text[0] == '-'
	if negative {
		text = text[1:]
		if text == "" {
			return "", false
		}
	}
	exponent := int64(0)
	if exponentIndex := strings.IndexAny(text, "eE"); exponentIndex >= 0 {
		exponentText := text[exponentIndex+1:]
		if exponentText == "" {
			return "", false
		}
		for i := 0; i < len(exponentText); i++ {
			if i == 0 && (exponentText[i] == '+' || exponentText[i] == '-') {
				continue
			}
			if exponentText[i] < '0' || exponentText[i] > '9' {
				return "", false
			}
		}
		var err error
		exponent, err = strconv.ParseInt(exponentText, 10, 64)
		if err != nil {
			return "", false
		}
		text = text[:exponentIndex]
	}

	integerPart := text
	fractionPart := ""
	if decimalIndex := strings.IndexByte(text, '.'); decimalIndex >= 0 {
		integerPart = text[:decimalIndex]
		fractionPart = text[decimalIndex+1:]
		if fractionPart == "" || strings.IndexByte(fractionPart, '.') >= 0 {
			return "", false
		}
	}
	if integerPart == "" || (len(integerPart) > 1 && integerPart[0] == '0') {
		return "", false
	}
	for i := 0; i < len(integerPart); i++ {
		if integerPart[i] < '0' || integerPart[i] > '9' {
			return "", false
		}
	}
	for i := 0; i < len(fractionPart); i++ {
		if fractionPart[i] < '0' || fractionPart[i] > '9' {
			return "", false
		}
	}
	digits := strings.TrimLeft(integerPart+fractionPart, "0")
	if digits == "" {
		if negative {
			return "", false
		}
		return "0", true
	}
	fractionDigits := int64(len(fractionPart))
	if exponent < -maxInt64Value-1+fractionDigits {
		return "", false
	}
	scale := exponent - fractionDigits
	if scale > 0 {
		digitCount := int64(len(digits))
		if digitCount > 20 || scale > 20-digitCount {
			return "", false
		}
		digits += strings.Repeat("0", int(scale))
	} else if scale < 0 {
		if scale == -maxInt64Value-1 {
			return "", false
		}
		removeCount := -scale
		if removeCount >= int64(len(digits)) {
			return "", false
		}
		remove := int(removeCount)
		for i := len(digits) - remove; i < len(digits); i++ {
			if digits[i] != '0' {
				return "", false
			}
		}
		digits = digits[:len(digits)-remove]
	}
	if negative {
		digits = "-" + digits
	}
	return digits, true
}

func float64ToInt64(value float64) (int64, bool) {
	if math.IsNaN(value) || math.IsInf(value, 0) || math.Trunc(value) != value ||
		value < -0x1p63 || value >= 0x1p63 {
		return 0, false
	}
	return int64(value), true
}

func jsonValueAsInt64(value interface{}) (int64, bool) {
	switch number := value.(type) {
	case int:
		return int64(number), true
	case int8:
		return int64(number), true
	case int16:
		return int64(number), true
	case int32:
		return int64(number), true
	case int64:
		return number, true
	case uint:
		return uint64AsInt64(uint64(number))
	case uint8:
		return int64(number), true
	case uint16:
		return int64(number), true
	case uint32:
		return int64(number), true
	case uint64:
		return uint64AsInt64(number)
	case float64:
		return float64ToInt64(number)
	case json.Number:
		integer, ok := exactJSONInteger(number.String())
		if !ok {
			return 0, false
		}
		parsed, err := strconv.ParseInt(integer, 10, 64)
		return parsed, err == nil
	case string:
		parsed, err := strconv.ParseInt(number, 10, 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}

func uint64AsInt64(value uint64) (int64, bool) {
	if value > uint64(maxInt64Value) {
		return 0, false
	}
	return int64(value), true
}

func jsonGetAsTextPointer(doc map[string]interface{}, key string) *string {
	v, ok := doc[key]
	if !ok {
		return nil
	}
	// TODO: only allow *string ?
	if s, ok := v.(*string); ok {
		return s
	}
	s := v.(string)
	return &s
}

func jsonGetAsString(doc map[string]interface{}, key string) (string, bool) {
	return jsonGetAsText(doc, key)
}

func jsonGetAsText(doc map[string]interface{}, key string) (string, bool) {
	v, ok := doc[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	if !ok {
		return "", false
	}
	return s, true
}

func jsonGetAsInt(doc map[string]interface{}, key string) (int, bool) {
	v, ok := doc[key]
	if !ok {
		return 0, false
	}
	value, ok := jsonValueAsInt64(v)
	if !ok {
		return 0, false
	}
	result := int(value)
	if int64(result) != value {
		return 0, false
	}
	return result, true
}

func jsonGetAsInt64(doc map[string]interface{}, key string) (int64, bool) {
	v, ok := doc[key]
	if !ok {
		return 0, false
	}
	return jsonValueAsInt64(v)
}

func jsonGetAsBool(doc map[string]interface{}, key string) (bool, bool) {
	v, ok := doc[key]
	if !ok {
		return false, false
	}
	b, ok := v.(bool)
	if ok {
		return b, true
	}
	s, ok := v.(string)
	if !ok {
		return false, false
	}
	if strings.EqualFold(s, "true") {
		return true, true
	}
	if strings.EqualFold(s, "false") {
		return false, true
	}
	return false, false
}

// converts a struct to JSON representations as map of string to value
// TODO: could be faster
func structToJSONMap(v interface{}) map[string]interface{} {
	d, err := jsonMarshal(v)
	must(err)
	var res map[string]interface{}
	err = jsonUnmarshal(d, &res)
	must(err)
	return res
}

// given a json in the form of map[string]interface{}, de-serialize it to a struct
// TODO: could be faster
func structFromJSONMap(js map[string]interface{}, v interface{}) error {
	d, err := jsonMarshal(js)
	if err != nil {
		return err
	}
	return jsonUnmarshal(d, v)
}

// matches a Java naming from EnityMapper
func valueToTree(v interface{}) map[string]interface{} {
	return structToJSONMap(v)
}

// TODO: remove
// copyJSONMap makes a deep copy of map[string]interface{}
// TODO: possibly not the fastest way to do it
/*
func copyJSONMap(v map[string]interface{}) map[string]interface{} {
	d, err := jsonMarshal(v)
	must(err)
	var res map[string]interface{}
	err = jsonUnmarshal(d, &res)
	must(err)
	return res
}
*/

// jsonDecodeFirst decode first JSON object from d
// This is like jsonUnmarshal() but allows for d
// to contain multiple JSON objects
// This is for compatibility with Java's ObjectMapper.readTree()
func jsonUnmarshalFirst(d []byte, v interface{}) error {
	r := bytes.NewReader(d)
	dec := json.NewDecoder(r)
	err := dec.Decode(v)
	if err != nil {
		dbg("jsonDecodeFirst: dec.Decode() of type %T failed with %s. JSON:\n%s\n\n", v, err, string(d))
	}
	return err
}

func isUnprintable(c byte) bool {
	if c < 32 {
		// 9 - tab, 10 - LF, 13 - CR
		if c == 9 || c == 10 || c == 13 {
			return false
		}
		return true
	}
	return c >= 127
}

func isBinaryData(d []byte) bool {
	for _, b := range d {
		if isUnprintable(b) {
			return true
		}
	}
	return false
}

func asHex(d []byte) ([]byte, bool) {
	if !isBinaryData(d) {
		return d, false
	}

	// convert unprintable characters to hex
	var res []byte
	for i, c := range d {
		if i > 2048 {
			break
		}
		if isUnprintable(c) {
			s := fmt.Sprintf("x%02x ", c)
			res = append(res, s...)
		} else {
			res = append(res, c)
		}
	}
	return res, true
}

// if d is a valid json, pretty-print it
// only used for debugging
func maybePrettyPrintJSON(d []byte) []byte {
	if d2, ok := asHex(d); ok {
		return d2
	}
	var m map[string]interface{}
	err := json.Unmarshal(d, &m)
	if err != nil {
		return d
	}
	d2, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return d
	}
	return d2
}

func stringPtrToString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
