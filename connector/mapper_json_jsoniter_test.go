package connector

import (
	"github.com/myrteametrics/myrtea-sdk/v4/expression"
	"testing"
)

func TestMapMessage(t *testing.T) {
	message := KafkaMessage{Data: []byte(
		`{"uuid":{"least":-5360973783440353337,"most":-814119054879674195},"fields":{"mystring":"helloworld","myint":1234567,"mybool":true}}`,
	)}
	mapper := JSONMapperJsoniter{
		filters: make(map[string]JSONMapperFilterItem),
		mapping: map[string]map[string]JSONMapperConfigItem{
			"record": {
				"uuid": {
					FieldType: "uuid_from_longs",
					Paths: [][]string{
						{"uuid", "most"},
						{"uuid", "least"},
					},
				},
				"mystring": {
					FieldType: "string",
					Paths:     [][]string{{"fields", "mystring"}},
				},
				"myint": {
					FieldType: "int",
					Paths:     [][]string{{"fields", "myint"}},
				},
				"mybool": {
					FieldType: "boolean",
					Paths:     [][]string{{"fields", "mybool"}},
				},
			},
		},
	}
	msg, err := mapper.MapToDocument(message)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	output := msg.(TypedDataMessage)
	t.Log(output.Strings)
	t.Log(output.Ints)
	t.Log(output.Bools)
	t.Log(output.Times)
}

func TestLookupNestedMapWithSlice(t *testing.T) {
	data := map[string]interface{}{
		"a": map[string]interface{}{
			"b": map[string]interface{}{
				"c": []interface{}{
					map[string]interface{}{"d": 1},
					map[string]interface{}{"d": 2},
				},
			},
			"e": map[string]interface{}{
				"c": []interface{}{
					map[string]interface{}{"f": 98},
					map[string]interface{}{"f": 99},
				},
			},
		},
	}

	t.Log(lookupNestedMap([]string{"a", "e", "c", "[0]", "f"}, data))
}

func TestLookupNestedMapWithWildcard(t *testing.T) {
	data := map[string]interface{}{
		"a": map[string]interface{}{
			"b": map[string]interface{}{
				"c": 1,
			},
			"e": map[string]interface{}{
				"f": 99,
			},
		},
	}

	t.Log(lookupNestedMap([]string{"a", "*", "f"}, data))

	val, ok := lookupNestedMap([]string{"a", "*", "f"}, data)

	if !ok {
		t.FailNow()
	}

	switch v := val.(type) {
	case int:
		expression.AssertEqual(t, v, 99)
		break
	default:
		t.FailNow()
	}
}

func TestLookupNestedMapWithWildcardAndSlice(t *testing.T) {
	data := map[string]interface{}{
		"a": map[string]interface{}{
			"b": map[string]interface{}{
				"c": []interface{}{
					map[string]interface{}{"d": 1},
					map[string]interface{}{"d": 2},
				},
			},
			"e": map[string]interface{}{
				"c": []interface{}{
					map[string]interface{}{"f": 98},
					map[string]interface{}{"f": 99},
				},
			},
		},
	}

	t.Log(lookupNestedMap([]string{"a", "*", "c", "[0]", "f"}, data))

	val, ok := lookupNestedMap([]string{"a", "*", "c", "[0]", "f"}, data)

	if !ok {
		t.FailNow()
	}

	switch v := val.(type) {
	case int:
		expression.AssertEqual(t, v, 98)
		break
	default:
		t.FailNow()
	}

	val, ok = lookupNestedMap([]string{"a", "*", "c", "[1]", "f"}, data)

	if !ok {
		t.FailNow()
	}

	switch v := val.(type) {
	case int:
		expression.AssertEqual(t, v, 99)
		break
	default:
		t.FailNow()
	}

}

func TestLookupNestedMapFullPaths(t *testing.T) {
	data := map[string]interface{}{
		"a": map[string]interface{}{
			"b": map[string]interface{}{
				"c": []interface{}{
					map[string]interface{}{"d": "1"},
					map[string]interface{}{"d": "2"},
				},
			},
			"e": map[string]interface{}{
				"c": []interface{}{
					map[string]interface{}{"f": "98"},
					map[string]interface{}{"f": "99"},
				},
			},
		},
	}

	val, ok := lookupNestedMapFullPaths(data, [][]string{{"a", "*", "c", "[0]", "f"}, {"a", "*", "c", "[1]", "f"}}, "")

	if !ok {
		t.FailNow()
	}

	switch v := val.(type) {
	case string:
		expression.AssertEqual(t, v, "9899")
		break
	default:
		t.FailNow()
	}

}

// Create tests for FilterDocument
