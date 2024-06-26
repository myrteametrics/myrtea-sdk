package connector

import (
	"fmt"
	"github.com/google/go-cmp/cmp"
	"testing"

	"github.com/myrteametrics/myrtea-sdk/v5/expression"
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

func TestMapMessageWithCachedData(t *testing.T) {
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

	decodedMsg, err := mapper.DecodeDocument(message)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	_, ok := decodedMsg.(DecodedKafkaMessage)
	expression.AssertEqual(t, ok, true, "message should be of type DecodedKafkaMessage")

	dMsg, err := mapper.MapToDocument(decodedMsg)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	msg, err := mapper.MapToDocument(message)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	dOutput := dMsg.(TypedDataMessage)
	output := msg.(TypedDataMessage)

	expression.AssertEqual(t, cmp.Equal(dOutput, output), true, "output and dOutput should match")

}

func createTestJSONMapperJsoniter(Paths [][]string, Value string, Values []string, Condition string) *JSONMapperJsoniter {
	return &JSONMapperJsoniter{
		filters: map[string]JSONMapperFilterItem{
			"f1": {
				Paths:        Paths,
				Keep:         true,
				Separator:    "",
				Condition:    Condition,
				Value:        Value,
				Values:       Values,
				DefaultValue: "",
			},
		},
		mapping: make(map[string]map[string]JSONMapperConfigItem),
	}
}

func TestFilterDocument_EmptyFilters(t *testing.T) {
	mapper := &JSONMapperJsoniter{
		filters: map[string]JSONMapperFilterItem{},
		mapping: make(map[string]map[string]JSONMapperConfigItem),
	}
	expression.AssertEqual(t, len(mapper.filters), 0)

	var doc Message
	doc = DecodedKafkaMessage{Data: map[string]interface{}{}}

	keep, reason := mapper.FilterDocument(doc)
	expression.AssertEqual(t, keep, true, fmt.Sprintf("No filters: msg should be keeped (DecodedKafkaMessage), reason: %s", reason))
	expression.AssertEqual(t, reason, "")

	doc = KafkaMessage{Data: []byte{}}

	keep, reason = mapper.FilterDocument(doc)
	expression.AssertEqual(t, keep, true, "No filters: msg should be keeped (KafkaMessage)")
	expression.AssertEqual(t, reason, "")

}

// Create tests for FilterDocument
func TestFilterDocument(t *testing.T) {
	message := KafkaMessage{Data: []byte(
		`{"uuid":{"least":-5360973783440353337,"most":-814119054879674195},"fields":{"mystring":"helloworld","myint":1234567,"mybool":true}}`,
	)}
	message1 := KafkaMessage{Data: []byte(
		`{"a":{"b":{"c":[{"d":1},{"d":2}]},"e":{"c":[{"f":98},{"f":99}]}}}`,
	)}

	// test supports only kafka message
	mapper := createTestJSONMapperJsoniter(nil, "", nil, "")
	keep, reason := mapper.FilterDocument(&MessageWithOptions{})
	expression.AssertEqual(t, keep, false)
	expression.AssertEqual(t, reason, "message type not supported")

	// test filter with decoded message
	decoded, err := mapper.DecodeDocument(message)
	if err != nil {
		t.Error()
		t.FailNow()
	}
	_, ok := decoded.(DecodedKafkaMessage)
	expression.AssertEqual(t, ok, true, "message should be of type DecodedKafkaMessage")

	// we run the filter function with any filter to see whether it runs fine
	mapper = createTestJSONMapperJsoniter([][]string{{"uuid", "most"}}, "", nil, "exists")
	keep, _ = mapper.FilterDocument(decoded)
	expression.AssertEqual(t, keep, true)

	// test exists
	mapper = createTestJSONMapperJsoniter([][]string{{"uuid", "most"}}, "", nil, "exists")
	keep, _ = mapper.FilterDocument(message)
	expression.AssertEqual(t, keep, true)

	// test not exists
	mapper = createTestJSONMapperJsoniter([][]string{{"uuid", "notexists"}}, "", nil, "exists")
	keep, _ = mapper.FilterDocument(message)
	expression.AssertEqual(t, keep, false)

	// test equals
	mapper = createTestJSONMapperJsoniter([][]string{{"fields", "mystring"}}, "helloworld", nil, "equals")
	keep, _ = mapper.FilterDocument(message)
	expression.AssertEqual(t, keep, true)

	// test equals with multiple paths
	mapper = createTestJSONMapperJsoniter([][]string{{"fields", "mystring"}, {"fields", "myint"}}, "helloworld1234567", nil, "equals")
	keep, _ = mapper.FilterDocument(message)
	expression.AssertEqual(t, keep, true)

	// test equals with multiple paths, wildcard and array access
	mapper = createTestJSONMapperJsoniter([][]string{{"a", "*", "c", "[0]", "f"}, {"a", "*", "c", "[1]", "f"}}, "9899", nil, "equals")
	keep, _ = mapper.FilterDocument(message1)
	expression.AssertEqual(t, keep, true)

	// test not equals
	mapper = createTestJSONMapperJsoniter([][]string{{"fields", "mystring"}}, "hellorld", nil, "equals")
	keep, _ = mapper.FilterDocument(message)
	expression.AssertEqual(t, keep, false)

	// test not equals with multiple paths
	mapper = createTestJSONMapperJsoniter([][]string{{"fields", "mystring"}, {"fields", "myint"}}, "helloworld14567", nil, "equals")
	keep, _ = mapper.FilterDocument(message)
	expression.AssertEqual(t, keep, false)

	// test not equals with multiple paths, wildcard and array access
	mapper = createTestJSONMapperJsoniter([][]string{{"a", "*", "c", "[0]", "f"}, {"a", "*", "c", "[1]", "f"}}, "9a99", nil, "equals")
	keep, _ = mapper.FilterDocument(message1)
	expression.AssertEqual(t, keep, false)

	// test equals_atleastone
	mapper = createTestJSONMapperJsoniter([][]string{{"fields", "mystring"}, {"fields", "myint"}}, "", []string{"helloworld1234567", "test"}, "equals_atleastone")
	keep, _ = mapper.FilterDocument(message)
	expression.AssertEqual(t, keep, true)

	// test not equals_atleastone
	mapper = createTestJSONMapperJsoniter([][]string{{"uuid", "most"}, {"uuid", "least"}}, "", []string{"helloworld1234567", "test"}, "equals_atleastone")
	keep, _ = mapper.FilterDocument(message)
	expression.AssertEqual(t, keep, false)

	// test notEquals
	mapper = createTestJSONMapperJsoniter([][]string{{"fields", "mystring"}}, "helloworld", nil, "notEquals")
	keep, _ = mapper.FilterDocument(message)
	expression.AssertEqual(t, keep, false)

	// test notEquals with multiple paths
	mapper = createTestJSONMapperJsoniter([][]string{{"fields", "mystring"}, {"fields", "myint"}}, "helloworld1234567", nil, "notEquals")
	keep, _ = mapper.FilterDocument(message)
	expression.AssertEqual(t, keep, false)

	// test notEquals with multiple paths, wildcard and array access
	mapper = createTestJSONMapperJsoniter([][]string{{"a", "*", "c", "[0]", "f"}, {"a", "*", "c", "[1]", "f"}}, "9899", nil, "notEquals")
	keep, _ = mapper.FilterDocument(message1)
	expression.AssertEqual(t, keep, false)

	// test not notEquals
	mapper = createTestJSONMapperJsoniter([][]string{{"fields", "mystring"}}, "hellowsorld", nil, "notEquals")
	keep, _ = mapper.FilterDocument(message)
	expression.AssertEqual(t, keep, true)

	// test not notEquals with multiple paths
	mapper = createTestJSONMapperJsoniter([][]string{{"fields", "mystring"}, {"fields", "myint"}}, "hellowsorld1234567", nil, "notEquals")
	keep, _ = mapper.FilterDocument(message)
	expression.AssertEqual(t, keep, true)

	// test not notEquals with multiple paths, wildcard and array access
	mapper = createTestJSONMapperJsoniter([][]string{{"a", "*", "c", "[0]", "f"}, {"a", "*", "c", "[1]", "f"}}, "98s99", nil, "notEquals")
	keep, _ = mapper.FilterDocument(message1)
	expression.AssertEqual(t, keep, true)

	// test startWith
	mapper = createTestJSONMapperJsoniter([][]string{{"fields", "mystring"}}, "hello", nil, "startWith")
	keep, _ = mapper.FilterDocument(message)
	expression.AssertEqual(t, keep, true)

	// test not startWith
	mapper = createTestJSONMapperJsoniter([][]string{{"fields", "mystring"}}, "world", nil, "startWith")
	keep, _ = mapper.FilterDocument(message)
	expression.AssertEqual(t, keep, false)

	// test endWith
	mapper = createTestJSONMapperJsoniter([][]string{{"fields", "mystring"}}, "world", nil, "endWith")
	keep, _ = mapper.FilterDocument(message)
	expression.AssertEqual(t, keep, true)

	// test not endWith
	mapper = createTestJSONMapperJsoniter([][]string{{"fields", "mystring"}}, "hello", nil, "endWith")
	keep, _ = mapper.FilterDocument(message)
	expression.AssertEqual(t, keep, false)

	// test contains
	mapper = createTestJSONMapperJsoniter([][]string{{"fields", "mystring"}}, "world", nil, "contains")
	keep, _ = mapper.FilterDocument(message)
	expression.AssertEqual(t, keep, true)

	// test not contains
	mapper = createTestJSONMapperJsoniter([][]string{{"fields", "mystring"}}, "test", nil, "contains")
	keep, _ = mapper.FilterDocument(message)
	expression.AssertEqual(t, keep, false)

}

func TestFilterNotEqualAny(t *testing.T) {
	message := KafkaMessage{Data: []byte(
		`{"uuid":{"least":-5360973783440353337,"most":-814119054879674195},"fields":{"mystring":"NN","mystring2":"NI","mybool":true}}`,
	)}

	// test equals_atleastone
	mapper := createTestJSONMapperJsoniter([][]string{{"fields", "mystring"}}, "", []string{"NI", "IN"}, "notEquals_any")
	keep, _ := mapper.FilterDocument(message)
	expression.AssertEqual(t, keep, true)

	// test not equals_atleastone
	mapper = createTestJSONMapperJsoniter([][]string{{"fields", "mystring2"}}, "", []string{"NI", "IN"}, "notEquals_any")
	keep, _ = mapper.FilterDocument(message)
	expression.AssertEqual(t, keep, false)

}
