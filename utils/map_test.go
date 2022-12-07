package utils

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestSearchNestedMap(t *testing.T) {
	testSearchNestedMap(t,
		strings.Split("a.b", "."),
		map[string]interface{}{"a": map[string]interface{}{"b": 10}},
		true,
		10,
	)

	testSearchNestedMap(t,
		strings.Split("a.b.c.d.e", "."),
		map[string]interface{}{"a": map[string]interface{}{"b": map[string]interface{}{"c": map[string]interface{}{"d": map[string]interface{}{"e": 10}}}}},
		true,
		10,
	)

	testSearchNestedMap(t,
		strings.Split("a.b", "."),
		map[string]interface{}{"a": map[string]interface{}{"b": map[string]interface{}{"c": 10}}},
		false,
		nil,
	)

	testSearchNestedMap(t,
		strings.Split("a.b.c.d", "."),
		map[string]interface{}{"a": map[string]interface{}{"b": map[string]interface{}{"c": 10}}},
		false,
		nil,
	)
}

func TestUpdateNestedMap(t *testing.T) {
	testUpdateNestedMap(t,
		strings.Split("a.b.c", "."),
		map[string]interface{}{"a": map[string]interface{}{"b": map[string]interface{}{"c": 10}}},
		50,
		true,
		map[string]interface{}{"a": map[string]interface{}{"b": map[string]interface{}{"c": 50}}},
	)

	testUpdateNestedMap(t,
		strings.Split("a.b", "."),
		map[string]interface{}{"a": map[string]interface{}{"b": map[string]interface{}{"c": 10}}},
		50,
		false,
		map[string]interface{}{"a": map[string]interface{}{"b": map[string]interface{}{"c": 10}}},
	)

	testUpdateNestedMap(t,
		strings.Split("a.b.e", "."),
		map[string]interface{}{"a": map[string]interface{}{"b": map[string]interface{}{"c": 10}}},
		50,
		false,
		map[string]interface{}{"a": map[string]interface{}{"b": map[string]interface{}{"c": 10, "e": 50}}},
	)

	testUpdateNestedMap(t,
		strings.Split("a.b.c.d", "."),
		map[string]interface{}{"a": map[string]interface{}{"b": map[string]interface{}{"c": 10}}},
		50,
		false,
		map[string]interface{}{"a": map[string]interface{}{"b": map[string]interface{}{"c": 10}}},
	)

	testUpdateNestedMap(t,
		strings.Split("a.b.e.f.g", "."),
		map[string]interface{}{"a": map[string]interface{}{"b": map[string]interface{}{"c": 10}}},
		50,
		false,
		map[string]interface{}{"a": map[string]interface{}{"b": map[string]interface{}{"c": 10, "e": map[string]interface{}{"f": map[string]interface{}{"g": 50}}}}},
	)

	testUpdateNestedMap(t,
		strings.Split("a.b.e.f.g", "."),
		map[string]interface{}{},
		50,
		false,
		map[string]interface{}{"a": map[string]interface{}{"b": map[string]interface{}{"e": map[string]interface{}{"f": map[string]interface{}{"g": 50}}}}},
	)
}
func TestFlattenMap(t *testing.T){
	data := make([]interface{},0)
	data = append(data, map[string]interface{}{
		"key": "2022-02-24T05:00:00.000",
		"aggs": map[string]interface{}{
			"doc_count": map[string]interface{}{
				"value": 2,
			},
		},
	})
	data = append(data, map[string]interface{}{
		"key": "2022-02-24T08:00:00.000",
		"aggs": map[string]interface{}{
			"doc_count": map[string]interface{}{
				"value": 12,
			},
		},
	})
	testFlattenMap(t,
		data,
		"key",
		"aggs.doc_count.value",
		map[string]interface{}{
			"2022-02-24T05:00:00.000":2,
			"2022-02-24T08:00:00.000":12,
		},
	)

	testFlattenMap(t,
		data,
		"a",
		"aggs.doc_count.value",
		nil,
	)

	data = data[:1]
	testFlattenMap(t,
		data,
		"key",
		"aggs.doc_count.value",
		nil,
	)

	dataS := append(data, map[string]int{"a" : 12})
	testFlattenMap(t,
		dataS,
		"key",
		"aggs.doc_count.value",
		nil)

}


func TestDeleteNestedMap(t *testing.T){
		

	testDeleteNestedMap(t,
		strings.Split("a.b", "."),
		map[string]interface{}{"c": map[string]interface{}{"b":16, "a":"12"}},
		false,
		map[string]interface{}{"c": map[string]interface{}{"b":16, "a":"12"}},
	)

	testDeleteNestedMap(t,
		strings.Split("a.b", "."),
		map[string]interface{}{},
		false,
		map[string]interface{}{"c": map[string]interface{}{"b":16, "a":"12"}},
	)

	testDeleteNestedMap(t,
		strings.Split("a","."),
		map[string]interface{}{"a": 12},
		true,
		map[string]interface{}{},
	)

	testDeleteNestedMap(t,
		strings.Split("a.b.c.d", "."),
		map[string]interface{}{"a": map[string]interface{}{"b": map[string]interface{}{"c": 10}}},
		false,
		map[string]interface{}{"a": map[string]interface{}{"b": map[string]interface{}{"c": 10}}},
	)

	testDeleteNestedMap(t,
		strings.Split("a.b.e.f.g", "."),
		map[string]interface{}{"a": map[string]interface{}{"b": map[string]interface{}{"c": 10, "e": map[string]interface{}{"f": map[string]interface{}{"g": 50}}}}},
		true,
		map[string]interface{}{"a": map[string]interface{}{"b": map[string]interface{}{"c": 10, "e": map[string]interface{}{"f": map[string]interface{}{}}}}},
	)

	testDeleteNestedMap(t,
		strings.Split("a.b.e.f.g", "."),
		map[string]interface{}{},
		false,
		map[string]interface{}{"a": map[string]interface{}{"b": map[string]interface{}{"e": map[string]interface{}{"f": map[string]interface{}{"g": 50}}}}},
	)
}

func testDeleteNestedMap(t *testing.T, path []string, data map[string]interface{}, expected bool, expectedData map[string]interface{}){
	success := DeleteNestedMap(path, data)
	if success != expected {
		t.Log(success)
		t.Log(expected)
	}

	dataJSON, _ := json.Marshal(data)
	expectedDataJSON, _ := json.Marshal(expectedData)
	if string(dataJSON) != string(expectedDataJSON) {
		t.Log(string(dataJSON))
		t.Log(string(expectedDataJSON))
	}
}

func testSearchNestedMap(t *testing.T, path []string, data map[string]interface{}, expected bool, value interface{}) {
	val, found := LookupNestedMap(path, data)
	if found != expected {
		t.Error("Invalid found")
		t.FailNow()
	}
	if val != value {
		t.Error("invalid value")
		t.FailNow()
	}
}

func testUpdateNestedMap(t *testing.T, path []string, data map[string]interface{}, newValue interface{}, expected bool, expectedData map[string]interface{}) {
	success := PatchNestedMap(path, data, newValue)
	if success != expected {
		t.Log(success)
		t.Log(expected)
	}

	dataJSON, _ := json.Marshal(data)
	expectedDataJSON, _ := json.Marshal(expectedData)
	if string(dataJSON) != string(expectedDataJSON) {
		t.Log(string(dataJSON))
		t.Log(string(expectedDataJSON))
	}
}

func testFlattenMap(t *testing.T, data []interface{}, pathKey string, pathValue string, expectedData map[string]interface{}){
	flatMap := FlattenFact(data, pathKey, pathValue)
	dataJSON, _ := json.Marshal(flatMap)
	expectedDataJSON, _ := json.Marshal(expectedData)
	if string(dataJSON) != string(expectedDataJSON) {
		t.Log(string(dataJSON))
		t.Log(string(expectedDataJSON))
	}

}