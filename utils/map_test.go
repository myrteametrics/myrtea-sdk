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

	// Array access tests
	testSearchNestedMap(t,
		strings.Split("pairs[0]", "."),
		map[string]interface{}{"pairs": []interface{}{10, 20, 30}},
		true,
		10,
	)

	testSearchNestedMap(t,
		strings.Split("pairs[1]", "."),
		map[string]interface{}{"pairs": []interface{}{10, 20, 30}},
		true,
		20,
	)

	testSearchNestedMap(t,
		strings.Split("test[0].test", "."),
		map[string]interface{}{"test": []interface{}{
			map[string]interface{}{"test": "value1"},
			map[string]interface{}{"test": "value2"},
		}},
		true,
		"value1",
	)

	testSearchNestedMap(t,
		strings.Split("test[1].test", "."),
		map[string]interface{}{"test": []interface{}{
			map[string]interface{}{"test": "value1"},
			map[string]interface{}{"test": "value2"},
		}},
		true,
		"value2",
	)

	testSearchNestedMap(t,
		strings.Split("a.b[2].c", "."),
		map[string]interface{}{"a": map[string]interface{}{"b": []interface{}{
			map[string]interface{}{"c": 1},
			map[string]interface{}{"c": 2},
			map[string]interface{}{"c": 3},
		}}},
		true,
		3,
	)

	// Array out of bounds
	testSearchNestedMap(t,
		strings.Split("pairs[5]", "."),
		map[string]interface{}{"pairs": []interface{}{10, 20, 30}},
		false,
		nil,
	)

	// Array negative index
	testSearchNestedMap(t,
		strings.Split("pairs[-1]", "."),
		map[string]interface{}{"pairs": []interface{}{10, 20, 30}},
		false,
		nil,
	)

	// Not an array
	testSearchNestedMap(t,
		strings.Split("test[0]", "."),
		map[string]interface{}{"test": "not an array"},
		false,
		nil,
	)
}

func TestLookupNestedMapValue(t *testing.T) {
	// Basic nested lookup (same as LookupNestedMap for non-map values)
	testLookupNestedMapValue(t,
		strings.Split("a.b", "."),
		map[string]interface{}{"a": map[string]interface{}{"b": 10}},
		true,
		10,
	)

	testLookupNestedMapValue(t,
		strings.Split("a.b.c.d.e", "."),
		map[string]interface{}{"a": map[string]interface{}{"b": map[string]interface{}{"c": map[string]interface{}{"d": map[string]interface{}{"e": 10}}}}},
		true,
		10,
	)

	// Key difference: LookupNestedMapValue SHOULD return map values
	testLookupNestedMapValue(t,
		strings.Split("a.b", "."),
		map[string]interface{}{"a": map[string]interface{}{"b": map[string]interface{}{"c": 10}}},
		true,
		map[string]interface{}{"c": 10},
	)

	// Another map value test
	testLookupNestedMapValue(t,
		strings.Split("a", "."),
		map[string]interface{}{"a": map[string]interface{}{"b": 20, "c": 30}},
		true,
		map[string]interface{}{"b": 20, "c": 30},
	)

	// Non-existent path
	testLookupNestedMapValue(t,
		strings.Split("a.b.c.d", "."),
		map[string]interface{}{"a": map[string]interface{}{"b": map[string]interface{}{"c": 10}}},
		false,
		nil,
	)

	// Array access tests
	testLookupNestedMapValue(t,
		strings.Split("pairs[0]", "."),
		map[string]interface{}{"pairs": []interface{}{10, 20, 30}},
		true,
		10,
	)

	testLookupNestedMapValue(t,
		strings.Split("pairs[1]", "."),
		map[string]interface{}{"pairs": []interface{}{10, 20, 30}},
		true,
		20,
	)

	testLookupNestedMapValue(t,
		strings.Split("test[0].test", "."),
		map[string]interface{}{"test": []interface{}{
			map[string]interface{}{"test": "value1"},
			map[string]interface{}{"test": "value2"},
		}},
		true,
		"value1",
	)

	testLookupNestedMapValue(t,
		strings.Split("test[1].test", "."),
		map[string]interface{}{"test": []interface{}{
			map[string]interface{}{"test": "value1"},
			map[string]interface{}{"test": "value2"},
		}},
		true,
		"value2",
	)

	testLookupNestedMapValue(t,
		strings.Split("a.b[2].c", "."),
		map[string]interface{}{"a": map[string]interface{}{"b": []interface{}{
			map[string]interface{}{"c": 1},
			map[string]interface{}{"c": 2},
			map[string]interface{}{"c": 3},
		}}},
		true,
		3,
	)

	// Key difference: Array element that is a map should be returned
	testLookupNestedMapValue(t,
		strings.Split("test[0]", "."),
		map[string]interface{}{"test": []interface{}{
			map[string]interface{}{"nested": "value"},
			map[string]interface{}{"other": "data"},
		}},
		true,
		map[string]interface{}{"nested": "value"},
	)

	testLookupNestedMapValue(t,
		strings.Split("test[1]", "."),
		map[string]interface{}{"test": []interface{}{
			map[string]interface{}{"nested": "value"},
			map[string]interface{}{"other": "data"},
		}},
		true,
		map[string]interface{}{"other": "data"},
	)

	// Array out of bounds
	testLookupNestedMapValue(t,
		strings.Split("pairs[5]", "."),
		map[string]interface{}{"pairs": []interface{}{10, 20, 30}},
		false,
		nil,
	)

	// Array negative index
	testLookupNestedMapValue(t,
		strings.Split("pairs[-1]", "."),
		map[string]interface{}{"pairs": []interface{}{10, 20, 30}},
		false,
		nil,
	)

	// Not an array
	testLookupNestedMapValue(t,
		strings.Split("test[0]", "."),
		map[string]interface{}{"test": "not an array"},
		false,
		nil,
	)

	// Complex nested structure with array containing map
	testLookupNestedMapValue(t,
		strings.Split("root.items[1]", "."),
		map[string]interface{}{"root": map[string]interface{}{"items": []interface{}{
			map[string]interface{}{"id": 1, "name": "first"},
			map[string]interface{}{"id": 2, "name": "second"},
		}}},
		true,
		map[string]interface{}{"id": 2, "name": "second"},
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

	// Array update tests
	testUpdateNestedMap(t,
		strings.Split("pairs[1]", "."),
		map[string]interface{}{"pairs": []interface{}{10, 20, 30}},
		99,
		true,
		map[string]interface{}{"pairs": []interface{}{10, 99, 30}},
	)

	testUpdateNestedMap(t,
		strings.Split("test[0].value", "."),
		map[string]interface{}{"test": []interface{}{
			map[string]interface{}{"value": 100},
			map[string]interface{}{"value": 200},
		}},
		999,
		true,
		map[string]interface{}{"test": []interface{}{
			map[string]interface{}{"value": 999},
			map[string]interface{}{"value": 200},
		}},
	)

	testUpdateNestedMap(t,
		strings.Split("a.items[2].name", "."),
		map[string]interface{}{"a": map[string]interface{}{"items": []interface{}{
			map[string]interface{}{"name": "first"},
			map[string]interface{}{"name": "second"},
			map[string]interface{}{"name": "third"},
		}}},
		"updated",
		true,
		map[string]interface{}{"a": map[string]interface{}{"items": []interface{}{
			map[string]interface{}{"name": "first"},
			map[string]interface{}{"name": "second"},
			map[string]interface{}{"name": "updated"},
		}}},
	)

	// Array out of bounds
	testUpdateNestedMap(t,
		strings.Split("pairs[5]", "."),
		map[string]interface{}{"pairs": []interface{}{10, 20, 30}},
		99,
		false,
		map[string]interface{}{"pairs": []interface{}{10, 20, 30}},
	)

	// Not an array
	testUpdateNestedMap(t,
		strings.Split("test[0]", "."),
		map[string]interface{}{"test": "not an array"},
		99,
		false,
		map[string]interface{}{"test": "not an array"},
	)

	// Array element is a map - should now succeed in replacing it
	testUpdateNestedMap(t,
		strings.Split("test[0]", "."),
		map[string]interface{}{"test": []interface{}{
			map[string]interface{}{"nested": "value"},
		}},
		map[string]interface{}{"replaced": "object"},
		true,
		map[string]interface{}{"test": []interface{}{
			map[string]interface{}{"replaced": "object"},
		}},
	)
}
func TestFlattenMap(t *testing.T) {
	data := make([]interface{}, 0)
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
			"2022-02-24T05:00:00.000": 2,
			"2022-02-24T08:00:00.000": 12,
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

	dataS := append(data, map[string]int{"a": 12})
	testFlattenMap(t,
		dataS,
		"key",
		"aggs.doc_count.value",
		nil)

}

func TestDeleteNestedMap(t *testing.T) {

	testDeleteNestedMap(t,
		strings.Split("a.b", "."),
		map[string]interface{}{"c": map[string]interface{}{"b": 16, "a": "12"}},
		false,
		map[string]interface{}{"c": map[string]interface{}{"b": 16, "a": "12"}},
	)

	testDeleteNestedMap(t,
		strings.Split("a.b", "."),
		map[string]interface{}{},
		false,
		map[string]interface{}{"c": map[string]interface{}{"b": 16, "a": "12"}},
	)

	testDeleteNestedMap(t,
		strings.Split("a", "."),
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

	// Array deletion tests
	testDeleteNestedMap(t,
		strings.Split("pairs[1]", "."),
		map[string]interface{}{"pairs": []interface{}{10, 20, 30}},
		true,
		map[string]interface{}{"pairs": []interface{}{10, 30}},
	)

	testDeleteNestedMap(t,
		strings.Split("pairs[0]", "."),
		map[string]interface{}{"pairs": []interface{}{10, 20, 30}},
		true,
		map[string]interface{}{"pairs": []interface{}{20, 30}},
	)

	testDeleteNestedMap(t,
		strings.Split("test[1].value", "."),
		map[string]interface{}{"test": []interface{}{
			map[string]interface{}{"value": 100, "other": "data"},
			map[string]interface{}{"value": 200, "other": "info"},
		}},
		true,
		map[string]interface{}{"test": []interface{}{
			map[string]interface{}{"value": 100, "other": "data"},
			map[string]interface{}{"other": "info"},
		}},
	)

	testDeleteNestedMap(t,
		strings.Split("a.items[0].name", "."),
		map[string]interface{}{"a": map[string]interface{}{"items": []interface{}{
			map[string]interface{}{"name": "first", "id": 1},
			map[string]interface{}{"name": "second", "id": 2},
		}}},
		true,
		map[string]interface{}{"a": map[string]interface{}{"items": []interface{}{
			map[string]interface{}{"id": 1},
			map[string]interface{}{"name": "second", "id": 2},
		}}},
	)

	// Array out of bounds
	testDeleteNestedMap(t,
		strings.Split("pairs[5]", "."),
		map[string]interface{}{"pairs": []interface{}{10, 20, 30}},
		false,
		map[string]interface{}{"pairs": []interface{}{10, 20, 30}},
	)

	// Not an array
	testDeleteNestedMap(t,
		strings.Split("test[0]", "."),
		map[string]interface{}{"test": "not an array"},
		false,
		map[string]interface{}{"test": "not an array"},
	)
}

func testDeleteNestedMap(t *testing.T, path []string, data map[string]interface{}, expected bool, expectedData map[string]interface{}) {
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

func testLookupNestedMapValue(t *testing.T, path []string, data map[string]interface{}, expected bool, value interface{}) {
	val, found := LookupNestedMapValue(path, data)
	if found != expected {
		t.Errorf("LookupNestedMapValue(%v) found = %v, expected %v", path, found, expected)
		t.FailNow()
	}

	// For map comparisons, we need to use JSON marshaling
	if expected && val != nil && value != nil {
		valJSON, _ := json.Marshal(val)
		expectedJSON, _ := json.Marshal(value)
		if string(valJSON) != string(expectedJSON) {
			t.Errorf("LookupNestedMapValue(%v) value = %s, expected %s", path, string(valJSON), string(expectedJSON))
			t.FailNow()
		}
	} else if val != value {
		t.Errorf("LookupNestedMapValue(%v) value = %v, expected %v", path, val, value)
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

func testFlattenMap(t *testing.T, data []interface{}, pathKey string, pathValue string, expectedData map[string]interface{}) {
	flatMap := FlattenFact(data, pathKey, pathValue)
	dataJSON, _ := json.Marshal(flatMap)
	expectedDataJSON, _ := json.Marshal(expectedData)
	if string(dataJSON) != string(expectedDataJSON) {
		t.Log(string(dataJSON))
		t.Log(string(expectedDataJSON))
	}

}

func BenchmarkLookupNestedMapRecursive(b *testing.B) {
	// Set up a sample nested map for testing
	nestedMap := map[string]interface{}{
		"key1": map[string]interface{}{
			"key2": map[string]interface{}{
				"key3": "value",
			},
		},
	}

	// Run the function and measure its performance
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = LookupNestedMap([]string{"key1", "key2", "key3"}, nestedMap)
	}
}
