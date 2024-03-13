package connector

import (
	"github.com/myrteametrics/myrtea-sdk/v4/expression"
	"testing"
)

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

	t.Log(LookupNestedMap([]string{"a", "e", "c", "[0]", "f"}, data))
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

	t.Log(LookupNestedMap([]string{"a", "*", "f"}, data))

	val, ok := LookupNestedMap([]string{"a", "*", "f"}, data)

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

	t.Log(LookupNestedMap([]string{"a", "*", "c", "[0]", "f"}, data))

	val, ok := LookupNestedMap([]string{"a", "*", "c", "[0]", "f"}, data)

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

	val, ok = LookupNestedMap([]string{"a", "*", "c", "[1]", "f"}, data)

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

	val, ok := LookupNestedMapFullPaths(data, [][]string{{"a", "*", "c", "[0]", "f"}, {"a", "*", "c", "[1]", "f"}}, "")

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
