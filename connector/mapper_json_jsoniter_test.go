package connector

import "testing"

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
}
