package expression

import "testing"

// Usage: <[]value> <value>
func TestContains(t *testing.T) {

	tests := []struct {
		arg1     []interface{}
		arg2     string
		expected bool
	}{
		{arg1: []interface{}{"a"}, arg2: "a", expected: true},
		{arg1: []interface{}{"a", "b", "c"}, arg2: "b", expected: true},
		{arg1: []interface{}{"a", "b", "c"}, arg2: "d", expected: false},
		{arg1: []interface{}{}, arg2: "d", expected: false},
		{arg1: []interface{}{"ab", "ba"}, arg2: "a", expected: false},
		{arg1: []interface{}{"a", "b"}, arg2: "", expected: false},
	}
	for _, test := range tests {
		if output, err := contains(test.arg1, test.arg2); err != nil {
			t.Error(err)
		} else if output != test.expected {
			t.Errorf("Output %t not equal to expected %t", output, test.expected)
		}
	}
}


func TestFetchValueByMatchingAttribute(t *testing.T) {
	tests := []struct {
		arguments []interface{}
		expected  int
		err       string
	}{
		{
			arguments: []interface{}{[]map[string]interface{}{{"a": "b", "c": 1}}, "a", "b", "c"},
			expected:  1,
			err:       "",
		},
		{
			arguments: []interface{}{[]map[string]interface{}{{"a": "b", "c": 1}, {"a": "c", "c": 2}}, "a", "b", "c"},
			expected:  1,
			err:       "",
		},
		{
			arguments: []interface{}{[]map[string]interface{}{{"a": "b", "c": 1}, {"a": "b", "c": 2}}, "a", "b", "c"},
			expected:  1,
			err:       "",
		},
		{
			arguments: []interface{}{[]map[string]interface{}{{"a": "b", "c": 1}}, "a", "c", "c"},
			expected:  0,
			err:       "",
		},
		{
			arguments: []interface{}{[]map[string]interface{}{{"a": "b", "c": 1}}, "c", "b", "c"},
			expected:  0,
			err:       "",
		},
		{
			arguments: []interface{}{[]map[string]interface{}{{"a": "b", "c": 1}}, "a", "c"},
			expected:  0,
			err:       "findByAttribute() expects exactly four arguments",
		},
		{
			arguments: []interface{}{[]map[string]interface{}{{"a": "b", "c": 1}}, 1, "b", "c"},
			expected:  0,
			err:       "findByAttribute() expects second argument to be a string",
		},
		{
			arguments: []interface{}{"not a slice of map", "a", "b", "c"},
			expected:  0,
			err:       "findByAttribute() expects first argument to be a slice of map[string]interface{}",
		},
		{
			arguments: []interface{}{[]map[string]interface{}{{"a": "b", "c": "not an int"}}, "a", "b", "c"},
			expected:  0,
			err:       "value of c is not int",
		},
	}
	for _, test := range tests {
		output, err := fetchValueByMatchingAttribute(test.arguments...)
		var errMsg string
		if err != nil {
			errMsg = err.Error() 
		}
		if errMsg != test.err { 
			t.Errorf("Expected error %v, got %v", test.err, errMsg)
		} else if output != test.expected {
			t.Errorf("Output %d not equal to expected %d", output, test.expected)
		}
	}
}
