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
