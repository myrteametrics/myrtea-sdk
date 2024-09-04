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

func TestAppendSlice(t *testing.T) {
	// Test error
	output, err := appendSlice()
	if err == nil {
		t.Errorf("Error should not be nil")
	}

	// Test with single arg
	output, err = appendSlice("test")
	if err != nil {
		t.Error(err)
	}
	if len(output) != 1 {
		t.Fatal("Output length not equal to 1")
	}
	if output[0] != "test" {
		t.Errorf("Output not equal to test")
	}

	// Test combine two args
	output, err = appendSlice("first", "test2")
	if err != nil {
		t.Error(err)
	}
	if len(output) != 2 {
		t.Fatal("Output length not equal to 2")
	}
	if output[0] != "first" {
		t.Errorf("Output not equal to first")
	}
	if output[1] != "test2" {
		t.Errorf("Output not equal to test2")
	}

	// Test combine slice with value
	output, err = appendSlice([]interface{}{"first", "first2"}, "second")
	if err != nil {
		t.Error(err)
	}
	if len(output) != 3 {
		t.Fatal("Output length not equal to 3")
	}
	if output[0] != "first" {
		t.Errorf("Output not equal to first")
	}
	if output[1] != "first2" {
		t.Errorf("Output not equal to first2")
	}
	if output[2] != "second" {
		t.Errorf("Output not equal to second")
	}

	// Test combine slice with slice
	output, err = appendSlice([]interface{}{1, 2}, []interface{}{"3", "4"})
	if err != nil {
		t.Error(err)
	}
	if len(output) != 4 {
		t.Fatal("Output length not equal to 4")
	}
	if output[0] != 1 {
		t.Errorf("Output not equal to 1")
	}
	if output[1] != 2 {
		t.Errorf("Output not equal to 2")
	}
	if output[2] != "3" {
		t.Errorf("Output not equal to 3")
	}
	if output[3] != "4" {
		t.Errorf("Output not equal to 4")
	}
}
