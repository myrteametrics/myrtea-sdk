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
}

func TestFilter(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected []interface{}
		wantErr  bool
	}{
		{
			name:     "filter single value",
			args:     []interface{}{[]interface{}{"a", "b", "c"}, "b"},
			expected: []interface{}{"b"},
			wantErr:  false,
		},
		{
			name:     "filter multiple values",
			args:     []interface{}{[]interface{}{"a", "b", "c"}, "a", "c"},
			expected: []interface{}{"a", "c"},
			wantErr:  false,
		},
		{
			name:     "filter no matches",
			args:     []interface{}{[]interface{}{"a", "b", "c"}, "d"},
			expected: []interface{}{},
			wantErr:  false,
		},
		{
			name:     "filter all values",
			args:     []interface{}{[]interface{}{"a", "b", "c"}, "a", "b", "c"},
			expected: []interface{}{"a", "b", "c"},
			wantErr:  false,
		},
		{
			name:     "filter empty array",
			args:     []interface{}{[]interface{}{}, "a"},
			expected: []interface{}{},
			wantErr:  false,
		},
		{
			name:     "filter with numbers",
			args:     []interface{}{[]interface{}{1, 2, 3}, 2},
			expected: []interface{}{2},
			wantErr:  false,
		},
		{
			name:    "filter error - not enough args",
			args:    []interface{}{[]interface{}{"a", "b"}},
			wantErr: true,
		},
		{
			name:    "filter error - first arg not array",
			args:    []interface{}{"not an array", "b"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output, err := filter(tt.args...)
			if tt.wantErr {
				if err == nil {
					t.Errorf("filter() expected error but got none")
				}
				return
			}
			if err != nil {
				t.Errorf("filter() unexpected error: %v", err)
				return
			}
			if len(output) != len(tt.expected) {
				t.Errorf("filter() length = %v, want %v", len(output), len(tt.expected))
				return
			}
			for i, v := range output {
				if v != tt.expected[i] {
					t.Errorf("filter() output[%d] = %v, want %v", i, v, tt.expected[i])
				}
			}
		})
	}
}

func TestSortSlice(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected []interface{}
		wantErr  bool
	}{
		// Ascending tests
		{
			name:     "sort numbers ascending",
			args:     []interface{}{[]interface{}{3, 1, 2}, "asc"},
			expected: []interface{}{1, 2, 3},
			wantErr:  false,
		},
		{
			name:     "sort strings ascending",
			args:     []interface{}{[]interface{}{"c", "a", "b"}, "asc"},
			expected: []interface{}{"a", "b", "c"},
			wantErr:  false,
		},
		{
			name:     "sort mixed numeric types ascending",
			args:     []interface{}{[]interface{}{3.5, 1, 2.2, 0.5}, "asc"},
			expected: []interface{}{0.5, 1, 2.2, 3.5},
			wantErr:  false,
		},
		{
			name:     "sort negative numbers ascending",
			args:     []interface{}{[]interface{}{-1, -5, 0, 3, -2}, "asc"},
			expected: []interface{}{-5, -2, -1, 0, 3},
			wantErr:  false,
		},
		// Descending tests
		{
			name:     "sort numbers descending",
			args:     []interface{}{[]interface{}{1, 2, 3}, "desc"},
			expected: []interface{}{3, 2, 1},
			wantErr:  false,
		},
		{
			name:     "sort strings descending",
			args:     []interface{}{[]interface{}{"a", "b", "c"}, "desc"},
			expected: []interface{}{"c", "b", "a"},
			wantErr:  false,
		},
		{
			name:     "sort mixed numeric types descending",
			args:     []interface{}{[]interface{}{0.5, 2.2, 1, 3.5}, "desc"},
			expected: []interface{}{3.5, 2.2, 1, 0.5},
			wantErr:  false,
		},
		{
			name:     "sort negative numbers descending",
			args:     []interface{}{[]interface{}{-1, -5, 0, 3, -2}, "desc"},
			expected: []interface{}{3, 0, -1, -2, -5},
			wantErr:  false,
		},
		// Default order (ascending when not specified)
		{
			name:     "sort without order parameter defaults to asc",
			args:     []interface{}{[]interface{}{3, 1, 2}},
			expected: []interface{}{1, 2, 3},
			wantErr:  false,
		},
		{
			name:     "sort strings without order defaults to asc",
			args:     []interface{}{[]interface{}{"c", "a", "b"}},
			expected: []interface{}{"a", "b", "c"},
			wantErr:  false,
		},
		// Edge cases
		{
			name:     "sort single element",
			args:     []interface{}{[]interface{}{42}, "asc"},
			expected: []interface{}{42},
			wantErr:  false,
		},
		{
			name:     "sort empty array",
			args:     []interface{}{[]interface{}{}, "asc"},
			expected: []interface{}{},
			wantErr:  false,
		},
		{
			name:     "sort already sorted ascending",
			args:     []interface{}{[]interface{}{1, 2, 3}, "asc"},
			expected: []interface{}{1, 2, 3},
			wantErr:  false,
		},
		{
			name:     "sort already sorted descending",
			args:     []interface{}{[]interface{}{3, 2, 1}, "desc"},
			expected: []interface{}{3, 2, 1},
			wantErr:  false,
		},
		// Error cases
		{
			name:    "error - mixed types",
			args:    []interface{}{[]interface{}{"a", 1, "b"}, "asc"},
			wantErr: true,
		},
		{
			name:    "error - no arguments",
			args:    []interface{}{},
			wantErr: true,
		},
		{
			name:    "error - too many arguments",
			args:    []interface{}{[]interface{}{1, 2}, "asc", "extra"},
			wantErr: true,
		},
		{
			name:    "error - not an array",
			args:    []interface{}{"not an array", "asc"},
			wantErr: true,
		},
		{
			name:    "error - invalid order",
			args:    []interface{}{[]interface{}{1, 2, 3}, "invalid"},
			wantErr: true,
		},
		{
			name:    "error - order not a string",
			args:    []interface{}{[]interface{}{1, 2, 3}, 123},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output, err := sortSlice(tt.args...)
			if tt.wantErr {
				if err == nil {
					t.Errorf("sort() expected error but got none")
				}
				return
			}
			if err != nil {
				t.Errorf("sort() unexpected error: %v", err)
				return
			}
			if len(output) != len(tt.expected) {
				t.Errorf("sort() length = %v, want %v", len(output), len(tt.expected))
				return
			}
			for i, v := range output {
				// Compare numeric values as floats to handle type differences
				vNum, vIsNum := toFloat64(v)
				expNum, expIsNum := toFloat64(tt.expected[i])
				if vIsNum && expIsNum {
					if vNum != expNum {
						t.Errorf("sort() output[%d] = %v, want %v", i, v, tt.expected[i])
					}
				} else if v != tt.expected[i] {
					t.Errorf("sort() output[%d] = %v, want %v", i, v, tt.expected[i])
				}
			}
		})
	}
}

func TestJoin(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected string
		wantErr  bool
	}{
		{
			name:     "join strings with comma",
			args:     []interface{}{[]interface{}{"a", "b", "c"}, ", "},
			expected: "a, b, c",
			wantErr:  false,
		},
		{
			name:     "join numbers with dash",
			args:     []interface{}{[]interface{}{1, 2, 3}, "-"},
			expected: "1-2-3",
			wantErr:  false,
		},
		{
			name:     "join with empty separator",
			args:     []interface{}{[]interface{}{"a", "b", "c"}, ""},
			expected: "abc",
			wantErr:  false,
		},
		{
			name:     "join single element",
			args:     []interface{}{[]interface{}{"only"}, ", "},
			expected: "only",
			wantErr:  false,
		},
		{
			name:     "join empty array",
			args:     []interface{}{[]interface{}{}, ", "},
			expected: "",
			wantErr:  false,
		},
		{
			name:     "join mixed types",
			args:     []interface{}{[]interface{}{1, "a", 2.5, true}, " | "},
			expected: "1 | a | 2.5 | true",
			wantErr:  false,
		},
		{
			name:     "join with pipe separator",
			args:     []interface{}{[]interface{}{"red", "green", "blue"}, "|"},
			expected: "red|green|blue",
			wantErr:  false,
		},
		{
			name:     "join with space",
			args:     []interface{}{[]interface{}{"hello", "world"}, " "},
			expected: "hello world",
			wantErr:  false,
		},
		{
			name:    "error - no arguments",
			args:    []interface{}{},
			wantErr: true,
		},
		{
			name:    "error - only one argument",
			args:    []interface{}{[]interface{}{"a", "b"}},
			wantErr: true,
		},
		{
			name:    "error - too many arguments",
			args:    []interface{}{[]interface{}{"a"}, ",", "extra"},
			wantErr: true,
		},
		{
			name:    "error - first arg not array",
			args:    []interface{}{"not an array", ","},
			wantErr: true,
		},
		{
			name:    "error - second arg not string",
			args:    []interface{}{[]interface{}{"a", "b"}, 42},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output, err := join(tt.args...)
			if tt.wantErr {
				if err == nil {
					t.Errorf("join() expected error but got none")
				}
				return
			}
			if err != nil {
				t.Errorf("join() unexpected error: %v", err)
				return
			}
			if output != tt.expected {
				t.Errorf("join() output = %v, want %v", output, tt.expected)
			}
		})
	}
}

func TestExclude(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected []interface{}
		wantErr  bool
	}{
		{
			name:     "exclude single value",
			args:     []interface{}{[]interface{}{"a", "b", "c"}, "b"},
			expected: []interface{}{"a", "c"},
			wantErr:  false,
		},
		{
			name:     "exclude multiple values",
			args:     []interface{}{[]interface{}{"a", "b", "c"}, "a", "c"},
			expected: []interface{}{"b"},
			wantErr:  false,
		},
		{
			name:     "exclude no matches",
			args:     []interface{}{[]interface{}{"a", "b", "c"}, "d"},
			expected: []interface{}{"a", "b", "c"},
			wantErr:  false,
		},
		{
			name:     "exclude all values",
			args:     []interface{}{[]interface{}{"a", "b", "c"}, "a", "b", "c"},
			expected: []interface{}{},
			wantErr:  false,
		},
		{
			name:     "exclude empty array",
			args:     []interface{}{[]interface{}{}, "a"},
			expected: []interface{}{},
			wantErr:  false,
		},
		{
			name:     "exclude with numbers",
			args:     []interface{}{[]interface{}{1, 2, 3}, 2},
			expected: []interface{}{1, 3},
			wantErr:  false,
		},
		{
			name:    "exclude error - not enough args",
			args:    []interface{}{[]interface{}{"a", "b"}},
			wantErr: true,
		},
		{
			name:    "exclude error - first arg not array",
			args:    []interface{}{"not an array", "b"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output, err := exclude(tt.args...)
			if tt.wantErr {
				if err == nil {
					t.Errorf("exclude() expected error but got none")
				}
				return
			}
			if err != nil {
				t.Errorf("exclude() unexpected error: %v", err)
				return
			}
			if len(output) != len(tt.expected) {
				t.Errorf("exclude() length = %v, want %v", len(output), len(tt.expected))
				return
			}
			for i, v := range output {
				if v != tt.expected[i] {
					t.Errorf("exclude() output[%d] = %v, want %v", i, v, tt.expected[i])
				}
			}
		})
	}
}
