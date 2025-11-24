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
