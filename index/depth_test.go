package index

import (
	"github.com/myrteametrics/myrtea-sdk/v5/expression"
	"testing"
)

func TestDepth_String(t *testing.T) {
	tests := []struct {
		name     string
		depth    Depth
		expected string
	}{
		{"test1", All, "search"},
		{"test2", Patch, "Patch"},
		{"test3", Last, "current"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.depth.String(); got != tt.expected {
				t.Errorf("Depth.String() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestDepth_MarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		depth    Depth
		expected string
	}{
		{"test1", All, `"search"`},
		{"test2", Patch, `"Patch"`},
		{"test3", Last, `"current"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, err := tt.depth.MarshalJSON(); err != nil || string(got) != tt.expected {
				t.Errorf("Depth.MarshalJSON() = %v, %v, want %v", string(got), err, tt.expected)
			}
		})
	}
}

func TestDepth_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected Depth
	}{
		{"test1", `"search"`, All},
		{"test2", `"Patch"`, Patch},
		{"test3", `"current"`, Last},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var d Depth
			if err := d.UnmarshalJSON([]byte(tt.input)); err != nil || d != tt.expected {
				t.Errorf("Depth.UnmarshalJSON() = %v, %v, want %v", d, err, tt.expected)
			}
		})
	}
}

func TestDepth_UnmarshalJSON_Error(t *testing.T) {
	var d Depth
	err := d.UnmarshalJSON([]byte(`"invalid""`))
	expression.AssertNotEqual(t, nil, err, "Error should not be nil")
}
