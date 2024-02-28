package index

import "testing"

func TestBuildAliasName(t *testing.T) {
	tests := []struct {
		name     string
		alias    string
		depth    Depth
		expected string
	}{
		{"test1", "alias1", All, "test1-alias1-search"},
		{"test2", "alias2", Patch, "test2-alias2-Patch"},
		{"test3", "alias3", Last, "test3-alias3-current"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := BuildAliasName(tt.name, tt.alias, tt.depth); got != tt.expected {
				t.Errorf("BuildAliasName() = %v, want %v", got, tt.expected)
			}
		})
	}
}
