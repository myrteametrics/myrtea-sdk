package variablesconfig

import (
	"reflect"
	"testing"
)

func TestReplaceKeysWithValues(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]string
		vars     map[string]string
		expected map[string]string
	}{
		{
			name: "basic replacement",
			input: map[string]string{
				"lien_adel": "https://+{{ keyUnique }}+url",
			},
			vars: map[string]string{
				"keyUnique": "url.exemple.eu/bin/view/Main/H24/FLUX/FLUX%20CLP/%F0%9F%93%82%20MYRTEA/",
			},
			expected: map[string]string{
				"lien_adel": "https://url.exemple.eu/bin/view/Main/H24/FLUX/FLUX%20CLP/%F0%9F%93%82%20MYRTEA/url",
			},
		},
		{
			name: "no replacement",
			input: map[string]string{
				"lien_adel": "https://+{{ keyMissing }}+",
			},
			vars: map[string]string{
				"keyUnique": "url.exemple.eu/bin/view/Main/H24/FLUX/FLUX%20CLP/%F0%9F%93%82%20MYRTEA/",
			},
			expected: map[string]string{
				"lien_adel": "https://+{{keyMissing}}+",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := replaceKeysWithValues(tt.input, tt.vars)
			if !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("replaceKeysWithValues() = %v, want %v", got, tt.expected)
			}
		})
	}
}
