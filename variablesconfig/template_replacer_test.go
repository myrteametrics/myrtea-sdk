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
		{
			name: "no key",
			input: map[string]string{
				"lien_adel": "https://exemple",
			},
			vars: map[string]string{
				"keyUnique": "url.exemple.eu/bin/view/Main/H24/FLUX/FLUX%20CLP/%F0%9F%93%82%20MYRTEA/",
			},
			expected: map[string]string{
				"lien_adel": "https://exemple",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ReplaceKeysWithValues(&tt.input, tt.vars)
			if !reflect.DeepEqual(tt.input, tt.expected) {
				t.Errorf("replaceKeysWithValues() = %v, want %v", tt.input, tt.expected)
			}
		})
	}
}


func TestReplaceKeysWithValuesAllPattern(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]string
		vars     map[string]string
		expected map[string]string
	}{
		{
			name: "format {{key}}+ replacement",
			input: map[string]string{
				"lien_adel": "https://{{keyUnique}}+url",
			},
			vars: map[string]string{
				"keyUnique": "url.exemple.eu",
			},
			expected: map[string]string{
				"lien_adel": "https://url.exemple.euurl",
			},
		},
		{
			name: "format +{{key}} replacement",
			input: map[string]string{
				"lien_adel": "https://+{{keyUnique}}url",
			},
			vars: map[string]string{
				"keyUnique": "url.exemple.eu",
			},
			expected: map[string]string{
				"lien_adel": "https://url.exemple.euurl",
			},
		},
		{
			name: "format {{key}} replacement",
			input: map[string]string{
				"lien_adel": "https://{{keyUnique}}url",
			},
			vars: map[string]string{
				"keyUnique": "url.exemple.eu",
			},
			expected: map[string]string{
				"lien_adel": "https://url.exemple.euurl",
			},
		},
		{
			name: "multiple formats replacement",
			input: map[string]string{
				"lien_adel": "https://{{key1}}+url+{{key2}}domain+{{key3}}",
			},
			vars: map[string]string{
				"key1": "firstPart",
				"key2": "secondPart",
				"key3": "thirdPart",
			},
			expected: map[string]string{
				"lien_adel": "https://firstParturlsecondPartdomainthirdPart",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ReplaceKeysWithValues(&tt.input, tt.vars)
			if !reflect.DeepEqual(tt.input, tt.expected) {
				t.Errorf("replaceKeysWithValues() = %v, want %v", tt.input, tt)
			}
		})
	}
}