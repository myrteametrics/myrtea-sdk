package connector

import (
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
)

func TestGetHeader(t *testing.T) {
	headers := []*sarama.RecordHeader{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
	}

	tests := []struct {
		name     string
		key      string
		expected string
		found    bool
	}{
		{"Header exists", "key1", "value1", true},
		{"Header does not exist", "key3", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, found := GetHeader(tt.key, headers)
			assert.Equal(t, tt.expected, value)
			assert.Equal(t, tt.found, found)
		})
	}
}

func TestFilterHeaders(t *testing.T) {
	headers := []*sarama.RecordHeader{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
	}

	tests := []struct {
		name         string
		filters      []FilterHeaderOption
		expectedPass bool
		expectedMsg  string
	}{
		{
			name: "Condition exists, header exists",
			filters: []FilterHeaderOption{
				{Key: "key1", Condition: "exists"},
			},
			expectedPass: true,
			expectedMsg:  "",
		},
		{
			name: "Condition exists, header does not exist",
			filters: []FilterHeaderOption{
				{Key: "key3", Condition: "exists"},
			},
			expectedPass: false,
			expectedMsg:  "key_not_found not match with Condition=exists, Value=",
		},
		{
			name: "Condition equals, matches value",
			filters: []FilterHeaderOption{
				{Key: "key1", Condition: "equals", Value: "value1"},
			},
			expectedPass: true,
			expectedMsg:  "",
		},
		{
			name: "Condition equals, value mismatch",
			filters: []FilterHeaderOption{
				{Key: "key1", Condition: "equals", Value: "valueX"},
			},
			expectedPass: false,
			expectedMsg:  "value1 not match with Condition=equals, Value=valueX",
		},
		{
			name: "Condition equals_atleastone, value matches",
			filters: []FilterHeaderOption{
				{Key: "key1", Condition: "equals_atleastone", Values: []string{"valueX", "value1"}},
			},
			expectedPass: true,
			expectedMsg:  "",
		},
		{
			name: "Condition equals_atleastone, no matches",
			filters: []FilterHeaderOption{
				{Key: "key1", Condition: "equals_atleastone", Values: []string{"valueX", "valueY"}},
			},
			expectedPass: false,
			expectedMsg:  "value1 not match with Condition=equals_atleastone, Value=[valueX valueY]",
		},
		{
			name: "Condition contains, value contains substring",
			filters: []FilterHeaderOption{
				{Key: "key1", Condition: "contains", Value: "val"},
			},
			expectedPass: true,
			expectedMsg:  "",
		},
		{
			name: "Condition contains, value does not contain substring",
			filters: []FilterHeaderOption{
				{Key: "key1", Condition: "contains", Value: "not_present"},
			},
			expectedPass: false,
			expectedMsg:  "value1 not match with Condition=contains, Value=not_present",
		},
		{
			name: "Condition startWith, matches",
			filters: []FilterHeaderOption{
				{Key: "key1", Condition: "startWith", Value: "val"},
			},
			expectedPass: true,
			expectedMsg:  "",
		},
		{
			name: "Condition startWith, does not match",
			filters: []FilterHeaderOption{
				{Key: "key1", Condition: "startWith", Value: "wrong"},
			},
			expectedPass: false,
			expectedMsg:  "value1 not match with Condition=startWith, Value=wrong",
		},
		{
			name: "Condition endWith, matches",
			filters: []FilterHeaderOption{
				{Key: "key1", Condition: "endWith", Value: "1"},
			},
			expectedPass: true,
			expectedMsg:  "",
		},
		{
			name: "Condition endWith, does not match",
			filters: []FilterHeaderOption{
				{Key: "key1", Condition: "endWith", Value: "wrong"},
			},
			expectedPass: false,
			expectedMsg:  "value1 not match with Condition=endWith, Value=wrong",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pass, msg := FilterHeaders(tt.filters, headers)
			assert.Equal(t, tt.expectedPass, pass)
			assert.Equal(t, tt.expectedMsg, msg)
		})
	}
}
