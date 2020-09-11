package modeler

import (
	"bytes"
	"encoding/json"
)

// FieldType is an enumeration of all allowed types in the modeler mapping
type FieldType int

const (
	// String is elasticsearch keyword datatype
	String FieldType = iota + 1
	// Int is elasticsearch integer datatype
	Int
	// Float is elasticsearch float datatype
	Float
	// DateTime is elasticsearch date datatype with specific format {"type": "date", "format": "date_hour_minute_second_millis"}
	DateTime
	// Boolean is elasticsearch boolean datatype
	Boolean
	// Object is elasticsearch object or nested datatype (based on some other attributes)
	Object
)

// String returns the string version of FieldType
func (s FieldType) String() string {
	return toString[s]
}

var toString = map[FieldType]string{
	String:   "string",
	Int:      "int",
	Float:    "float",
	DateTime: "datetime",
	Boolean:  "boolean",
	Object:   "object",
}

var toID = map[string]FieldType{
	"string":   String,
	"int":      Int,
	"float":    Float,
	"datetime": DateTime,
	"boolean":  Boolean,
	"object":   Object,
}

// MarshalJSON marshals the enum as a quoted json string
func (s FieldType) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString(`"`)
	buffer.WriteString(toString[s])
	buffer.WriteString(`"`)
	return buffer.Bytes(), nil
}

// UnmarshalJSON unmashals a quoted json string to the enum value
func (s *FieldType) UnmarshalJSON(b []byte) error {
	var j string
	err := json.Unmarshal(b, &j)
	if err != nil {
		return err
	}
	// Note that if the string cannot be found then it will be set to the zero value, 'Created' in this case.
	*s = toID[j]
	return nil
}
