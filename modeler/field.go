package modeler

import (
	"encoding/json"
	"errors"
)

// Field is an interface for recursive mapping modeling (Leaf fields and object fields)
type Field interface {
	Source() (string, map[string]interface{})
	IsValid() (bool, error)
}

// FieldLeaf implements Field interface and represents a terminal node (or primitive type) in the modeler mapping
type FieldLeaf struct {
	Name     string    `json:"name"`
	Ftype    FieldType `json:"type"`
	Semantic bool      `json:"semantic"`
	Synonyms []string  `json:"synonyms"`
}

// IsValid checks if a model field leaf is valid and has no missing mandatory fields
func (field *FieldLeaf) IsValid() (bool, error) {
	if field.Name == "" {
		return false, errors.New("Missing Name")
	}
	if field.Ftype == 0 {
		return false, errors.New("Missing Ftype (or 0 value)")
	}
	return true, nil
}

// Source (FieldLeaf) returns a elasticsearch field with a name and a slice of attributes
// It mainly process primitives elasticsearch data type
func (field *FieldLeaf) Source() (string, map[string]interface{}) {
	var fieldContent map[string]interface{}
	switch field.Ftype {
	case Int:
		fieldContent = map[string]interface{}{
			"type": "integer",
		}

	case String:
		fieldContent = map[string]interface{}{
			"type": "keyword",
		}

	case Float:
		fieldContent = map[string]interface{}{
			"type": "float",
		}

	case Boolean:
		fieldContent = map[string]interface{}{
			"type": "boolean",
		}

	case DateTime:
		fieldContent = map[string]interface{}{
			"type":   "date",
			"format": "date_hour_minute_second_millis",
		}
	}
	return field.Name, fieldContent
}

// FieldObject implements Field interface and represents a non-terminal node (or object) in the modeler mapping
type FieldObject struct {
	Name                 string    `json:"name"`
	Ftype                FieldType `json:"type"`
	KeepObjectSeparation bool      `json:"keepObjectSeparation"`
	Fields               []Field   `json:"fields"`
}

// IsValid checks if a model definition is valid and has no missing mandatory fields
func (field *FieldObject) IsValid() (bool, error) {
	if field.Name == "" {
		return false, errors.New("Missing Name")
	}
	if field.Ftype == 0 {
		return false, errors.New("Missing Ftype (or 0 value)")
	}
	for _, field := range field.Fields {
		if ok, err := field.IsValid(); !ok {
			return false, errors.New("Invalid Field:" + err.Error())
		}
	}
	return true, nil
}

// Source (FieldObject) returns a elasticsearch field with a name and a slice of attributes
// It mainly manages "object" and "nested" elasticsearch data type
func (field *FieldObject) Source() (string, map[string]interface{}) {
	var fieldContent map[string]interface{}

	properties := make(map[string]interface{}, 0)
	for _, field := range field.Fields {
		fieldName, fieldContent := field.Source()
		properties[fieldName] = fieldContent
	}

	if field.KeepObjectSeparation {
		fieldContent = map[string]interface{}{
			"type":       "nested",
			"properties": properties,
		}
	} else {
		fieldContent = map[string]interface{}{
			"properties": properties,
		}
	}
	return field.Name, fieldContent
}

// UnmarshalJSON (FieldObject) unmarshall a JSON byte slice in FieldObject struct
func (field *FieldObject) UnmarshalJSON(b []byte) error {
	type Alias FieldObject
	aux := &struct {
		*Alias
		Fields []*json.RawMessage `json:"fields,omitempty"`
	}{
		Alias: (*Alias)(field),
	}
	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}
	if aux.Fields != nil {
		fields, err := unMarshallFields(aux.Fields)
		if err != nil {
			return err
		}
		field.Fields = fields
	}
	return nil
}
