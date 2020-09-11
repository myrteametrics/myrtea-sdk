package modeler

import "fmt"

// FindFieldLeaf returns if a field name exists in a fields tree
func FindFieldLeaf(search string, parent string, fields []Field) bool {
	for _, field := range fields {
		switch v := field.(type) {
		case *FieldLeaf:
			name := v.Name
			if parent != "" {
				name = fmt.Sprintf("%s.%s", parent, v.Name)
			}
			if search == name {
				return true
			}
		case *FieldObject:
			name := v.Name
			if parent != "" {
				name = fmt.Sprintf("%s.%s", parent, v.Name)
			}
			if FindFieldLeaf(search, name, v.Fields) {
				return true
			}
		}
	}
	return false
}

// BuildFieldPath returns the full fields tree path to a field
func BuildFieldPath(name string, fields []Field) string {
	for _, field := range fields {
		switch v := field.(type) {
		case *FieldLeaf:
			if name == v.Name {
				return v.Name
			}
		case *FieldObject:
			path := BuildFieldPath(name, v.Fields)
			if path != "" {
				return fmt.Sprintf("%s.%s", v.Name, path)
			}
		}
	}
	return ""
}
