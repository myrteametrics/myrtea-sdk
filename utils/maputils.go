package utils

// LookupNestedMap lookup for a value corresponding to the exact specified path inside a map
func LookupNestedMap(pathParts []string, data map[string]interface{}) (interface{}, bool) {
	if val, found := data[pathParts[0]]; found {
		if len(pathParts) > 1 {
			if subdata, ok := val.(map[string]interface{}); ok {
				return LookupNestedMap(pathParts[1:], subdata)
			}
		} else {
			if _, ok := val.(map[string]interface{}); ok {
				return nil, false
			}
			return val, true
		}
	}
	return nil, false
}

// PatchNestedMap update a specific path value in a map
func PatchNestedMap(pathParts []string, data map[string]interface{}, newValue interface{}) bool {
	if val, found := data[pathParts[0]]; found {
		if len(pathParts) > 1 {
			if subdata, ok := val.(map[string]interface{}); ok {
				return PatchNestedMap(pathParts[1:], subdata, newValue)
			}
		} else {
			if _, ok := val.(map[string]interface{}); ok {
				return false
			}
			data[pathParts[0]] = newValue
			return true
		}
	} else {
		if len(pathParts) > 1 {
			data[pathParts[0]] = buildNestedMap(pathParts[1:], newValue)
		} else {
			data[pathParts[0]] = newValue
		}
	}
	return false
}

func buildNestedMap(pathParts []string, newValue interface{}) map[string]interface{} {
	m := make(map[string]interface{}, 0)
	mElement := m
	for i, pathPart := range pathParts {
		if i < len(pathParts)-1 {
			newMap := make(map[string]interface{}, 0)
			mElement[pathPart] = newMap
			mElement = newMap
		} else {
			mElement[pathPart] = newValue
		}
	}
	return m
}
