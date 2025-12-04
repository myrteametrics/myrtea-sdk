package utils

import (
	"strconv"
	"strings"
)

// parsePathPart parses a path part and extracts the key and optional array index
// Examples: "test[0]" -> ("test", 0, true), "test" -> ("test", -1, false)
func parsePathPart(part string) (key string, index int, hasIndex bool) {
	if len(part) >= 3 && part[len(part)-1] == ']' {
		if idx := strings.IndexByte(part, '['); idx != -1 && idx < len(part)-1 {
			if i, err := strconv.Atoi(part[idx+1 : len(part)-1]); err == nil {
				return part[:idx], i, true
			}
		}
	}
	return part, -1, false
}

// LookupNestedMap lookup for a value corresponding to the exact specified path inside a map
func LookupNestedMap(pathParts []string, data map[string]interface{}) (interface{}, bool) {
	key, arrIndex, hasIndex := parsePathPart(pathParts[0])

	if val, found := data[key]; found {
		// Handle array index access
		if hasIndex {
			arr, ok := val.([]interface{})
			if !ok {
				return nil, false
			}
			if arrIndex < 0 || arrIndex >= len(arr) {
				return nil, false
			}
			val = arr[arrIndex]

			// If this is the last part and val is a map, return false
			if len(pathParts) == 1 {
				if _, ok := val.(map[string]interface{}); ok {
					return nil, false
				}
				return val, true
			}
		}

		if len(pathParts) > 1 {
			if subdata, ok := val.(map[string]interface{}); ok {
				return LookupNestedMap(pathParts[1:], subdata)
			}
			return nil, false
		} else {
			if _, ok := val.(map[string]interface{}); ok {
				return nil, false
			}
			return val, true
		}
	}
	return nil, false
}

// LookupNestedMapValue lookup for any value (including maps) corresponding to the exact specified path inside a map
// Unlike LookupNestedMap, this function can return map values, making it suitable for Replace operations
func LookupNestedMapValue(pathParts []string, data map[string]interface{}) (interface{}, bool) {
	key, arrIndex, hasIndex := parsePathPart(pathParts[0])

	if val, found := data[key]; found {
		// Handle array index access
		if hasIndex {
			arr, ok := val.([]interface{})
			if !ok {
				return nil, false
			}
			if arrIndex < 0 || arrIndex >= len(arr) {
				return nil, false
			}
			val = arr[arrIndex]

			// If this is the last part, return the value (even if it's a map)
			if len(pathParts) == 1 {
				return val, true
			}
		}

		if len(pathParts) > 1 {
			if subdata, ok := val.(map[string]interface{}); ok {
				return LookupNestedMapValue(pathParts[1:], subdata)
			}
			return nil, false
		} else {
			// Return the value (even if it's a map)
			return val, true
		}
	}
	return nil, false
}

// PatchNestedMap update a specific path value in a map
func PatchNestedMap(pathParts []string, data map[string]interface{}, newValue interface{}) bool {
	key, arrIndex, hasIndex := parsePathPart(pathParts[0])

	if val, found := data[key]; found {
		// Handle array index access
		if hasIndex {
			arr, ok := val.([]interface{})
			if !ok {
				return false
			}
			if arrIndex < 0 || arrIndex >= len(arr) {
				return false
			}

			if len(pathParts) > 1 {
				if subdata, ok := arr[arrIndex].(map[string]interface{}); ok {
					return PatchNestedMap(pathParts[1:], subdata, newValue)
				}
				return false
			} else {
				// Replace the array element with the new value
				arr[arrIndex] = newValue
				return true
			}
		}

		if len(pathParts) > 1 {
			if subdata, ok := val.(map[string]interface{}); ok {
				return PatchNestedMap(pathParts[1:], subdata, newValue)
			}
			return false
		} else {
			// Allow replacing maps only with other maps, prevent replacing maps with non-map values
			if _, existingIsMap := val.(map[string]interface{}); existingIsMap {
				if _, newIsMap := newValue.(map[string]interface{}); newIsMap {
					// Allow replacing map with another map
					data[key] = newValue
					return true
				}
				// Prevent replacing map with non-map value
				return false
			}
			data[key] = newValue
			return true
		}
	} else {
		if len(pathParts) > 1 {
			data[key] = buildNestedMap(pathParts[1:], newValue)
		} else {
			data[key] = newValue
		}
	}
	return false
}

// DeleteNestedMap delete a specific path value in a map
func DeleteNestedMap(pathParts []string, data map[string]interface{}) bool {
	key, arrIndex, hasIndex := parsePathPart(pathParts[0])

	if val, found := data[key]; found {
		// Handle array index access
		if hasIndex {
			arr, ok := val.([]interface{})
			if !ok {
				return false
			}
			if arrIndex < 0 || arrIndex >= len(arr) {
				return false
			}

			if len(pathParts) > 1 {
				if subdata, ok := arr[arrIndex].(map[string]interface{}); ok {
					return DeleteNestedMap(pathParts[1:], subdata)
				}
				return false
			} else {
				// Remove element from array by creating a new slice
				data[key] = append(arr[:arrIndex], arr[arrIndex+1:]...)
				return true
			}
		}

		if len(pathParts) > 1 {
			if subdata, ok := val.(map[string]interface{}); ok {
				return DeleteNestedMap(pathParts[1:], subdata)
			}
			return false
		} else {
			delete(data, key)
			return true
		}
	}
	return false
}

func buildNestedMap(pathParts []string, newValue interface{}) map[string]interface{} {
	m := make(map[string]interface{})
	mElement := m
	for i, pathPart := range pathParts {
		if i < len(pathParts)-1 {
			newMap := make(map[string]interface{})
			mElement[pathPart] = newMap
			mElement = newMap
		} else {
			mElement[pathPart] = newValue
		}
	}
	return m
}

func FlattenFact(dataArray []interface{}, pathKey string, pathValue string) map[string]interface{} {
	m := make(map[string]interface{})
	for _, data := range dataArray {
		val, ok := data.(map[string]interface{})
		if !ok {
			return nil
		}
		if key, found := LookupNestedMap(strings.Split(pathKey, "."), val); found {
			if val, found := LookupNestedMap(strings.Split(pathValue, "."), val); found {
				m[key.(string)] = val
			} else {
				return nil
			}
		} else {
			return nil
		}
	}
	return m
}
