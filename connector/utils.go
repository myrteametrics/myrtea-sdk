package connector

import (
	"fmt"
	"strconv"
)

// LookupNestedMapFullPaths Looks searches value of all paths in data and concatenates them with a separator
func LookupNestedMapFullPaths(data interface{}, paths [][]string, separator string) (interface{}, bool) {
	if len(paths) == 0 {
		return nil, false
	}

	val, found := LookupNestedMap(paths[0], data)
	if !found {
		return nil, false
	}

	if len(paths) > 1 {
		// don't look up twice for first element
		result := fmt.Sprintf("%v%s", val, separator)

		for i, path := range paths[1:] {
			if i > 0 {
				result += separator
			}

			val, found = LookupNestedMap(path, data)
			if !found {
				continue
			} else {
				result = fmt.Sprintf("%s%v", result, val)
			}
		}
		val = result
	}

	return val, true
}

// LookupNestedMap lookup for a value corresponding to the exact specified path inside a map
func LookupNestedMap(pathParts []string, data interface{}) (interface{}, bool) {
	if len(pathParts) == 0 {
		return data, true
	}

	searchField := pathParts[0]

	switch v := data.(type) {
	case map[string]interface{}:
		if searchField != "*" {
			if val, found := v[searchField]; found {
				return LookupNestedMap(pathParts[1:], val)
			}
		} else {
			for _, l := range v {
				if val, found := LookupNestedMap(pathParts[1:], l); found {
					return val, found
				}
			}
		}
	case []interface{}:
		// this code stays here for performance improvements (0 index is mostly used)
		if searchField == "[0]" && len(v) > 0 {
			return LookupNestedMap(pathParts[1:], v[0])
		}

		// Check if searchField is in the form of "[...]"
		if len(searchField) > 2 && searchField[0] == '[' && searchField[len(searchField)-1] == ']' {
			// Extract the index as a string and convert it to an integer
			indexStr := searchField[1 : len(searchField)-1]
			index, err := strconv.Atoi(indexStr)
			if err == nil && index >= 0 && index < len(v) {
				return LookupNestedMap(pathParts[1:], v[index])
			}
		}
	}

	return nil, false
}
