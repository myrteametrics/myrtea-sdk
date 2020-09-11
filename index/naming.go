package index

import "fmt"

// BuildAliasName build a elasticsearch index alias based on an instance, a document type and a depth
func BuildAliasName(instance string, documentType string, depth Depth) string {
	access := fmt.Sprintf("%s-%s-%s", instance, documentType, depth)
	return access
}
