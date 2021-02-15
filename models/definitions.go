package models

//Doc is contained in the templateMapping
type Doc struct {
	Properties map[string]interface{} `json:"properties"`
}

//TemplateMapping embedded in template
type TemplateMapping struct {
	Document Doc `json:"document"`
}

// Template is the ES template
type Template struct {
	IndexPatterns []string               `json:"index_patterns"` // Keep the snake_case for elasticsearch template generation
	Settings      map[string]interface{} `json:"settings,omitempty"`
	Mappings      TemplateMapping        `json:"mappings,omitempty"`
}

//NewTemplate constructor the ES template
func NewTemplate(indexPatterns []string, mapping map[string]interface{}, settings map[string]interface{}) *Template {
	return &Template{
		IndexPatterns: indexPatterns,
		Mappings: TemplateMapping{
			Document: Doc{
				Properties: mapping,
			},
		},
		Settings: settings,
	}
}

//Document represent an es document
type Document struct {
	ID        string      `json:"id"`
	Index     string      `json:"index"`
	IndexType string      `json:"type"`
	Source    interface{} `json:"source"`
}

//NewDocument Construct a new Document
func NewDocument(id string, index string, indexType string, source interface{}) *Document {
	return &Document{id, index, indexType, source}
}
