package models

//TemplateMapping embedded in template
type TemplateMappingV8 struct {
	Properties map[string]interface{} `json:"properties"`
}

// Template is the ES template
type TemplateV8 struct {
	IndexPatterns []string               `json:"index_patterns"` // Keep the snake_case for elasticsearch template generation
	Settings      map[string]interface{} `json:"settings,omitempty"`
	Mappings      TemplateMappingV8      `json:"mappings,omitempty"`
}

//NewTemplate constructor the ES template
func NewTemplateV8(indexPatterns []string, mapping map[string]interface{}, settings map[string]interface{}) TemplateV8 {
	return TemplateV8{
		IndexPatterns: indexPatterns,
		Mappings: TemplateMappingV8{
			Properties: mapping,
		},
		Settings: settings,
	}
}
