package models

//Doc is contained in the templateMapping
type DocV6 struct {
	Properties map[string]interface{} `json:"properties"`
}

//TemplateMapping embedded in template
type TemplateMappingV6 struct {
	Document DocV6 `json:"document"`
}

// Template is the ES template
type TemplateV6 struct {
	IndexPatterns []string               `json:"index_patterns"` // Keep the snake_case for elasticsearch template generation
	Settings      map[string]interface{} `json:"settings,omitempty"`
	Mappings      TemplateMappingV6      `json:"mappings,omitempty"`
}

//NewTemplate constructor the ES template
func NewTemplateV6(indexPatterns []string, mapping map[string]interface{}, settings map[string]interface{}) *TemplateV6 {
	return &TemplateV6{
		IndexPatterns: indexPatterns,
		Mappings: TemplateMappingV6{
			Document: DocV6{
				Properties: mapping,
			},
		},
		Settings: settings,
	}
}
