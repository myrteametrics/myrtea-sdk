package elasticsearchv8

import (
	"github.com/elastic/go-elasticsearch/v8/typedapi/indices/puttemplate"
	"github.com/elastic/go-elasticsearch/v8/typedapi/some"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/myrteametrics/myrtea-sdk/v4/modeler"
)

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
func NewTemplateV8(indexPatterns []string, model modeler.Model) *puttemplate.Request {
	mappings := modelToMappingV8(model)
	settings := model.ElasticsearchOptions.AdvancedSettings

	req := puttemplate.NewRequest()
	req.IndexPatterns = indexPatterns
	req.Mappings = mappings
	req.Settings = settings
	return req
}

func modelToMappingV8(model modeler.Model) *types.TypeMapping {
	properties := make(map[string]types.Property)
	for _, field := range model.Fields {
		name, property := fieldToPropertyV8(field)
		properties[name] = property
	}

	mappings := types.NewTypeMapping()
	mappings.Properties = properties

	return mappings
}

func fieldToPropertyV8(rawField modeler.Field) (string, types.Property) {

	switch field := rawField.(type) {
	case *modeler.FieldObject:
		var property types.Property

		properties := make(map[string]types.Property, 0)
		for _, field := range field.Fields {
			name, childProperty := fieldToPropertyV8(field)
			properties[name] = childProperty
		}

		if field.KeepObjectSeparation {
			p := types.NewNestedProperty()
			p.Properties = properties
			property = p
		} else {
			p := types.NewObjectProperty()
			p.Properties = properties
			property = p
		}

		return field.Name, property

	case *modeler.FieldLeaf:
		var property types.Property

		switch field.Ftype {
		case modeler.Int:
			property = types.NewIntegerNumberProperty()

		case modeler.String:
			property = types.NewKeywordProperty()

		case modeler.Float:
			property = types.NewFloatNumberProperty()

		case modeler.Boolean:
			property = types.NewBooleanProperty()

		case modeler.DateTime:
			p := types.NewDateProperty()
			p.Format = some.String("date_hour_minute_second_millis")
			property = p
		}

		return field.Name, property
	}
	return "", nil
}
