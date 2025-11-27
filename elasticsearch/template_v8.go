package elasticsearch

import (
	"github.com/elastic/go-elasticsearch/v8/typedapi/indices/putindextemplate"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/myrteametrics/myrtea-sdk/v5/modeler"
)

// NewPutIndexTemplateRequestV8 constructor the ES template
func NewPutIndexTemplateRequestV8(indexPatterns []string, model modeler.Model) *putindextemplate.Request {
	mappings := modelToMappingV8(model)

	req := putindextemplate.NewRequest()
	req.IndexPatterns = indexPatterns
	req.Template = &types.IndexTemplateMapping{
		Mappings: mappings,
		Settings: &model.ElasticsearchOptions.AdvancedSettings,
	}
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

		properties := make(map[string]types.Property)
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
			property = types.NewDateProperty()
		}

		return field.Name, property
	}
	return "", nil
}
