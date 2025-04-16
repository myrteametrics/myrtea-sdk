package modeler

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

var (
	cronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
)

// IndexIntervalType represents the index interval options for time-based
type IndexIntervalType string

const (
	Daily   IndexIntervalType = "daily"
	Monthly IndexIntervalType = "monthly"
)

// RollmodeType represents the possible types of Rollmode
type RollmodeType string

const (
	RollmodeCron      RollmodeType = "cron"
	RollmodeTimeBased RollmodeType = "timebased"
)

// TimebasedSettings contains the specific settings for "timebased"
type TimebasedSettings struct {
	Interval IndexIntervalType `json:"interval"`
}

// RollmodeSettings represents either a simple string for "cron" or a configuration for "timebased".
type RollmodeSettings struct {
	Type      RollmodeType       `json:"type"`
	Timebased *TimebasedSettings `json:"timebased,omitempty"`
}

// ElasticsearchOptions regroups every elasticsearch specific options
type ElasticsearchOptions struct {
	// Rollmode can only be "rollover" atm
	Rollmode                  RollmodeSettings    `json:"rollmode"`
	Rollcron                  string              `json:"rollcron"`
	EnablePurge               bool                `json:"enablePurge"`
	PurgeMaxConcurrentIndices int                 `json:"purgeMaxConcurrentIndices"`
	PatchAliasMaxIndices      int                 `json:"patchAliasMaxIndices"`
	AdvancedSettings          types.IndexSettings `json:"advancedSettings,omitempty"`
}

// IsValid checks if a model elasticsearch options is valid and has no missing mandatory fields
func (eso ElasticsearchOptions) IsValid() (bool, error) {
	if eso.Rollmode.Type == "" {
		return false, errors.New("missing Rollmode")
	}

	if eso.Rollmode.Type == RollmodeTimeBased && eso.Rollmode.Timebased == nil {
		return false, errors.New("missing Timebased settings for timebased rollmode")
	}

	if eso.Rollmode.Type == RollmodeCron {
		if eso.Rollcron == "" {
			return false, errors.New("missing Rollcron")
		}
		if parsedCron, err := cronParser.Parse(eso.Rollcron); err != nil {
			return false, fmt.Errorf("invalid Rollcron: %w", err)
		} else {
			now := time.Now()
			year, month, day := now.Date()
			now = time.Date(year, month, day, 0, 0, 0, 0, now.Location())

			if parsedCron.Next(now).Sub(now) < 24*time.Hour {
				return false, fmt.Errorf("invalid Rollcron: interval must be at every day")
			}
		}
	}

	if eso.PurgeMaxConcurrentIndices < 0 {
		return false, errors.New("invalid PurgeMaxConcurrentIndices")
	}
	if eso.EnablePurge && eso.PurgeMaxConcurrentIndices < 1 {
		return false, errors.New("invalid PurgeMaxConcurrentIndices")
	}
	if eso.PatchAliasMaxIndices < 0 {
		return false, errors.New("invalid PatchAliasMaxIndices")
	}
	return true, nil
}

// Model represents a business entity model
type Model struct {
	ID                   int64                `json:"id"`
	Name                 string               `json:"name"`
	Synonyms             []string             `json:"synonyms"`
	Fields               []Field              `json:"fields"`
	Source               string               `json:"source,omitempty"`
	ElasticsearchOptions ElasticsearchOptions `json:"elasticsearchOptions"`
}

// IsValid checks if a model definition is valid and has no missing mandatory fields
func (model *Model) IsValid() (bool, error) {
	if model.Name == "" {
		return false, errors.New("missing Name")
	}
	if model.Name != strings.ToLower(model.Name) {
		return false, errors.New("name must be lower case")
	}
	for _, field := range model.Fields {
		if ok, err := field.IsValid(); !ok {
			if err != nil {
				return false, errors.New("Invalid Field:" + err.Error())
			}
			return false, errors.New("invalid field")
		}
	}
	if ok, err := model.ElasticsearchOptions.IsValid(); !ok {
		if err != nil {
			return false, errors.New("Invalid ElasticsearchOptions:" + err.Error())
		}
		return false, errors.New("invalid ElasticsearchOptions")
	}
	return true, nil
}

// ToElasticsearchMappingProperties converts a modeler mapping to an elasticsearch mapping
func (model *Model) ToElasticsearchMappingProperties() map[string]interface{} {
	properties := make(map[string]interface{})
	for _, field := range model.Fields {
		fieldName, fieldContent := field.Source()
		properties[fieldName] = fieldContent
	}
	return properties
}

// UnmarshalJSON unmarshal a quoted json string to a Model instance
func (model *Model) UnmarshalJSON(b []byte) error {
	type Alias Model
	aux := &struct {
		*Alias
		Fields []*json.RawMessage `json:"fields,omitempty"`
	}{
		Alias: (*Alias)(model),
	}
	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}
	if aux.Fields != nil {
		fields, err := unMarshallFields(aux.Fields)
		if err != nil {
			return err
		}
		model.Fields = fields
	}
	return nil
}

func unMarshallFields(fieldsJSON []*json.RawMessage) ([]Field, error) {
	var fields = make([]Field, 0)
	for _, raw := range fieldsJSON {
		var m map[string]interface{}
		err := json.Unmarshal(*raw, &m)
		if err != nil {
			return nil, err
		}

		switch m["type"] {
		case "object":
			var f FieldObject
			err := json.Unmarshal(*raw, &f)
			if err != nil {
				return nil, err
			}
			fields = append(fields, &f)

		default:
			var f FieldLeaf
			err := json.Unmarshal(*raw, &f)
			if err != nil {
				return nil, err
			}
			fields = append(fields, &f)
		}
	}
	return fields, nil
}
