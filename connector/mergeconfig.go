package connector

import (
	"strings"
	"time"

	"github.com/myrteametrics/myrtea-sdk/v4/expression"
	"github.com/myrteametrics/myrtea-sdk/v4/models"
	"github.com/myrteametrics/myrtea-sdk/v4/utils"
	"go.uber.org/zap"
)

var (
	dateLayouts = [...]string{
		time.ANSIC,
		time.UnixDate,
		time.RubyDate,
		time.Kitchen,
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02",                         // RFC 3339
		"2006-01-02 15:04",                   // RFC 3339 with minutes
		"2006-01-02 15:04:05",                // RFC 3339 with seconds
		"2006-01-02 15:04:05-07:00",          // RFC 3339 with seconds and timezone
		"2006-01-02T15Z0700",                 // ISO8601 with hour
		"2006-01-02T15:04Z0700",              // ISO8601 with minutes
		"2006-01-02T15:04:05Z0700",           // ISO8601 with seconds
		"2006-01-02T15:04:05.999999999Z0700", // ISO8601 with nanoseconds
		"2006-01-02T15:04:05.999999999",      // ISO8601 with nanoseconds
	}
)

// Config wraps all rules for document merging
type Config struct {
	Mode             Mode    `json:"mode"`
	ExistingAsMaster bool    `json:"existingAsMaster"`
	Type             string  `json:"type,omitempty"`
	LinkKey          string  `json:"linkKey,omitempty"`
	Groups           []Group `json:"groups,omitempty"`
}

// Group allows to group un set of merge fields and to define an optional condition to applay the merge fields
type Group struct {
	Condition             string      `json:"condition,omitempty"`
	FieldReplace          []string    `json:"fieldReplace,omitempty"`
	FieldReplaceIfMissing []string    `json:"fieldReplaceIfMissing,omitempty"`
	FieldMerge            []string    `json:"fieldMerge,omitempty"`
	FieldMath             []FieldMath `json:"fieldMath,omitempty"`
	FieldKeepLatest       []string    `json:"fieldKeepLatest,omitempty"`
	FieldKeepEarliest     []string    `json:"fieldKeepEarliest,omitempty"`
	FieldForceUpdate      []string    `json:"fieldForceUpdate,omitempty"`
}

// FieldMath specify a merge rule using a math expression
type FieldMath struct {
	Expression  string `json:"expression"`
	OutputField string `json:"outputField"`
}

// Apply returns a pre-build merge function, configured with a specific merge config
// Merge is done in the following order : FieldMath, FieldReplace, FieldMerge
func (config *Config) Apply(newDoc *models.Document, existingDoc *models.Document) *models.Document {
	if existingDoc == nil {
		return newDoc
	}
	if newDoc.Source == nil {
		newDoc.Source = make(map[string]interface{})
	}
	if existingDoc.Source == nil {
		existingDoc.Source = make(map[string]interface{})
	}

	// Select "master" output document (new one vs existing one)
	var output, enricher *models.Document
	if config.ExistingAsMaster { // existingDoc is now master
		enricher = newDoc
		output = existingDoc
	} else { // newDoc is now master
		output = newDoc
		enricher = existingDoc
	}

	// copy exitingDoc source and add missing keys for conditions evaluation
	data, _ := jsoni.Marshal(existingDoc.Source)
	existingCopy := make(map[string]interface{})
	jsoni.Unmarshal(data, &existingCopy)

	addKeys(newDoc.Source, existingCopy)

	for _, mergeGroup := range config.Groups {
		var applyMergeGroup bool
		if mergeGroup.Condition != "" {
			result, err := expression.Process(
				expression.LangEval,
				mergeGroup.Condition,
				map[string]interface{}{"New": newDoc.Source, "Existing": existingCopy},
			)
			if err != nil {
				if strings.Contains(err.Error(), "unknown parameter") {
					zap.L().Debug("Math evaluation is invalid", zap.Error(err))
				} else {
					zap.L().Debug("Math evaluation is invalid", zap.Error(err))
					zap.L().Debug("eval",
						zap.Any("new", newDoc.Source),
						zap.Any("existing", existingDoc.Source),
						zap.Any("expression", mergeGroup.Condition),
						zap.Any("result", result),
					)
				}
				continue
			}
			if val, ok := result.(bool); !ok {
				zap.L().Warn("Math evaluation does not returns an boolean value")
				continue
			} else {
				applyMergeGroup = val
			}
		}

		if mergeGroup.Condition == "" || applyMergeGroup {
			ApplyFieldMath(mergeGroup.FieldMath, newDoc, existingDoc, output.Source)
			// zap.L().Debug("math", zap.Any("source", outputSource))

			ApplyFieldReplaceIfMissing(mergeGroup.FieldReplaceIfMissing, enricher.Source, output.Source)
			// zap.L().Debug("replace", zap.Any("source", outputSource))

			ApplyFieldReplace(mergeGroup.FieldReplace, enricher.Source, output.Source)
			// zap.L().Debug("replace", zap.Any("source", outputSource))

			ApplyFieldKeepLatest(mergeGroup.FieldKeepLatest, enricher.Source, output.Source)
			// zap.L().Debug("KeepLatest", zap.Any("source", outputSource))

			ApplyFieldKeepEarliest(mergeGroup.FieldKeepEarliest, enricher.Source, output.Source)
			// zap.L().Debug("KeepEarliest", zap.Any("source", outputSource))

			ApplyFieldMerge(mergeGroup.FieldMerge, enricher.Source, output.Source)

			// keepBigger + keepMostrecent etc...
			// keepSmaller + keepOlder etc...
			// ...

			ApplyFieldForceUpdate(mergeGroup.FieldForceUpdate, enricher.Source, output.Source)
			// zap.L().Debug("update", zap.Any("source", outputSource))
		}
	}
	return output
}

// ApplyFieldMath applies all FieldMath merging configuration on input documents
func ApplyFieldMath(config []FieldMath, newDoc *models.Document, existingDoc *models.Document, outputSource map[string]interface{}) {
	for _, math := range config {
		result, err := expression.Process(
			expression.LangEval,
			math.Expression,
			map[string]interface{}{"New": newDoc.Source, "Existing": existingDoc.Source, "Output": outputSource},
		)
		if err != nil {
			if strings.Contains(err.Error(), "unknown parameter") {
				zap.L().Debug("Math evaluation is invalid", zap.Error(err))
			} else {
				zap.L().Debug("Math evaluation is invalid", zap.Error(err))
				zap.L().Debug("eval",
					zap.Any("new", newDoc.Source),
					zap.Any("existing", existingDoc.Source),
					zap.Any("output", outputSource),
					zap.Any("expression", math.Expression),
					zap.Any("result", result),
				)
			}
			continue
		}

		outputPart := strings.Split(math.OutputField, ".")
		utils.PatchNestedMap(outputPart, outputSource, result)
	}
}

// ApplyFieldReplaceIfMissing applies all FieldReplace merging configuration on input documents
func ApplyFieldReplaceIfMissing(fieldReplace []string, enricherSource map[string]interface{}, outputSource map[string]interface{}) {
	for _, field := range fieldReplace {
		_, okOutput := outputSource[field]
		_, foundOutput := utils.LookupNestedMap(strings.Split(field, "."), outputSource)
		if !okOutput && !foundOutput {
			if _, ok := enricherSource[field]; ok {
				outputSource[field] = enricherSource[field]
			} else if val, found := utils.LookupNestedMap(strings.Split(field, "."), enricherSource); found {
				utils.PatchNestedMap(strings.Split(field, "."), outputSource, val)
			}
		}
	}
}

// ApplyFieldReplace applies all FieldReplace merging configuration on input documents
func ApplyFieldReplace(fieldReplace []string, enricherSource map[string]interface{}, outputSource map[string]interface{}) {
	for _, field := range fieldReplace {
		if val, ok := enricherSource[field]; ok {
			if !isEmpty(val) {
				outputSource[field] = enricherSource[field]
			}
		} else if val, found := utils.LookupNestedMap(strings.Split(field, "."), enricherSource); found {
			if !isEmpty(val) {
				utils.PatchNestedMap(strings.Split(field, "."), outputSource, val)
			}
		}
	}
}

// ApplyFieldForceUpdate applies FieldForceUpdate merging configuration on input documents
func ApplyFieldForceUpdate(fieldUpdate []string, enricherSource map[string]interface{}, outputSource map[string]interface{}) {
	for _, field := range fieldUpdate {
		if val, ok := enricherSource[field]; ok {
			if isEmpty(val) {
				delete(outputSource, field)
			} else {
				outputSource[field] = enricherSource[field]
			}
		} else if val, found := utils.LookupNestedMap(strings.Split(field, "."), enricherSource); found {
			if isEmpty(val) {
				utils.DeleteNestedMap(strings.Split(field, "."), outputSource)
			} else {
				utils.PatchNestedMap(strings.Split(field, "."), outputSource, val)
			}
		}
	}
}

// ApplyFieldMerge applies all FieldReplace merging configuration on input documents
func ApplyFieldMerge(fieldMerge []string, enricherSource map[string]interface{}, outputSource map[string]interface{}) {
	for _, field := range fieldMerge {
		if _, ok := enricherSource[field]; ok {
			m := make(map[interface{}]bool)

			switch v := outputSource[field].(type) {
			case []interface{}:
				for _, e := range v {
					m[e] = true
				}
			case interface{}:
				m[v] = true
			}

			switch v := enricherSource[field].(type) {
			case []interface{}:
				for _, e := range v {
					m[e] = true
				}
			case interface{}:
				m[v] = true
			}

			newSlice := make([]interface{}, 0)
			for k := range m {
				newSlice = append(newSlice, k)
			}
			outputSource[field] = newSlice
		}
	}
}

// ApplyFieldKeepLatest applies all FieldKeepLatest merging configuration on input documents
func ApplyFieldKeepLatest(fieldKeepLatest []string, enricherSource map[string]interface{}, outputSource map[string]interface{}) {
	for _, field := range fieldKeepLatest {
		if sourceValue, okSource, nested, sourceStr := getValueAsTime(field, enricherSource); okSource {
			var updateValue bool
			if outputValue, okOutput, _, _ := getValueAsTime(field, outputSource); okOutput {
				if outputValue.Before(sourceValue) {
					updateValue = true
				}
			} else {
				updateValue = true
			}

			if updateValue {
				if nested {
					utils.PatchNestedMap(strings.Split(field, "."), outputSource, sourceStr)
				} else {
					outputSource[field] = sourceStr
				}
			}
		}
	}
}

// ApplyFieldKeepEarliest applies all FieldKeepEarliest merging configuration on input documents
func ApplyFieldKeepEarliest(fieldKeepEarliest []string, enricherSource map[string]interface{}, outputSource map[string]interface{}) {
	for _, field := range fieldKeepEarliest {
		if sourceValue, okSource, nested, sourceStr := getValueAsTime(field, enricherSource); okSource {
			var updateValue bool
			if outputValue, okOutput, _, _ := getValueAsTime(field, outputSource); okOutput {
				if outputValue.After(sourceValue) {
					updateValue = true
				}
			} else {
				updateValue = true
			}

			if updateValue {
				if nested {
					utils.PatchNestedMap(strings.Split(field, "."), outputSource, sourceStr)
				} else {
					outputSource[field] = sourceStr
				}
			}
		}
	}
}

func getValueAsTime(field string, source map[string]interface{}) (time.Time, bool, bool, string) {
	var value string
	var nested bool
	if val, ok := source[field].(string); !ok {
		if val, found := utils.LookupNestedMap(strings.Split(field, "."), source); found {
			nested = true
			value, _ = val.(string)
		}
	} else {
		value = val
	}
	if value != "" {
		for _, format := range dateLayouts {
			val, err := time.ParseInLocation(format, value, time.Local)
			if err != nil {
				continue
			}
			return val, true, nested, value
		}
	}
	return time.Time{}, false, false, ""
}

func addKeys(source map[string]interface{}, target map[string]interface{}) {
	for key, value := range source {
		if _, found := target[key]; !found {
			switch v := value.(type) {
			case map[string]interface{}:
				target[key] = make(map[string]interface{})
				addKeys(v, target[key].(map[string]interface{}))
			default:
				target[key] = nil
			}
		}
	}
}

func isEmpty(value interface{}) bool {
	if value == nil {
		return true
	}
	switch val := value.(type) {
	case string:
		return val == ""
	default:
		return false
	}
}
