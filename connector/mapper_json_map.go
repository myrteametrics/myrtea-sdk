package connector

import (
	"errors"
	"fmt"
	"github.com/myrteametrics/myrtea-sdk/v5/utils"
	"go.uber.org/zap"
	"strconv"
	str "strings"
	"time"
)

// JSONMapperMap :
type JSONMapperMap struct {
	filters map[string]JSONMapperFilterItem
	mapping map[string]map[string]JSONMapperConfigItem
}

// NewJSONMapperMap :
func NewJSONMapperMap(name, path string) (*JSONMapperMap, error) {
	filters, mapping, err := getConfig(name, path)
	if err != nil {
		return nil, err
	}
	return &JSONMapperMap{filters: filters, mapping: mapping}, nil
}

// FilterDocument checks if document is filtered or not, returns if documents valid and if invalid, the following reason
func (mapper JSONMapperMap) FilterDocument(msg Message) (bool, string) {
	var data map[string]interface{}

	switch message := msg.(type) {
	case DecodedKafkaMessage:
		// don't handle message if there's no filters
		if len(mapper.filters) == 0 {
			return true, ""
		}

		data = message.Data
	default:
		return false, "message type not supported"
	}

	for _, filter := range mapper.filters {
		fieldExtractedValueRaw, found := LookupNestedMapFullPaths(data, filter.Paths, filter.Separator)
		fieldExtractedValue := ""
		if !found || fieldExtractedValueRaw == "" {
			if filter.DefaultValue != "" {
				fieldExtractedValue = filter.DefaultValue
			} else {
				return false, fmt.Sprintf("Filter Field missing : %+v", filter)
			}
		} else {
			fieldExtractedValue = fmt.Sprintf("%v", fieldExtractedValueRaw)
		}

		switch filter.Condition {
		case "exists":

		case "equals":
			if fieldExtractedValue != filter.Value {
				return false, fmt.Sprintf("%s not match with Condition=%s, Value=%s", fieldExtractedValue, filter.Condition, filter.Value)
			}
		case "equals_atleastone":
			isValid := false
			for _, value := range filter.Values {
				if fieldExtractedValue == value {
					isValid = true
					break
				}
			}
			if !isValid {
				return false, fmt.Sprintf("%s not match with Condition=%s, Value=%s", fieldExtractedValue, filter.Condition, filter.Values)
			}
		case "notEquals_any":
			for _, value := range filter.Values {
				if fieldExtractedValue == value {
					return false, fmt.Sprintf("%s matches with one of the values in Condition=%s, Values=%s", fieldExtractedValue, filter.Condition, filter.Values)
				}
			}
		case "notEquals":
			if fieldExtractedValue == filter.Value {
				return false, fmt.Sprintf("%s not match with Condition=%s, Value=%s", fieldExtractedValue, filter.Condition, filter.Value)
			}
		case "startWith":
			if !str.HasPrefix(fieldExtractedValue, filter.Value) {
				return false, fmt.Sprintf("%s not match with Condition=%s, Value=%s", fieldExtractedValue, filter.Condition, filter.Value)
			}
		case "endWith":
			if !str.HasSuffix(fieldExtractedValue, filter.Value) {
				return false, fmt.Sprintf("%s not match with Condition=%s, Value=%s", fieldExtractedValue, filter.Condition, filter.Value)
			}
		case "contains":
			if !str.Contains(fieldExtractedValue, filter.Value) {
				return false, fmt.Sprintf("%s not match with Condition=%s, Value=%s", fieldExtractedValue, filter.Condition, filter.Value)
			}
		}
	}
	return true, ""
}

// MapToDocument Maps data to document
func (mapper JSONMapperMap) MapToDocument(msg Message) (Message, error) {
	var data map[string]interface{}

	switch message := msg.(type) {
	case DecodedKafkaMessage:
		data = message.Data
	default:
		return TypedDataMessage{}, errors.New("message type not supported")
	}

	strings := make(map[string]string)
	ints := make(map[string]int64)
	bools := make(map[string]bool)
	times := make(map[string]time.Time)

	for _, groupVal := range mapper.mapping {
		for fieldKey, fieldConfig := range groupVal {

			var val interface{}
			if fieldConfig.FieldType != "uuid_from_longs" {
				var found bool
				val, found = LookupNestedMapFullPaths(data, fieldConfig.Paths, fieldConfig.Separator)
				if !found {
					continue
				}
			}

			switch v := val.(type) {
			case nil:
				switch fieldConfig.FieldType {
				case "uuid_from_longs":
					rawMostSig, _ := LookupNestedMap(fieldConfig.Paths[0], data)
					rawLeastSig, _ := LookupNestedMap(fieldConfig.Paths[1], data)

					invalid := false

					if mostSignN, ok := rawMostSig.(int64); !ok {
						invalid = true
					} else if leastSigN, ok := rawLeastSig.(int64); !ok {
						invalid = true
					} else {
						strings[fieldKey] = utils.NewUUIDFromBits(mostSignN, leastSigN)
					}

					if invalid {
						if defaultValue, ok := fieldConfig.DefaultValue.(string); ok && defaultValue != "" {
							strings[fieldKey] = defaultValue
						}
					}

				}

			case int64:
				ints[fieldKey] = v

			case int32:
				ints[fieldKey] = int64(v)

			case int16:
				ints[fieldKey] = int64(v)

			case int8:
				ints[fieldKey] = int64(v)

			case int:
				ints[fieldKey] = int64(v)

			case time.Time:
				times[fieldKey] = v

			case bool:
				switch fieldConfig.FieldType {
				case "boolean":
					bools[fieldKey] = v
				}

			case string:
				switch fieldConfig.FieldType {
				case "string":
					strings[fieldKey] = v

				case "boolean":
					i, err := strconv.ParseBool(v)
					if err != nil {
						switch db := fieldConfig.DefaultValue.(type) {
						case bool:
							bools[fieldKey] = db
						}
					} else {
						bools[fieldKey] = i
					}

				case "date":
					if v == "now" {
						times[fieldKey] = time.Now().UTC().Truncate(1 * time.Second)
					} else {
						dt, err := time.Parse(fieldConfig.DateFormat, v)
						if err != nil {
							ddt, err := time.Parse(fieldConfig.DateFormat, v)
							if err != nil {
								// TODO: handle this error !
							}
							times[fieldKey] = ddt.UTC().Truncate(1 * time.Second)
						} else {
							times[fieldKey] = dt.UTC().Truncate(1 * time.Second)
						}
					}
				default:
					zap.L().Warn("mapping type configuration unsupported", zap.String("type", fieldConfig.FieldType))
				}
			}
		}
	}
	filteredMsg := TypedDataMessage{
		Ints:    ints,
		Strings: strings,
		Times:   times,
		Bools:   bools,
	}
	return filteredMsg, nil
}

// DecodeDocument returns a DecodedKafkaMessage and contains a map with json decoded data
func (mapper JSONMapperMap) DecodeDocument(_ Message) (Message, error) {
	return nil, errors.New("decodeDocument function is not supported with JSONMapperMap")
}
