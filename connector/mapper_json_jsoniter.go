package connector

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	str "strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/myrteametrics/myrtea-sdk/v4/utils"
	"go.uber.org/zap"
)

var jsoni = jsoniter.ConfigCompatibleWithStandardLibrary

// JSONMapperJsoniter :
type JSONMapperJsoniter struct {
	filters map[string]JSONMapperFilterItem
	mapping map[string]map[string]JSONMapperConfigItem
}

// NewJSONMapperJsoniter :
func NewJSONMapperJsoniter(name, path string) (*JSONMapperJsoniter, error) {
	filters, mapping, err := getConfig(name, path)
	if err != nil {
		return nil, err
	}
	return &JSONMapperJsoniter{filters: filters, mapping: mapping}, nil
}

func (mapper JSONMapperJsoniter) FilterDocument(msg Message) (bool, string) {
	switch message := msg.(type) {
	case KafkaMessage:
		for _, filter := range mapper.filters {
			fieldExtractedValueRaw, found := lookupNestedMapFullPaths(message.Data, filter.Paths, filter.Separator)
			fieldExtractedValue := ""
			if !found || fieldExtractedValueRaw == "" {
				if filter.DefaultValue != "" {
					fieldExtractedValue = filter.DefaultValue
				} else {
					return false, fmt.Sprintf("Filter Field missing : %+v", filter)
				}
			} else {
				var ok bool
				fieldExtractedValue, ok = fieldExtractedValueRaw.(string)

				if !ok {
					return false, fmt.Sprintf("Filter Field could't be cast to string : %+v", filter)
				}
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
	default:
		return false, "message type not supported"
	}
}

// MapAvroToDocument :
func (mapper JSONMapperJsoniter) MapToDocument(msg Message) (Message, error) {
	switch message := msg.(type) {
	case KafkaMessage:

		var data map[string]interface{}
		// if err := jsoni.Unmarshal(message.Data, &data) ; err != nil {
		// 	zap.L().Error("unmarshall", zap.Error(err))
		// }

		d := jsoni.NewDecoder(bytes.NewBuffer(message.Data))
		d.UseNumber()

		if err := d.Decode(&data); err != nil {
			zap.L().Error("decode", zap.Error(err))
		}

		strings := make(map[string]string)
		ints := make(map[string]int64)
		bools := make(map[string]bool)
		times := make(map[string]time.Time)

		for _, groupVal := range mapper.mapping {
			for fieldKey, fieldConfig := range groupVal {
				// if fieldConfig.Paths == nil {
				// 	formatedMap[fieldKey] = fieldConfig.DefaultValue
				// 	continue
				// }

				var val interface{}
				if fieldConfig.FieldType != "uuid_from_longs" {
					var found bool
					val, found = lookupNestedMapFullPaths(data, fieldConfig.Paths, fieldConfig.Separator)
					if !found {
						continue
					}
				}

				switch v := val.(type) {
				case nil:
					switch fieldConfig.FieldType {
					case "uuid_from_longs":
						rawMostSig, _ := lookupNestedMap(fieldConfig.Paths[0], data)
						rawLeastSig, _ := lookupNestedMap(fieldConfig.Paths[1], data)
						if mostSigN, ok := rawMostSig.(json.Number); !ok {
							// TODO: handle this case !
							// strings[fieldKey] = uuid.UUID{}.String()
							zap.L().Warn("rawMostSig not json.Number", zap.String("path", str.Join(fieldConfig.Paths[0], ".")))
						} else if leastSigN, ok := rawLeastSig.(json.Number); !ok {
							// TODO: handle this case !
							// strings[fieldKey] = uuid.UUID{}.String()
							zap.L().Warn("rawLeastSig not json.Number", zap.String("path", str.Join(fieldConfig.Paths[1], ".")))
						} else {
							if mostSig, err := mostSigN.Int64(); err != nil {
								// TODO: handle this case !
								// strings[fieldKey] = uuid.UUID{}.String()
								zap.L().Warn("cannot convert to int64", zap.String("path", str.Join(fieldConfig.Paths[0], ".")), zap.Error(err))
							} else if leastSig, err := leastSigN.Int64(); err != nil {
								// TODO: handle this case !
								// strings[fieldKey] = uuid.UUID{}.String()
								zap.L().Warn("cannot convert to int64", zap.String("path", str.Join(fieldConfig.Paths[1], ".")), zap.Error(err))
							} else {
								strings[fieldKey] = utils.NewUUIDFromBits(mostSig, leastSig)
							}
						}
					}

				case json.Number:
					switch fieldConfig.FieldType {
					case "int":
						if i, err := v.Int64(); err != nil {
							switch di := fieldConfig.DefaultValue.(type) {
							case int64:
								ints[fieldKey] = di
							}
						} else {
							ints[fieldKey] = i
						}
					}

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
	default:
		//
		return TypedDataMessage{}, errors.New("message type not supported")
	}
}

// lookupNestedMapFullPaths Looks searches value of all paths in data and concatenates them with a separator
func lookupNestedMapFullPaths(data interface{}, paths [][]string, separator string) (interface{}, bool) {
	if len(paths) == 0 {
		return nil, false
	}

	val, found := lookupNestedMap(paths[0], data)
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

			val, found = lookupNestedMap(path, data)
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

// lookupNestedMap lookup for a value corresponding to the exact specified path inside a map
func lookupNestedMap(pathParts []string, data interface{}) (interface{}, bool) {
	if len(pathParts) == 0 {
		return data, true
	}

	searchField := pathParts[0]

	switch v := data.(type) {
	case map[string]interface{}:
		if searchField != "*" {
			if val, found := v[searchField]; found {
				return lookupNestedMap(pathParts[1:], val)
			}
		} else {
			for _, l := range v {
				if val, found := lookupNestedMap(pathParts[1:], l); found {
					return val, found
				}
			}
		}
	case []interface{}:
		// this code stays here for performance improvements (0 index is mostly used)
		if searchField == "[0]" && len(v) > 0 {
			return lookupNestedMap(pathParts[1:], v[0])
		}

		// Check if searchField is in the form of "[...]"
		if len(searchField) > 2 && searchField[0] == '[' && searchField[len(searchField)-1] == ']' {
			// Extract the index as a string and convert it to an integer
			indexStr := searchField[1 : len(searchField)-1]
			index, err := strconv.Atoi(indexStr)
			if err == nil && index >= 0 && index < len(v) {
				return lookupNestedMap(pathParts[1:], v[index])
			}
		}
	}

	return nil, false
}
