package connector

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/buger/jsonparser"
	"github.com/myrteametrics/myrtea-sdk/v4/utils"
)

// JSONMapper :
type JSONMapper struct {
	filters map[string]JSONMapperFilterItem
	mapping map[string]map[string]JSONMapperConfigItem
}

// NewJSONMapper :
func NewJSONMapper(name, path string) (*JSONMapper, error) {
	filters, mapping, err := getConfig(name, path)
	if err != nil {
		return nil, err
	}
	return &JSONMapper{filters: filters, mapping: mapping}, nil
}

// DecodeDocument not implemented here, it only uses msg
func (mapper JSONMapper) DecodeDocument(msg Message) (Message, error) {
	return msg, nil
}

func (mapper JSONMapper) FilterDocument(msg Message) (bool, string) {
	switch message := msg.(type) {
	case KafkaMessage:
		for _, filter := range mapper.filters {
			fieldExtractedValue, _ := getExtractedValue(message.Data, filter.Paths, filter.Separator)
			if fieldExtractedValue == "" {
				if filter.DefaultValue != "" {
					fieldExtractedValue = filter.DefaultValue
				} else {
					return false, fmt.Sprintf("Filter Field missing : %+v", filter)
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
				if !strings.HasPrefix(fieldExtractedValue, filter.Value) {
					return false, fmt.Sprintf("%s not match with Condition=%s, Value=%s", fieldExtractedValue, filter.Condition, filter.Value)
				}
			case "endWith":
				if !strings.HasSuffix(fieldExtractedValue, filter.Value) {
					return false, fmt.Sprintf("%s not match with Condition=%s, Value=%s", fieldExtractedValue, filter.Condition, filter.Value)
				}
			case "contains":
				if !strings.Contains(fieldExtractedValue, filter.Value) {
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
func (mapper JSONMapper) MapToDocument(msg Message) (Message, error) {
	switch message := msg.(type) {
	case KafkaMessage:
		formatedMap := make(map[string]interface{})
		var payload []byte
		for _, groupVal := range mapper.mapping {
			for fieldKey, fieldConfig := range groupVal {
				if fieldConfig.Paths == nil {
					formatedMap[fieldKey] = fieldConfig.DefaultValue
					continue
				}
				payload = message.Data

				var fieldExtractedValue string
				if fieldConfig.FieldType != "uuid_from_longs" {
					fieldExtractedValue, payload = getExtractedValue(message.Data, fieldConfig.Paths, fieldConfig.Separator)
					if fieldExtractedValue == "" && fieldConfig.OtherPaths != nil {
						for _, otherPath := range fieldConfig.OtherPaths {
							fieldExtractedValue, payload = getExtractedValue(message.Data, [][]string{otherPath}, fieldConfig.Separator)
							if fieldExtractedValue != "" {
								break
							}
						}
					}
				}
				// if fieldConfig.DefaultValue != nil {
				// 	fieldExtractedValue = fieldConfig.DefaultValue.(string)
				// }

				if fieldConfig.Mandatory && fieldExtractedValue == "" {
					return FilteredJsonMessage{}, fmt.Errorf("extracted field value is empty : %+v", fieldConfig)
				}

				switch fieldConfig.FieldType {
				case "all_string_in_array":
					var strings = make([]string, 0)
					jsonparser.ArrayEach(payload, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
						str, err := jsonparser.GetString(value, fieldConfig.Paths[0]...)
						if err == nil {
							strings = append(strings, str)
						}
					}, fieldConfig.ArrayPath...)

					if len(strings) > 0 {
						formatedMap[fieldKey] = strings
					}
				case "first_string_in_array":
					var found = false
					var str string

					for _, path := range fieldConfig.Paths {
						jsonparser.ArrayEach(payload, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
							if !found {
								str, err = jsonparser.GetString(value, path...)
								if err == nil {
									found = true
								}
							}
						}, fieldConfig.ArrayPath...)

						if found {
							break
						}
					}

					if !found {
						formatedMap[fieldKey] = fieldConfig.DefaultValue
					} else {
						formatedMap[fieldKey] = str
					}
				case "uuid_from_longs":
					mostSig, _ := getExtractedValueInt64(payload, fieldConfig.Paths[0], ",")
					leastSig, _ := getExtractedValueInt64(payload, fieldConfig.Paths[1], ",")
					if mostSig == 0 || leastSig == 0 {
						formatedMap[fieldKey] = fieldConfig.DefaultValue
					} else {
						formatedMap[fieldKey] = utils.NewUUIDFromBits(mostSig, leastSig)
					}
				case "long":
					l, err := jsonparser.GetInt(payload, fieldConfig.Paths[0]...)
					if err != nil {
						formatedMap[fieldKey] = fieldConfig.DefaultValue
					} else {
						formatedMap[fieldKey] = l
					}
				case "integer":
					i, err := strconv.Atoi(fieldExtractedValue)
					if err != nil {
						formatedMap[fieldKey] = fieldConfig.DefaultValue
					} else {
						formatedMap[fieldKey] = i
					}
				case "boolean":
					boolValue, err := jsonparser.GetBoolean(payload, fieldConfig.Paths[0]...)
					if err != nil {
						formatedMap[fieldKey] = fieldConfig.DefaultValue
					} else {
						formatedMap[fieldKey] = boolValue
					}
				case "date":
					if fieldExtractedValue == "now" {
						formatedMap[fieldKey] = time.Now().UTC().Truncate(1 * time.Second).Format("2006-01-02T15:04:05.000")
					} else {
						dt, err := time.Parse(fieldConfig.DateFormat, fieldExtractedValue)
						if err != nil {
							formatedMap[fieldKey] = fieldConfig.DefaultValue
						} else {
							d := dt.UTC().Truncate(1 * time.Second).Format("2006-01-02T15:04:05.000")
							formatedMap[fieldKey] = d
						}
					}
				default:
					formatedMap[fieldKey] = fieldExtractedValue
				}
			}
		}
		filteredMsg := FilteredJsonMessage{Data: formatedMap}
		return filteredMsg, nil
	default:
		//
		return FilteredJsonMessage{}, errors.New("message type not supported")
	}
}

func getExtractedValue(data []byte, paths [][]string, separator string) (string, []byte) {
	var fieldExtractedValue string
	var err error
	var payload []byte

	for i, path := range paths {
		payload = data

		var firstPath []string
		var count = 0
		var keyBody string
		var value []byte

		//TODO: handle only one wildcard, should handle many of them
		for _, element := range path {
			count++

			if element == wildcard {
				value, _, _, err = jsonparser.Get(payload, firstPath...)
				if err != nil {
					break
				}
				var handler func([]byte, []byte, jsonparser.ValueType, int) error

				handler = func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
					//fmt.Printf("Key: '%s'\n Value: '%s'\n Type: %s\n", string(key), string(value), dataType)
					keyBody = string(key)
					return nil
				}

				//FIXME: Probably a problem with concurrency (the call of the callback)
				jsonparser.ObjectEach(value, handler)

				payload = value
				path = append([]string{keyBody}, path[count:]...)
				firstPath = nil
				break
			}
			firstPath = append(firstPath, element)
		}

		var str string
		str, err = jsonparser.GetString(payload, path...)
		if err == nil {
			if i > 0 {
				fieldExtractedValue += separator
			}
			fieldExtractedValue += str
		}
	}

	return fieldExtractedValue, payload
}

func getExtractedValueInt64(data []byte, path []string, separator string) (int64, []byte) {
	var err error
	var payload []byte

	payload = data

	var firstPath []string
	var count = 0
	var keyBody string
	var value []byte

	//TODO: handle only one wildcard, should handle many of them
	for _, element := range path {
		count++

		if element == wildcard {
			value, _, _, err = jsonparser.Get(payload, firstPath...)
			if err != nil {
				break
			}

			handler := func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
				//fmt.Printf("Key: '%s'\n Value: '%s'\n Type: %s\n", string(key), string(value), dataType)
				keyBody = string(key)
				return nil
			}

			//FIXME: Probably a problem with concurrency (the call of the callback)
			jsonparser.ObjectEach(value, handler)

			payload = value
			path = append([]string{keyBody}, path[count:]...)
			firstPath = nil
			break
		}
		firstPath = append(firstPath, element)
	}

	var val int64
	val, err = jsonparser.GetInt(payload, path...)
	if err != nil {
		fmt.Println(err)
	}

	return val, payload
}
