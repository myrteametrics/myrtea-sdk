package connector

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/buger/jsonparser"
	"github.com/mitchellh/mapstructure"
	"github.com/myrteametrics/myrtea-sdk/v4/utils"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

const wildcard = "*"

// ConfigItem :
type ConfigItem struct {
	Mandatory    bool
	FieldType    string
	DefaultValue interface{}
	DateFormat   string
	Paths        [][]string
	OtherPaths   [][]string
	ArrayPath    []string
	Separator    string
}

type FilterItem struct {
	Keep         bool
	FieldType    string
	DefaultValue string
	Paths        [][]string
	Condition    string
	Value        string
	Values       []string
	Separator    string
}

// Mapper :
type Mapper struct {
	filters map[string]FilterItem
	mapping map[string]map[string]ConfigItem
}

//InitAvroMapper init avro mapper
func InitAvroMapper(path string, file string) *Mapper {
	mapper, err := New(file, path)
	if err != nil {
		zap.L().Error("Couldn't init the mapper", zap.Error(err))
		os.Exit(1)
	}
	return mapper
}

// New :
func New(name, path string) (*Mapper, error) {
	filters, mapping, err := getConfig(name, path)
	if err != nil {
		return nil, err
	}
	return &Mapper{filters: filters, mapping: mapping}, nil
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

	var val int64
	val, err = jsonparser.GetInt(payload, path...)
	if err != nil {
		fmt.Println(err)
	}

	return val, payload
}

func (mapper Mapper) FilterDocument(msg KafkaMessage) (bool, string) {
	for _, filter := range mapper.filters {
		fieldExtractedValue, _ := getExtractedValue(msg.Data, filter.Paths, filter.Separator)
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
}

// MapAvroToDocument :
func (mapper Mapper) MapAvroToDocument(msg KafkaMessage) (FilteredJsonMessage, error) {

	formatedMap := make(map[string]interface{})
	var payload []byte
	for _, groupVal := range mapper.mapping {
		for fieldKey, fieldConfig := range groupVal {
			if fieldConfig.Paths == nil {
				formatedMap[fieldKey] = fieldConfig.DefaultValue
				continue
			}
			payload = msg.Data

			var fieldExtractedValue string
			if fieldConfig.FieldType != "uuid_from_longs" {
				fieldExtractedValue, payload = getExtractedValue(msg.Data, fieldConfig.Paths, fieldConfig.Separator)
				if fieldExtractedValue == "" && fieldConfig.OtherPaths != nil {
					for _, otherPath := range fieldConfig.OtherPaths {
						fieldExtractedValue, payload = getExtractedValue(msg.Data, [][]string{otherPath}, fieldConfig.Separator)
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
}

// with Fixed GetStringSlice key ("keys_list.keys" instead of "mapping.keys")
func getConfig(name, path string) (map[string]FilterItem, map[string]map[string]ConfigItem, error) {
	viperMapper := viper.New()
	viperMapper.SetConfigName(name)
	viperMapper.AddConfigPath(path)

	err := viperMapper.ReadInConfig()
	if err != nil {
		zap.L().Error("getConfig.ReadInConfig:", zap.Error(err))
		return nil, nil, err
	}

	filtersConfig := make(map[string]FilterItem)
	mapConfig := make(map[string]map[string]ConfigItem)
	for groupKey := range viperMapper.AllSettings() {
		if groupKey == "filter" {
			for fieldKey, fieldConfig := range viperMapper.GetStringMap(groupKey) {
				itemConfig := FilterItem{}
				err := mapstructure.Decode(fieldConfig, &itemConfig)
				if err != nil {
					zap.L().Fatal("Cannot decode config file ", zap.Error(err))
					return nil, nil, err
				}
				filtersConfig[fieldKey] = itemConfig
			}
		} else {
			mapConfig[groupKey] = make(map[string]ConfigItem)
			for fieldKey, fieldConfig := range viperMapper.GetStringMap(groupKey) {
				itemConfig := ConfigItem{}
				err := mapstructure.Decode(fieldConfig, &itemConfig)
				if err != nil {
					zap.L().Fatal("Cannot decode config file ", zap.Error(err))
					return nil, nil, err
				}
				mapConfig[groupKey][fieldKey] = itemConfig
			}
		}
	}

	return filtersConfig, mapConfig, nil
}
