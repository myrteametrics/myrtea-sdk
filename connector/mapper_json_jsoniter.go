package connector

import (
	"errors"
	"strconv"
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
	return true, ""
}

// MapAvroToDocument :
func (mapper JSONMapperJsoniter) MapToDocument(msg Message) (Message, error) {
	switch message := msg.(type) {
	case KafkaMessage:

		var data map[string]interface{}
		err := jsoni.Unmarshal(message.Data, &data)
		if err != nil {
			zap.L().Error("unmarshall", zap.Error(err))
		}

		strings := make(map[string]string, 0)
		ints := make(map[string]int64, 0)
		bools := make(map[string]bool, 0)
		times := make(map[string]time.Time, 0)

		for _, groupVal := range mapper.mapping {
			for fieldKey, fieldConfig := range groupVal {
				// if fieldConfig.Paths == nil {
				// 	formatedMap[fieldKey] = fieldConfig.DefaultValue
				// 	continue
				// }

				var val interface{}
				if fieldConfig.FieldType != "uuid_from_longs" {
					var found bool
					val, found = lookupNestedMap(fieldConfig.Paths[0], data)
					if !found {
						continue
					}

					if len(fieldConfig.Paths) > 1 {
						var str string = ""
						for i, path := range fieldConfig.Paths {
							if i > 0 {
								str += fieldConfig.Separator
							}

							val, found := lookupNestedMap(path, data)
							if !found {
								continue
							} else {
								switch v := val.(type) {
								case string:
									str += v
								}
							}
						}
						val = str
					}
				}

				switch v := val.(type) {
				case nil:
					switch fieldConfig.FieldType {
					case "uuid_from_longs":
						rawMostSig, _ := lookupNestedMap(fieldConfig.Paths[0], data)
						rawLeastSig, _ := lookupNestedMap(fieldConfig.Paths[1], data)
						if mostSig, ok := rawMostSig.(float64); !ok || mostSig == 0 {
							// TODO: handle this case !
							// strings[fieldKey] = uuid.UUID{}.String()
						} else if leastSig, ok := rawLeastSig.(float64); !ok || leastSig == 0 {
							// TODO: handle this case !
							// strings[fieldKey] = uuid.UUID{}.String()
						} else {
							strings[fieldKey] = utils.NewUUIDFromBits(int64(mostSig), int64(leastSig))
						}
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
					case "int":
						i, err := strconv.ParseInt(v, 10, 0)
						if err != nil {
							switch di := fieldConfig.DefaultValue.(type) {
							case int64:
								ints[fieldKey] = di
							}
						} else {
							ints[fieldKey] = i
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
		}
		return filteredMsg, nil
	default:
		//
		return TypedDataMessage{}, errors.New("message type not supported")
	}
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
		if searchField == "[0]" && len(v) > 0 {
			return lookupNestedMap(pathParts[1:], v[0])
		}
	}
	return nil, false
}
