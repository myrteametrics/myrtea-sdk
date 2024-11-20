package connector

import (
	"bytes"
	"encoding/json" // for json types
	"errors"
	"fmt"
	"strconv"
	str "strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/myrteametrics/myrtea-sdk/v5/utils"
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

// FilterDocument checks if document is filtered or not, returns if documents valid and if invalid, the following reason
func (mapper JSONMapperJsoniter) FilterDocument(msg Message) (bool, string) {
	var data map[string]interface{}

	switch message := msg.(type) {
	case KafkaMessage:
		// don't handle message if there's no filters
		if len(mapper.filters) == 0 {
			return true, ""
		}

		d := jsoni.NewDecoder(bytes.NewBuffer(message.Data))
		d.UseNumber()

		if err := d.Decode(&data); err != nil {
			zap.L().Error("decode", zap.Error(err))
		}
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
func (mapper JSONMapperJsoniter) MapToDocument(msg Message) (Message, error) {
	var data map[string]interface{}

	switch message := msg.(type) {
	case KafkaMessage:
		d := jsoni.NewDecoder(bytes.NewBuffer(message.Data))
		d.UseNumber()

		if err := d.Decode(&data); err != nil {
			zap.L().Error("decode", zap.Error(err))
		}
	case DecodedKafkaMessage:
		data = message.Data
	default:
		return TypedDataMessage{}, errors.New("message type not supported")
	}

	strings := make(map[string]string)
	ints := make(map[string]int64)
	bools := make(map[string]bool)
	times := make(map[string]time.Time)
	floats := make(map[string]float64)

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

					if mostSigN, ok := rawMostSig.(json.Number); !ok {
						invalid = true
					} else if leastSigN, ok := rawLeastSig.(json.Number); !ok {
						invalid = true
					} else {
						if mostSig, err := mostSigN.Int64(); err != nil {
							invalid = true
							// TODO: handle this case !
							// strings[fieldKey] = uuid.UUID{}.String()
							zap.L().Warn("cannot convert to int64", zap.String("path", str.Join(fieldConfig.Paths[0], ".")), zap.Error(err))
						} else if leastSig, err := leastSigN.Int64(); err != nil {
							invalid = true
							// TODO: handle this case !
							// strings[fieldKey] = uuid.UUID{}.String()
							zap.L().Warn("cannot convert to int64", zap.String("path", str.Join(fieldConfig.Paths[1], ".")), zap.Error(err))
						} else {
							strings[fieldKey] = utils.NewUUIDFromBits(mostSig, leastSig)
						}
					}

					if invalid {
						if defaultValue, ok := fieldConfig.DefaultValue.(string); ok && defaultValue != "" {
							strings[fieldKey] = defaultValue
						}
					}

				}

			case json.Number:
				switch fieldConfig.FieldType {
				case "long":
				case "int":
					if i, err := v.Int64(); err != nil {
						switch di := fieldConfig.DefaultValue.(type) {
						case int64:
							ints[fieldKey] = di
						}
					} else {
						ints[fieldKey] = i
					}
				case "float":
					if i, err := v.Float64(); err != nil {
						switch di := fieldConfig.DefaultValue.(type) {
						case float64:
							floats[fieldKey] = di
						}
					} else {
						floats[fieldKey] = i
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
		Floats:  floats,
	}
	return filteredMsg, nil
}

// DecodeDocument returns a DecodedKafkaMessage and contains a map with json decoded data
func (mapper JSONMapperJsoniter) DecodeDocument(msg Message) (Message, error) {
	switch message := msg.(type) {
	case KafkaMessage:
		decodedKafkaMsg := DecodedKafkaMessage{}

		d := jsoni.NewDecoder(bytes.NewBuffer(message.Data))
		d.UseNumber()

		if err := d.Decode(&decodedKafkaMsg.Data); err != nil {
			zap.L().Error("decode", zap.Error(err))
		}

		return decodedKafkaMsg, nil
	default:
		//
		return TypedDataMessage{}, errors.New("message type not supported")
	}
}
