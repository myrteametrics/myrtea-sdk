package connector

import (
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

const wildcard = "*"

// JSONMapperConfigItem :
type JSONMapperConfigItem struct {
	Mandatory    bool
	FieldType    string
	DefaultValue interface{}
	DateFormat   string
	Paths        [][]string
	OtherPaths   [][]string
	ArrayPath    []string
	Separator    string
}

type JSONMapperFilterItem struct {
	Keep         bool
	FieldType    string
	DefaultValue string
	Paths        [][]string
	Condition    string
	Value        string
	Values       []string
	Separator    string
}

// with Fixed GetStringSlice key ("keys_list.keys" instead of "mapping.keys")
func getConfig(name, path string) (map[string]JSONMapperFilterItem, map[string]map[string]JSONMapperConfigItem, error) {
	viperMapper := viper.New()
	viperMapper.SetConfigName(name)
	viperMapper.AddConfigPath(path)

	err := viperMapper.ReadInConfig()
	if err != nil {
		zap.L().Error("getConfig.ReadInConfig:", zap.Error(err))
		return nil, nil, err
	}

	filtersConfig := make(map[string]JSONMapperFilterItem)
	mapConfig := make(map[string]map[string]JSONMapperConfigItem)
	for groupKey := range viperMapper.AllSettings() {
		if groupKey == "filter" {
			for fieldKey, fieldConfig := range viperMapper.GetStringMap(groupKey) {
				itemConfig := JSONMapperFilterItem{}
				err := mapstructure.Decode(fieldConfig, &itemConfig)
				if err != nil {
					zap.L().Fatal("Cannot decode config file ", zap.Error(err))
					return nil, nil, err
				}
				filtersConfig[fieldKey] = itemConfig
			}
		} else {
			mapConfig[groupKey] = make(map[string]JSONMapperConfigItem)
			for fieldKey, fieldConfig := range viperMapper.GetStringMap(groupKey) {
				itemConfig := JSONMapperConfigItem{}
				err = mapstructure.Decode(fieldConfig, &itemConfig)
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
