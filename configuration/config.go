package configuration

import (
	"flag"

	"github.com/alecthomas/repr"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

// ConfigKey represents the definition of a config flag
type ConfigKey struct {
	Type         ConfigKeyType
	Name         string
	DefaultValue interface{}
	Description  string
}

// ConfigKeyType defines all configuration key types
type ConfigKeyType int

const (
	// StringFlag key for key-value flag
	StringFlag ConfigKeyType = iota + 1
	// StringSliceFlag for key-[values] flag
	StringSliceFlag
)

// InitializeConfig ...
func InitializeConfig(allowedConfigKey []ConfigKey, configName, configPath, envPrefix string) {
	zap.L().Info("Initialize Viper config")

	// Initialize external toml configuration
	viper.SetConfigName(configName)
	viper.AddConfigPath(configPath)
	viper.ReadInConfig()
	viperDebugFile := viper.AllSettings()

	// Initialize environment variables configuration
	viper.SetEnvPrefix(envPrefix)
	for _, configKey := range allowedConfigKey {
		viper.BindEnv(configKey.Name)
	}
	viperDebugEnv := viper.AllSettings()

	// Initialize flags configuration
	for _, configKey := range allowedConfigKey {
		switch configKey.Type {
		case StringFlag:
			pflag.String(configKey.Name, configKey.DefaultValue.(string), configKey.Description)
		case StringSliceFlag:
			pflag.StringSlice(configKey.Name, configKey.DefaultValue.([]string), configKey.Description)
		}
	}
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()
	viper.BindPFlags(pflag.CommandLine)
	viperDebugFlag := viper.AllSettings()
	delete(viperDebugFlag, "test") // Remove golang standarad test flags for cleaner debug

	if viper.GetBool("DEBUG_MODE") {
		// repr.Println("config/api.toml Configuration", viperDebugFile)
		repr.Println(configPath, "/", configName, ".toml Configuration", viperDebugFile)
		repr.Println("Environment variables Configuration", viperDebugEnv)
		repr.Println("Flag variables Configuration", viperDebugFlag)
	}
}
