package helpers

import (
	"flag"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

func GetGeneralConfigKeys() []ConfigKey {
	return []ConfigKey{
		{
			Type: StringFlag, Name: "DEBUG_MODE", DefaultValue: "false",
			Description: "Enable debug mode",
		},
		{
			Type: StringFlag, Name: "LOGGER_PRODUCTION", DefaultValue: "true",
			Description: "Enable or disable production log",
		},
		{
			Type: StringFlag, Name: "INSTANCE_NAME", DefaultValue: "myrtea",
			Description: "Instance name",
		},
	}
}

func GetHTTPServerConfigKeys() []ConfigKey {
	return []ConfigKey{
		{
			Type: StringFlag, Name: "HTTP_SERVER_PORT", DefaultValue: "9000",
			Description: "Server port",
		},
		{
			Type: StringFlag, Name: "HTTP_SERVER_ENABLE_TLS", DefaultValue: "false",
			Description: "Run the server in unsecured mode (without SSL)",
		},
		{
			Type: StringFlag, Name: "HTTP_SERVER_TLS_FILE_CRT", DefaultValue: "certs/server.rsa.crt",
			Description: "SSL certificate crt file location",
		},
		{
			Type: StringFlag, Name: "HTTP_SERVER_TLS_FILE_KEY", DefaultValue: "certs/server.rsa.key",
			Description: "SSL certificate key file location",
		},
		{
			Type: StringFlag, Name: "HTTP_SERVER_API_ENABLE_CORS", DefaultValue: "true",
			Description: "Run the api with CORS enabled",
		},
		{
			Type: StringFlag, Name: "HTTP_SERVER_API_ENABLE_SECURITY", DefaultValue: "true",
			Description: "Run the api in unsecured mode (without authentication)",
		},
		{
			Type: StringFlag, Name: "HTTP_SERVER_API_ENABLE_GATEWAY_MODE", DefaultValue: "false",
			Description: "Run the api without external Auth API (with gateway)",
		},
	}
}

func GetPostgresqlConfigKeys() []ConfigKey {
	return []ConfigKey{
		{
			Type: StringFlag, Name: "POSTGRESQL_HOSTNAME", DefaultValue: "localhost",
			Description: "PostgreSQL hostname",
		},
		{
			Type: StringFlag, Name: "POSTGRESQL_PORT", DefaultValue: "5432",
			Description: "PostgreSQL port",
		},
		{
			Type: StringFlag, Name: "POSTGRESQL_DBNAME", DefaultValue: "postgres",
			Description: "PostgreSQL database name",
		},
		{
			Type: StringFlag, Name: "POSTGRESQL_USERNAME", DefaultValue: "postgres",
			Description: "PostgreSQL user",
		},
		{
			Type: StringFlag, Name: "POSTGRESQL_PASSWORD", DefaultValue: "postgres",
			Description: "PostgreSQL pasword",
		},
	}
}

func GetElasticsearchConfigKeys() []ConfigKey {
	return []ConfigKey{
		{
			Type: StringFlag, Name: "ELASTICSEARCH_VERSION", DefaultValue: "6",
			Description: "Elasticsearch major version",
		},
		{
			Type: StringSliceFlag, Name: "ELASTICSEARCH_URLS", DefaultValue: []string{"http://localhost:9200"},
			Description: "Elasticsearch URLS",
		},
	}
}

func GetConsumerKafkaConfigKeys() []ConfigKey {
	return []ConfigKey{
		{
			Type: StringFlag, Name: "KAFKA_CONSUMER_VERBOSE", DefaultValue: "false",
			Description: "Enable debug mode",
		},
		{
			Type: StringFlag, Name: "KAFKA_CONSUMER_CLIENTID", DefaultValue: "",
			Description: "Enable debug mode",
		},
		{
			Type: StringSliceFlag, Name: "KAFKA_CONSUMER_BROKERS", DefaultValue: []string{},
			Description: "Enable debug mode",
		},
		{
			Type: StringFlag, Name: "KAFKA_CONSUMER_GROUPID", DefaultValue: "",
			Description: "Enable debug mode",
		},
		{
			Type: StringSliceFlag, Name: "KAFKA_CONSUMER_TOPICS", DefaultValue: []string{},
			Description: "Enable debug mode",
		},
		{
			Type: StringFlag, Name: "KAFKA_CONSUMER_OFFSET_OLDEST", DefaultValue: "false",
			Description: "Enable debug mode",
		},
		{
			Type: StringFlag, Name: "KAFKA_SCHEMA_REGISTRY_URL", DefaultValue: "",
			Description: "Enable debug mode",
		},
		{
			Type: StringFlag, Name: "KAFKA_SCHEMA_REGISTRY_TTL_DURATION", DefaultValue: "24",
			Description: "Enable debug mode",
		},
	}
}

func GetSinkIngesterConfigKeys() []ConfigKey {
	return []ConfigKey{
		{
			Type: StringFlag, Name: "SINK_INGESTER_API_HOST", DefaultValue: "localhost",
			Description: "Ingester-API host",
		},
		{
			Type: StringFlag, Name: "SINK_INGESTER_API_PORT", DefaultValue: "9001",
			Description: "Ingester-API port",
		},
		{
			Type: StringFlag, Name: "SINK_INGESTER_API_DRY_RUN", DefaultValue: "false",
			Description: "Ingester-API port",
		},
		{
			Type: StringFlag, Name: "SINK_HTTP_TIMEOUT", DefaultValue: "10s",
			Description: "HTTP client timeout for the Ingester sink",
		},
		{
			Type: StringFlag, Name: "SINK_BUFFER_SIZE", DefaultValue: "100",
			Description: "Sink buffer length",
		},
		{
			Type: StringFlag, Name: "SINK_FLUSH_TIMEOUT", DefaultValue: "10s",
			Description: "Flush timout for the Sink buffer",
		},
	}
}

func GetSinkEngineConfigKeys() []ConfigKey {
	return []ConfigKey{}
}

func GetMetricsConfigKeys() []ConfigKey {
	return []ConfigKey{
		// {
		// 	Type: StringFlag, Name: "METRICS_STATSD_ADDRESS", DefaultValue: "",
		// 	Description: "Address of the statsd server used by the back-end service",
		// },
		// {
		// 	Type: StringFlag, Name: "METRICS_STATSD_PREFIX", DefaultValue: "",
		// 	Description: "prefix is used by statsd to aggregate the metrics of the current connector",
		// },
	}
}

func GetConnectorConfigKeys() []ConfigKey {
	return []ConfigKey{
		{
			Type: StringFlag, Name: "ENGINE_API_KEY", DefaultValue: "",
			Description: "API Key that the engine uses to interact with this component",
		},
	}
}

func GetRedisConfigKeys() []ConfigKey {
	return []ConfigKey{
		{
			Type: StringFlag, Name: "REDIS_HOSTS", DefaultValue: []string{"http://localhost:6379"},
			Description: "Redis hostname(s)",
		},
		{
			Type: StringFlag, Name: "REDIS_PASSWORD", DefaultValue: "",
			Description: "Redis password",
		},
		{
			Type: StringFlag, Name: "REDIS_USE_RESP3", DefaultValue: "true",
			Description: "Use RESP3 protocol",
		},
		{
			Type: StringFlag, Name: "REDIS_CLIENT_CACHE", DefaultValue: "false",
			Description: "Enable client cache (only available when RESP3 is enabled)",
		},
	}
}

// ConfigKey represents the definition of a config flag.
type ConfigKey struct {
	Type         ConfigKeyType
	Name         string
	DefaultValue interface{}
	Description  string
}

// ConfigKeyType defines all configuration key types.
type ConfigKeyType int

const (
	// StringFlag key for key-value flag.
	StringFlag ConfigKeyType = iota + 1
	// StringSliceFlag for key-[values] flag.
	StringSliceFlag
)

// InitializeConfig ...
func InitializeConfig(allowedConfigKeys [][]ConfigKey, configName, configPath, envPrefix string) {
	// Initialize external toml configuration
	viper.SetConfigName(configName)
	viper.AddConfigPath(configPath)

	err := viper.ReadInConfig()
	if err != nil {
		zap.L().Fatal("Cannot read viper configuration")
	}

	// Initialize environment variables configuration
	viper.SetEnvPrefix(envPrefix)
	bindEnv(allowedConfigKeys)
	bindPFlag(allowedConfigKeys)

	viperDebugFlag := viper.AllSettings()
	delete(viperDebugFlag, "test") // Remove golang standard test flags for cleaner debug
}

func bindEnv(allowedConfigKeys [][]ConfigKey) {
	for _, allowedConfigKeyGroup := range allowedConfigKeys {
		for _, configKey := range allowedConfigKeyGroup {
			err := viper.BindEnv(configKey.Name)
			if err != nil {
				zap.L().Fatal("Cannot bind viper configkey", zap.String("name", configKey.Name))
			}
		}
	}
}

func bindPFlag(allowedConfigKeys [][]ConfigKey) {
	flagSet := pflag.NewFlagSet("myrtea", pflag.ExitOnError)
	// Initialize flags configuration
	for _, allowedConfigKeyGroup := range allowedConfigKeys {
		for _, configKey := range allowedConfigKeyGroup {
			switch configKey.Type {
			case StringFlag:
				if val, ok := configKey.DefaultValue.(string); ok {
					flagSet.String(configKey.Name, val, configKey.Description)
				}
			case StringSliceFlag:
				if val, ok := configKey.DefaultValue.([]string); ok {
					flagSet.StringSlice(configKey.Name, val, configKey.Description)
				}
			}
		}
	}

	pflag.CommandLine.AddFlagSet(flagSet)
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()

	err := viper.BindPFlags(pflag.CommandLine)
	if err != nil {
		zap.L().Fatal("Cannot bind viper pflags")
	}
}
