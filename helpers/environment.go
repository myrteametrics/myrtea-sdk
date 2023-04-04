package helpers

import (
	"flag"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var (
	GeneralConfigKeys = []ConfigKey{
		{Type: StringFlag, Name: "DEBUG_MODE", DefaultValue: "false", Description: "Enable debug mode"},
		{Type: StringFlag, Name: "LOGGER_PRODUCTION", DefaultValue: "true", Description: "Enable or disable production log"},
		{Type: StringFlag, Name: "INSTANCE_NAME", DefaultValue: "myrtea", Description: "Instance name"},
	}

	HTTPServerConfigKeys = []ConfigKey{
		{Type: StringFlag, Name: "HTTP_SERVER_PORT", DefaultValue: "9000", Description: "Server port"},
		{Type: StringFlag, Name: "HTTP_SERVER_ENABLE_TLS", DefaultValue: "false", Description: "Run the server in unsecured mode (without SSL)"},
		{Type: StringFlag, Name: "HTTP_SERVER_TLS_FILE_CRT", DefaultValue: "certs/server.rsa.crt", Description: "SSL certificate crt file location"},
		{Type: StringFlag, Name: "HTTP_SERVER_TLS_FILE_KEY", DefaultValue: "certs/server.rsa.key", Description: "SSL certificate key file location"},
		{Type: StringFlag, Name: "HTTP_SERVER_API_ENABLE_CORS", DefaultValue: "true", Description: "Run the api with CORS enabled"},
		{Type: StringFlag, Name: "HTTP_SERVER_API_ENABLE_SECURITY", DefaultValue: "true", Description: "Run the api in unsecured mode (without authentication)"},
		{Type: StringFlag, Name: "HTTP_SERVER_API_ENABLE_GATEWAY_MODE", DefaultValue: "false", Description: "Run the api without external Auth API (with gateway)"},
	}

	PostgresqlConfigKeys = []ConfigKey{
		{Type: StringFlag, Name: "POSTGRESQL_HOSTNAME", DefaultValue: "localhost", Description: "PostgreSQL hostname"},
		{Type: StringFlag, Name: "POSTGRESQL_PORT", DefaultValue: "5432", Description: "PostgreSQL port"},
		{Type: StringFlag, Name: "POSTGRESQL_DBNAME", DefaultValue: "postgres", Description: "PostgreSQL database name"},
		{Type: StringFlag, Name: "POSTGRESQL_USERNAME", DefaultValue: "postgres", Description: "PostgreSQL user"},
		{Type: StringFlag, Name: "POSTGRESQL_PASSWORD", DefaultValue: "postgres", Description: "PostgreSQL pasword"},
	}

	ElasticsearchConfigKeys = []ConfigKey{
		{Type: StringFlag, Name: "ELASTICSEARCH_VERSION", DefaultValue: "6", Description: "Elasticsearch major version"},
		{Type: StringSliceFlag, Name: "ELASTICSEARCH_URLS", DefaultValue: []string{"http://localhost:9200"}, Description: "Elasticsearch URLS"},
	}

	ConsumerKafkaConfigKeys = []ConfigKey{
		{Type: StringFlag, Name: "KAFKA_CONSUMER_VERBOSE", DefaultValue: "false", Description: "Enable debug mode"},
		{Type: StringFlag, Name: "KAFKA_CONSUMER_CLIENTID", DefaultValue: "", Description: "Enable debug mode"},
		{Type: StringSliceFlag, Name: "KAFKA_CONSUMER_BROKERS", DefaultValue: []string{}, Description: "Enable debug mode"},
		{Type: StringFlag, Name: "KAFKA_CONSUMER_GROUPID", DefaultValue: "", Description: "Enable debug mode"},
		{Type: StringSliceFlag, Name: "KAFKA_CONSUMER_TOPICS", DefaultValue: []string{}, Description: "Enable debug mode"},
		{Type: StringFlag, Name: "KAFKA_CONSUMER_OFFSET_OLDEST", DefaultValue: "false", Description: "Enable debug mode"},
		{Type: StringFlag, Name: "KAFKA_SCHEMA_REGISTRY_URL", DefaultValue: "", Description: "Enable debug mode"},
		{Type: StringFlag, Name: "KAFKA_SCHEMA_REGISTRY_TTL_DURATION", DefaultValue: "24", Description: "Enable debug mode"},
	}

	SinkIngesterConfigKeys = []ConfigKey{
		{Type: StringFlag, Name: "SINK_INGESTER_API_HOST", DefaultValue: "localhost", Description: "Ingester-API host"},
		{Type: StringFlag, Name: "SINK_INGESTER_API_PORT", DefaultValue: "9001", Description: "Ingester-API port"},
		{Type: StringFlag, Name: "SINK_INGESTER_API_DRY_RUN", DefaultValue: "false", Description: "Ingester-API port"},
		{Type: StringFlag, Name: "SINK_HTTP_TIMEOUT", DefaultValue: "", Description: "HTTP client timeout for the Ingester sink"},
		{Type: StringFlag, Name: "SINK_BUFFER_SIZE", DefaultValue: "", Description: "Sink buffer lenght"},
		{Type: StringFlag, Name: "SINK_FLUSH_TIMEOUT", DefaultValue: "", Description: "Flush timout for the Sink buffer"},
	}

	SinkEngineConfigKeys = []ConfigKey{}

	MetricsConfigKeys = []ConfigKey{
		// 	{Type: StringFlag, Name: "METRICS_STATSD_ADDRESS", DefaultValue: "", Description: "Address of the statsd server used by the back-end service"},
		// 	{Type: StringFlag, Name: "METRICS_STATSD_PREFIX", DefaultValue: "", Description: "prefix is used by statsd to aggregate the metrics of the current connector"},
	}
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
func InitializeConfig(allowedConfigKeys [][]ConfigKey, configName, configPath, envPrefix string) {
	zap.L().Info("Initialize Viper config")

	// Initialize external toml configuration
	viper.SetConfigName(configName)
	viper.AddConfigPath(configPath)
	viper.ReadInConfig()

	// Initialize environment variables configuration
	viper.SetEnvPrefix(envPrefix)
	for _, allowedConfigKeyGroup := range allowedConfigKeys {
		for _, configKey := range allowedConfigKeyGroup {
			viper.BindEnv(configKey.Name)
		}
	}

	// Initialize flags configuration
	for _, allowedConfigKeyGroup := range allowedConfigKeys {
		for _, configKey := range allowedConfigKeyGroup {
			switch configKey.Type {
			case StringFlag:
				pflag.String(configKey.Name, configKey.DefaultValue.(string), configKey.Description)
			case StringSliceFlag:
				pflag.StringSlice(configKey.Name, configKey.DefaultValue.([]string), configKey.Description)
			}
		}
	}

	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()
	viper.BindPFlags(pflag.CommandLine)
	viperDebugFlag := viper.AllSettings()
	delete(viperDebugFlag, "test") // Remove golang standarad test flags for cleaner debug
}
