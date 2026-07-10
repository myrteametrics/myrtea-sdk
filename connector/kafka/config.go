// Package kafka provides franz-go (github.com/twmb/franz-go) based building
// blocks for consuming and producing Kafka messages.
//
// It is the modern replacement for the sarama-based helpers located in the
// connector package (DefaultConsumer, DefaultMultiConsumer, SaramaLogger,
// FilterHeaders, ...). Both implementations can coexist so projects can migrate
// incrementally: pick the package that matches the client library you use.
package kafka

import (
	"crypto/tls"

	"github.com/spf13/viper"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"go.uber.org/zap"
)

// SASLConfig holds optional SASL/PLAIN over TLS authentication settings shared
// by both the consumer Handler and the Producer.
type SASLConfig struct {
	Enabled  bool
	User     string
	Password string
	// TLSMinVersion is the minimum TLS version used when Enabled is true.
	// Defaults to tls.VersionTLS12 when zero.
	TLSMinVersion uint16
}

// apply appends the SASL/TLS related options to opts when authentication is
// enabled and returns the resulting slice.
func (c SASLConfig) apply(opts []kgo.Opt) []kgo.Opt {
	if !c.Enabled {
		return opts
	}

	opts = append(opts, kgo.SASL(plain.Auth{
		User: c.User,
		Pass: c.Password,
	}.AsMechanism()))

	minVersion := c.TLSMinVersion
	if minVersion == 0 {
		minVersion = tls.VersionTLS12
	}
	opts = append(opts, kgo.DialTLSConfig(&tls.Config{MinVersion: minVersion}))

	zap.L().Info("Kafka: Using SASL/TLS authentication", zap.String("user", c.User))
	return opts
}

// SASLFromViper reads the standard KAFKA_SASL_* keys from viper. It mirrors the
// configuration used across myrtea connectors:
//
//	KAFKA_SASL_AUTH      (bool)
//	KAFKA_SASL_USER      (string)
//	KAFKA_SASL_PASSWORD  (string)
func SASLFromViper() SASLConfig {
	return SASLConfig{
		Enabled:  viper.GetBool("KAFKA_SASL_AUTH"),
		User:     viper.GetString("KAFKA_SASL_USER"),
		Password: viper.GetString("KAFKA_SASL_PASSWORD"),
	}
}
