package kafka

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

// KgoLogger implements the kgo.Logger interface on top of a zap.Logger.
type KgoLogger struct {
	logger *zap.Logger
	level  kgo.LogLevel
}

// NewKgoLogger creates a new KgoLogger wrapping the given zap.Logger at debug
// level. Use NewKgoLoggerWithLevel to pick another level.
func NewKgoLogger(logger *zap.Logger) *KgoLogger {
	return NewKgoLoggerWithLevel(logger, kgo.LogLevelDebug)
}

// NewKgoLoggerWithLevel creates a new KgoLogger reporting the given level to
// franz-go.
func NewKgoLoggerWithLevel(logger *zap.Logger, level kgo.LogLevel) *KgoLogger {
	return &KgoLogger{logger: logger, level: level}
}

// Level returns the configured log level for franz-go.
func (k *KgoLogger) Level() kgo.LogLevel {
	return k.level
}

// Log logs a message at the specified level, converting the franz-go key/value
// pairs into zap fields.
func (k *KgoLogger) Log(level kgo.LogLevel, msg string, keyvals ...interface{}) {
	fields := make([]zap.Field, 0, len(keyvals)/2)
	for i := 0; i+1 < len(keyvals); i += 2 {
		fields = append(fields, zap.Any(fmt.Sprint(keyvals[i]), keyvals[i+1]))
	}

	switch level {
	case kgo.LogLevelError:
		k.logger.Error(msg, fields...)
	case kgo.LogLevelWarn:
		k.logger.Warn(msg, fields...)
	case kgo.LogLevelInfo:
		k.logger.Info(msg, fields...)
	case kgo.LogLevelDebug:
		k.logger.Debug(msg, fields...)
	default:
		k.logger.Info(msg, fields...)
	}
}
