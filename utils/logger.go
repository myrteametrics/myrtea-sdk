package utils

import "go.uber.org/zap"

type ZapLeveledLogger struct {
	zapLogger *zap.Logger
}

func NewZapLeveledLogger(logger *zap.Logger) ZapLeveledLogger {
	return ZapLeveledLogger{
		zapLogger: logger,
	}
}

func (logger ZapLeveledLogger) Error(msg string, keysAndValues ...interface{}) {
	logger.zapLogger.Sugar().Error(msg, " ", keysAndValues)
}
func (logger ZapLeveledLogger) Info(msg string, keysAndValues ...interface{}) {
	logger.zapLogger.Sugar().Info(msg, " ", keysAndValues)
}
func (logger ZapLeveledLogger) Debug(msg string, keysAndValues ...interface{}) {
	logger.zapLogger.Sugar().Debug(msg, " ", keysAndValues)
}
func (logger ZapLeveledLogger) Warn(msg string, keysAndValues ...interface{}) {
	logger.zapLogger.Sugar().Warn(msg, " ", keysAndValues)
}
