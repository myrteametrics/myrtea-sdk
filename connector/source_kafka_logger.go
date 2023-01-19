package connector

import "go.uber.org/zap"

// SaramaLogger wraps zap.Logger with the sarama.StdLogger interface
type SaramaLogger struct {
	logger *zap.SugaredLogger
}

// NewSaramaLogger initializes a new SaramaLogger with a zap.Logger
func NewSaramaLogger(zl *zap.Logger) *SaramaLogger {
	return &SaramaLogger{
		logger: zl.Sugar(),
	}
}

func (s *SaramaLogger) Print(v ...interface{}) {
	s.logger.Info(v...)
}

func (s *SaramaLogger) Printf(format string, v ...interface{}) {
	s.logger.Infof(format, v...)
}

func (s *SaramaLogger) Println(v ...interface{}) {
	s.logger.Info(v...)
}

// SaramaDebugLogger wraps zap.Logger with the sarama.StdLogger interface
type SaramaDebugLogger struct {
	logger *zap.SugaredLogger
}

// NewSaramaLogger initializes a new SaramaDebugLogger with a zap.Logger
func NewSaramaDebugLogger(zl *zap.Logger) *SaramaDebugLogger {
	return &SaramaDebugLogger{
		logger: zl.Sugar(),
	}
}

func (s *SaramaDebugLogger) Print(v ...interface{}) {
	s.logger.Debug(v...)
}

func (s *SaramaDebugLogger) Printf(format string, v ...interface{}) {
	s.logger.Debugf(format, v...)
}

func (s *SaramaDebugLogger) Println(v ...interface{}) {
	s.logger.Debug(v...)
}
