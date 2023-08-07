package helpers

import (
	"log"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// InitLogger initialialize zap logging component.
func InitLogger(production bool) zap.Config {
	var zapConfig zap.Config
	if production {
		zapConfig = zap.NewProductionConfig()
	} else {
		zapConfig = zap.NewDevelopmentConfig()
	}

	zapConfig.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	zapConfig.Level.SetLevel(zap.InfoLevel)

	logger, err := zapConfig.Build(zap.AddStacktrace(zap.ErrorLevel))
	if err != nil {
		log.Fatalf("can't initialize zap logger: %v", err)
	}

	defer func() {
		if err := logger.Sync(); err != nil {
			log.Fatalf("Logger Sync() failed: %v", err)
		}
	}()

	zap.ReplaceGlobals(logger)

	return zapConfig
}
