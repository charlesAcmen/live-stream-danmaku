package logger

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Log is the global logger instance.
var Log *zap.Logger

// InitLogger initializes the global logger.
// env: "dev" for development (colored text), "prod" for production (json).
func InitLogger(env string) {
	var config zap.Config

	if env == "prod" {
		// Production: JSON format, Info level, write to file/stdout
		// {"level":"info","ts":"2026...","msg":"User login"}
		config = zap.NewProductionConfig()
		config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder // 2026-01-19T...
	} else {
		// Development: Console format, Debug level, colored
		config = zap.NewDevelopmentConfig()
		config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	}

	var err error
	Log, err = config.Build()
	if err != nil {
		os.Exit(1)
	}

	// Replace the standard global logger (optional but recommended)
	zap.ReplaceGlobals(Log)
}

// Sync flushes any buffered log entries.
// defer logger.Sync() before program exits
func Sync() {
	_ = Log.Sync()
}
