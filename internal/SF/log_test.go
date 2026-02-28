package log

import (
	"testing"
)

func TestLog(t *testing.T) {
	Info("test info message")
	Debug("test debug message")
	Warn("test warning message")
	Error("test error message")
}

func TestLogWithValues(t *testing.T) {
	Info("key=%s", "value")
	Debug("msg %d %s", 1, "test")
	Warn("level=%d", 1)
	Error("err=%v", nil)
}

func TestSetLevel(t *testing.T) {
	SetLevel(LevelDebug)
	SetLevel(LevelInfo)
	SetLevel(LevelWarn)
	SetLevel(LevelError)
}

func TestLogger(t *testing.T) {
	logger := &Logger{
		level:  LevelInfo,
		output: nil,
	}
	logger.log(LevelInfo, "test message")
	logger.log(LevelDebug, "should not print")
}
