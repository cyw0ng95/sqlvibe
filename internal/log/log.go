package log

import (
	"fmt"
	"os"
	"sync"
	"time"
)

type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
	LevelFatal
)

var levelNames = []string{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}

type Logger struct {
	mu     sync.Mutex
	level  Level
	output *os.File
}

var defaultLogger *Logger

func init() {
	defaultLogger = &Logger{
		level:  LevelInfo,
		output: os.Stderr,
	}
}

func SetLevel(level Level) {
	defaultLogger.mu.Lock()
	defer defaultLogger.mu.Unlock()
	defaultLogger.level = level
}

func Debug(format string, args ...interface{}) {
	defaultLogger.log(LevelDebug, format, args...)
}

func Info(format string, args ...interface{}) {
	defaultLogger.log(LevelInfo, format, args...)
}

func Warn(format string, args ...interface{}) {
	defaultLogger.log(LevelWarn, format, args...)
}

func Error(format string, args ...interface{}) {
	defaultLogger.log(LevelError, format, args...)
}

func Fatal(format string, args ...interface{}) {
	defaultLogger.log(LevelFatal, format, args...)
	os.Exit(1)
}

func (l *Logger) log(lvl Level, format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if lvl < l.level {
		return
	}

	msg := fmt.Sprintf(format, args...)
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")
	fmt.Fprintf(l.output, "[%s] [%s] %s\n", timestamp, levelNames[lvl], msg)
}
