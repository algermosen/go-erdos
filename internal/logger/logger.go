package logger

type Logger interface {
	Info(v ...interface{})
	Error(v ...interface{})
	Close() error
}
