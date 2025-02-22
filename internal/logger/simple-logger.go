package logger

import (
	"io"
	"log"
	"os"
)

type SimpleLogger struct {
	logger *log.Logger
	file   *os.File
}

func NewSimpleLogger(logFile string) (*SimpleLogger, error) {
	var output io.Writer = os.Stdout
	var file *os.File

	if logFile != "" {
		f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			return nil, err
		}

		output = io.MultiWriter(os.Stdout, f)
		file = f
	}

	l := log.New(output, "", log.LstdFlags)
	return &SimpleLogger{
		logger: l,
		file:   file,
	}, nil
}

func (l *SimpleLogger) Info(v ...interface{}) {
	l.logger.SetPrefix("[INFO] ")
	l.logger.Println(v...)
}

func (l *SimpleLogger) Error(v ...interface{}) {
	l.logger.SetPrefix("[ERR] ")
	l.logger.Println(v...)
}

func (l *SimpleLogger) Close() error {
	if l.file != nil {
		return l.file.Close()
	}

	return nil
}
