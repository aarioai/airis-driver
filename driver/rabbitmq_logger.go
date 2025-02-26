package driver

import (
	"context"
	"github.com/aarioai/airis/core/alog"
	"github.com/wagslane/go-rabbitmq"
)

type RabbitMQLogger struct {
	log alog.LogInterface
}

func NewRabbitMQLogger(logInterface alog.LogInterface) rabbitmq.Logger {
	return &RabbitMQLogger{
		log: logInterface,
	}
}

func (l *RabbitMQLogger) Fatalf(msg string, args ...any) {
	l.log.Fatal(context.Background(), msg, args...)
}
func (l *RabbitMQLogger) Errorf(msg string, args ...any) {
	l.log.Error(context.Background(), msg, args...)
}
func (l *RabbitMQLogger) Warnf(msg string, args ...any) {
	l.log.Warn(context.Background(), msg, args...)
}
func (l *RabbitMQLogger) Infof(msg string, args ...any) {
	l.log.Info(context.Background(), msg, args...)
}
func (l *RabbitMQLogger) Debugf(msg string, args ...any) {
	l.log.Debug(context.Background(), msg, args...)
}
