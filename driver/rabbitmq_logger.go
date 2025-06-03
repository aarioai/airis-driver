package driver

import (
	"context"
	"github.com/aarioai/airis/aa/alog"
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
	l.log.Fatalf(context.Background(), msg, args...)
}
func (l *RabbitMQLogger) Errorf(msg string, args ...any) {
	l.log.Errorf(context.Background(), msg, args...)
}
func (l *RabbitMQLogger) Warnf(msg string, args ...any) {
	l.log.Warnf(context.Background(), msg, args...)
}
func (l *RabbitMQLogger) Infof(msg string, args ...any) {
	l.log.Infof(context.Background(), msg, args...)
}
func (l *RabbitMQLogger) Debugf(msg string, args ...any) {
	l.log.Debugf(context.Background(), msg, args...)
}
