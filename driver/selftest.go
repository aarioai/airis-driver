package driver

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/aarioai/airis/aa"
	"github.com/aarioai/airis/aa/alog"
	"github.com/rabbitmq/amqp091-go"
	"github.com/wagslane/go-rabbitmq"
)

func CheckMongodbHealth(section string) func(app *aa.App, errChan chan<- error) {
	return func(app *aa.App, errChan chan<- error) {
		alog.Console("check health -> mongodb [" + section + "]")
		ctx := context.Background()
		client, _, e := NewMongodb(app, section)
		if e != nil {
			errChan <- fmt.Errorf("mongodb (%s) connect error: %s", section, e.Msg)
			return
		}
		defer client.Disconnect(ctx)

		if err := client.Ping(ctx, nil); err != nil {
			errChan <- fmt.Errorf("mongodb (%s) ping error: %s", section, err.Error())
			return
		}
		alog.Console("mongodb [" + section + "] is healthy")
		errChan <- nil
	}
}

func CheckMySQLHealth(section string) func(app *aa.App, errChan chan<- error) {
	return func(app *aa.App, errChan chan<- error) {
		alog.Console("check health -> mysql [" + section + "]")
		_, db, e := NewMysql(app, section)
		if e != nil {
			errChan <- fmt.Errorf("mysql (%s) connect error: %s", section, e.Msg)
			return
		}
		defer db.Close()

		if err := db.Ping(); err != nil {
			errChan <- fmt.Errorf("mysql (%s) ping error: %s", section, err.Error())
			return
		}
		alog.Console("mysql [" + section + "] is healthy")
		errChan <- nil
	}
}

func CheckRabbitmqHealth(section string, tlsConfig *tls.Config, sasl []amqp091.Authentication, opts []func(*rabbitmq.ConnectionOptions)) func(app *aa.App, errChan chan<- error) {
	return func(app *aa.App, errChan chan<- error) {
		alog.Console("check health -> rabbitmq [" + section + "]")
		conn, e := NewRabbitmqPool(app, section, tlsConfig, sasl, opts)
		if e != nil {
			errChan <- fmt.Errorf("rabbitmq (%s) connect error: %s", section, e.Msg)
			return
		}
		consumer, err := rabbitmq.NewConsumer(
			conn,
			"temp_health_check_queue",
			rabbitmq.WithConsumerOptionsQueueAutoDelete, // delete after all
			rabbitmq.WithConsumerOptionsLogging,
		)
		if err != nil {
			errChan <- fmt.Errorf("rabbitmq (%s) ping error: %s", section, err.Error())
			return
		}
		defer consumer.Close()
		alog.Console("rabbitmq [" + section + "] is healthy")
		errChan <- nil
	}
}

func CheckRedisHealth(section string) func(app *aa.App, errChan chan<- error) {
	return func(app *aa.App, errChan chan<- error) {
		alog.Console("check health -> redis [" + section + "]")
		client, e := NewRedis(app, section)
		if e != nil {
			errChan <- fmt.Errorf("redis (%s) connect error: %s", section, e.Msg)
			return
		}
		defer client.Close()
		_, err := client.Ping(context.Background()).Result()
		if err != nil {
			errChan <- fmt.Errorf("redis (%s) ping error: %s", section, err.Error())
			return
		}
		alog.Console("redis [" + section + "] is healthy")
		errChan <- nil
	}
}
