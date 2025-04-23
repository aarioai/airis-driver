package rabbitmqx

import "github.com/wagslane/go-rabbitmq"

type Declare struct {
	Queue      string
	Exchange   string
	RoutingKey string
}

func NewConsumer(conn *rabbitmq.Conn, declare Declare, options ...func(*rabbitmq.ConsumerOptions)) (*rabbitmq.Consumer, error) {
	if len(options) == 0 {
		options = make([]func(*rabbitmq.ConsumerOptions), 0)
	}
	if declare.Exchange != "" {
		options = append(options, rabbitmq.WithConsumerOptionsExchangeName(declare.Exchange))
	}
	if declare.RoutingKey != "" {
		options = append(options, rabbitmq.WithConsumerOptionsRoutingKey(declare.RoutingKey))
	}
	return rabbitmq.NewConsumer(conn, declare.Queue, options...)
}

func DeclareConsumer(conn *rabbitmq.Conn, declare Declare, options ...func(*rabbitmq.ConsumerOptions)) (*rabbitmq.Consumer, error) {
	if len(options) == 0 {
		options = make([]func(*rabbitmq.ConsumerOptions), 0, 1)
	}
	options = append(options, rabbitmq.WithConsumerOptionsExchangeDeclare)
	return NewConsumer(conn, declare, options...)
}

func NewPublisher(conn *rabbitmq.Conn, exchange string, options ...func(publisherOptions *rabbitmq.PublisherOptions)) (*rabbitmq.Publisher, error) {
	if len(options) == 0 {
		options = make([]func(*rabbitmq.PublisherOptions), 0)
	}
	if exchange != "" {
		options = append(options, rabbitmq.WithPublisherOptionsExchangeName(exchange))
	}
	return rabbitmq.NewPublisher(conn, options...)
}

func DeclarePublisher(conn *rabbitmq.Conn, exchange string, options ...func(publisherOptions *rabbitmq.PublisherOptions)) (*rabbitmq.Publisher, error) {
	if len(options) == 0 {
		options = make([]func(*rabbitmq.PublisherOptions), 0, 1)
	}
	options = append(options, rabbitmq.WithPublisherOptionsExchangeDeclare)
	return NewPublisher(conn, exchange, options...)
}
