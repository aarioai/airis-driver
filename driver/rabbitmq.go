package driver

import (
	"crypto/tls"
	"fmt"
	"github.com/aarioai/airis/aa"
	"github.com/aarioai/airis/aa/ae"
	"github.com/aarioai/airis/aa/alog"
	"github.com/aarioai/airis/pkg/types"
	"github.com/rabbitmq/amqp091-go"
	"github.com/wagslane/go-rabbitmq"
	"sync"
	"time"
)

type RabbitmqConfig struct {
	ConnectionOptions rabbitmq.Config
	ReconnectInterval time.Duration

	Host     string
	User     string
	Password string
}

var (
	rabbitmqClients sync.Map
)

// NewRabbitmq
// Note: better use NewRabbitmqPool instead
func NewRabbitmq(app *aa.App, section string, tlsConfig *tls.Config, sasl []amqp091.Authentication, opts []func(*rabbitmq.ConnectionOptions)) (*rabbitmq.Conn, *ae.Error) {
	c, err := ParseRabbitmqConfig(app, section, tlsConfig, sasl)
	if err != nil {
		return nil, newConfigError(section, err)
	}

	defaultOpts := []func(*rabbitmq.ConnectionOptions){
		rabbitmq.WithConnectionOptionsLogger(NewRabbitMQLogger(app.Log)),
		rabbitmq.WithConnectionOptionsConfig(c.ConnectionOptions),
	}
	if c.ReconnectInterval > 0 {
		defaultOpts = append(defaultOpts, rabbitmq.WithConnectionOptionsReconnectInterval(c.ReconnectInterval))
	}

	if len(opts) > 0 {
		defaultOpts = append(defaultOpts, opts...)
	}
	conn, err := rabbitmq.NewConn(c.Url(), defaultOpts...)
	if err != nil {
		return nil, NewRabbitmqError(err)
	}
	return conn, nil
}

// NewRabbitmqPool
// Warning: Do not unset the returned client as it is managed by the pool
// Warning: 使用完不要unset client，释放是错误人为操作，可能会导致其他正在使用该client的线程panic，这里不做过度处理。
func NewRabbitmqPool(app *aa.App, section string, tlsConfig *tls.Config, sasl []amqp091.Authentication, opts []func(*rabbitmq.ConnectionOptions)) (*rabbitmq.Conn, *ae.Error) {
	client, ok := rabbitmqClients.Load(section)
	if ok {
		if client != nil {
			return client.(*rabbitmq.Conn), nil
		}
		rabbitmqClients.Delete(section)
	}
	var e *ae.Error
	client, e = NewRabbitmq(app, section, tlsConfig, sasl, opts)
	if e != nil {
		return nil, e
	}
	client, _ = rabbitmqClients.LoadOrStore(section, client)
	return client.(*rabbitmq.Conn), nil
}

// CloseRabbitmqPool
// Each process should utilize a single connection, which is managed by the main function.
// This connection should be closed when the main function terminates.
func CloseRabbitmqPool() {
	rabbitmqClients.Range(func(k, v interface{}) bool {
		client := v.(*rabbitmq.Conn)
		if client != nil {
			alog.Stopf("rabbitmq client: %s", k)
			return client.Close() == nil
		}
		return true
	})
}

func ParseRabbitmqConfig(app *aa.App, section string, tlsConfig *tls.Config, sasl []amqp091.Authentication) (RabbitmqConfig, error) {
	host, err1 := tryGetSectionCfg(app, "rabbitmq", section, "host")
	user, err2 := tryGetSectionCfg(app, "rabbitmq", section, "user")
	password, err3 := tryGetSectionCfg(app, "rabbitmq", section, "password")
	if err := ae.FirstError(err1, err2, err3); err != nil {
		return RabbitmqConfig{}, err
	}
	vhost, _ := tryGetSectionCfg(app, "rabbitmq", section, "vhost", "/")
	channelMax, _ := tryGetSectionCfg(app, "rabbitmq", section, "channel_max")
	frameSize, _ := tryGetSectionCfg(app, "rabbitmq", section, "frame_size")
	heartbeat, _ := tryGetSectionCfg(app, "rabbitmq", section, "heartbeat")
	reconnectInterval, _ := tryGetSectionCfg(app, "rabbitmq", section, "reconnect_interval")

	if vhost == "" {
		vhost = "/"
	}
	amqpConfig := rabbitmq.Config{
		SASL:            sasl,
		Vhost:           vhost,
		ChannelMax:      types.ToUint16(channelMax),     // 0 max channels means 2^16 - 1
		FrameSize:       types.ToInt(frameSize),         // 0 max bytes means unlimited
		Heartbeat:       types.ParseDuration(heartbeat), // less than 1s uses the server's interval
		TLSClientConfig: tlsConfig,
		Properties:      nil,
		Locale:          app.Config.TimezoneID,
	}
	c := RabbitmqConfig{
		ConnectionOptions: amqpConfig,
		ReconnectInterval: types.ParseDuration(reconnectInterval),

		Host:     host,
		User:     user,
		Password: password,
	}
	return c, nil
}
func (c RabbitmqConfig) Url() string {
	return fmt.Sprintf("amqp://%s:%s@%s%s", c.User, c.Password, c.Host, c.ConnectionOptions.Vhost)
}
func NewRabbitmqError(err error, details ...any) *ae.Error {
	if err == nil {
		return nil
	}
	return ae.NewErr(err, details...)
}
