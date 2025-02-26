package driver

import (
	"crypto/tls"
	"fmt"
	"github.com/aarioai/airis/core"
	"github.com/aarioai/airis/core/ae"
	"github.com/aarioai/airis/pkg/types"
	"github.com/rabbitmq/amqp091-go"
	"github.com/wagslane/go-rabbitmq"
	"time"
)

type RabbitmqConfig struct {
	ConnectionOptions rabbitmq.Config
	ReconnectInterval time.Duration

	Host     string
	User     string
	Password string
}

func (c RabbitmqConfig) Url() string {
	return fmt.Sprintf("amqp://%s:%s@%s%s", c.User, c.Password, c.Host, c.ConnectionOptions.Vhost)
}

func NewRabbitmq(app *core.App, cfgSection string, tlsConfig *tls.Config, sasl []amqp091.Authentication, opts []func(*rabbitmq.ConnectionOptions)) (*rabbitmq.Conn, *ae.Error) {
	c, err := ParseRabbitmqConfig(app, cfgSection, tlsConfig, sasl)
	if err != nil {
		return nil, ae.NewE("parse config: %s failed", cfgSection).WithDetail(err.Error())
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

func ParseRabbitmqConfig(app *core.App, section string, tlsConfig *tls.Config, sasl []amqp091.Authentication) (RabbitmqConfig, error) {
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

func NewRabbitmqError(err error, details ...any) *ae.Error {
	if err == nil {
		return nil
	}
	return ae.NewError(err, details...)
}
