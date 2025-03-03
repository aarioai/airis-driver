package driver

import (
	"errors"
	"github.com/aarioai/airis/core"
	"github.com/aarioai/airis/core/ae"
	"github.com/aarioai/airis/pkg/types"
	"github.com/aarioai/airis/pkg/utils"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
	"strings"
	"sync"
	"time"
)

type MongodbConnectionOptions struct {
	// https://www.mongodb.com/docs/drivers/go/current/fundamentals/connections/connection-options/
	ConnectTimeout         time.Duration `json:"connect_timeout" query:"connectTimeoutMS"`
	Direct                 bool          `json:"direct" query:"directConnection"`
	HeartbeatFrequency     time.Duration `json:"heartbeat_frequency" query:"heartbeatFrequencyMS"`
	MaxIdleTime            time.Duration `json:"max_idle_time" query:"maxIdleTimeMS"`
	MaxPoolSize            uint64        `json:"max_pool_size" query:"maxPoolSize"` // default 100
	MinPoolSize            uint64        `json:"min_pool_size" query:"minPoolSize"`
	ReplicaSet             string        `json:"replica_set" query:"replicaSet"`
	ServerSelectionTimeout time.Duration `json:"server_selection_timeout" query:"serverSelectionTimeoutMS"`
	Timeout                time.Duration `json:"timeout" query:"timeoutMS"`
	//Tls                    bool          `json:"tls" query:"tls"`
	WriteConcern        string `json:"write_concern" query:"w"`
	WriteConcernJournal bool   `json:"write_concern_journal" query:"j"`
}

func (c *MongodbConnectionOptions) LoadTo(opts *options.ClientOptions) {
	if c.ConnectTimeout > 0 {
		opts.SetConnectTimeout(c.ConnectTimeout)
	}
	if c.Direct {
		opts.SetDirect(true)
	}
	if c.HeartbeatFrequency > 0 {
		opts.SetHeartbeatInterval(c.HeartbeatFrequency)
	}

	if c.MaxIdleTime > 0 {
		opts.SetMaxConnIdleTime(c.MaxIdleTime)
	}

	if c.MaxPoolSize > 0 {
		opts.SetMaxPoolSize(c.MaxPoolSize)
	}
	if c.MinPoolSize > 0 {
		opts.SetMinPoolSize(c.MinPoolSize)
	}
	if c.ReplicaSet != "" {
		opts.SetReplicaSet(c.ReplicaSet)
	}
	if c.ServerSelectionTimeout > 0 {
		opts.SetServerSelectionTimeout(c.ServerSelectionTimeout)
	}
	if c.Timeout > 0 {
		opts.SetConnectTimeout(c.Timeout)
	}
	if c.WriteConcern != "" {
		opts.SetWriteConcern(&writeconcern.WriteConcern{
			W:       c.WriteConcern,
			Journal: &c.WriteConcernJournal,
		})
	}
}

type MongodbCredential struct {
	AuthMechanism           string            `json:"auth_mechanism"`            // 可以为空
	AuthMechanismProperties map[string]string `json:"auth_mechanism_properties"` // 可以为空
	AuthSource              string            `json:"auth_source"`
	Username                string            `json:"username"`
	Password                string            `json:"password"`
}
type MongodbOptions struct {
	Protocol          string `json:"protocol"`
	Hosts             string `json:"hosts"` // e.g localhost,192.158.1.111:27017
	DB                string `json:"db"`
	Credential        *MongodbCredential
	ConnectionOptions *MongodbConnectionOptions
}
type MongodbClientData struct {
	Client *mongo.Client
	DB     string
}

var (
	mongodbClients sync.Map
)

// NewMongodb
// Note: better use NewMongodbPool instead
func NewMongodb(app *core.App, cfgSection string) (*mongo.Client, string, *ae.Error) {
	o, err := ParseMongodbConfig(app, cfgSection)
	if err != nil {
		return nil, "", NewMongodbError(err, "parse mongodb config section: "+cfgSection)
	}
	opts := o.ClientOptions()
	var client *mongo.Client
	if client, err = mongo.Connect(opts); err != nil {
		return nil, "", NewMongodbError(err, "connect to mongodb instance "+o.Hosts)
	}
	return client, o.DB, nil
}

// NewMongodbPool mongodb 自带连接池
// Warning: Do not unset the returned client as it is managed by the pool
// Warning: 使用完不要unset client，释放是错误人为操作，可能会导致其他正在使用该client的线程panic，这里不做过度处理。
func NewMongodbPool(app *core.App, cfgSection string) (*mongo.Client, string, *ae.Error) {
	d, ok := mongodbClients.Load(cfgSection)
	if ok {
		clientData := d.(MongodbClientData)
		if clientData.Client != nil {
			return clientData.Client, clientData.DB, nil
		}
		mongodbClients.Delete(cfgSection)
	}
	client, db, e := NewMongodb(app, cfgSection)
	if e != nil {
		return nil, "", e
	}
	mongodbClients.LoadOrStore(cfgSection, MongodbClientData{
		Client: client,
		DB:     db,
	})
	return client, db, nil
}

func (c *MongodbCredential) ToCredential() options.Credential {
	// PasswordSet: For GSSAPI, this must be true if a password is specified, even if the password is the empty string, and
	// false if no password is specified, indicating that the password should be taken from the context of the running
	// process. For other mechanisms, this field is ignored.
	passwordSet := c.AuthMechanism == "GSSAPI"
	credential := options.Credential{
		AuthMechanism:           c.AuthMechanism,
		AuthMechanismProperties: c.AuthMechanismProperties,
		AuthSource:              c.AuthSource,
		Username:                c.Username,
		Password:                c.Password,
		PasswordSet:             passwordSet,
	}
	return credential
}

func (o *MongodbOptions) ClientOptions() *options.ClientOptions {
	uri := o.Protocol + "://root:Mg1x0_NBdo0_x3Ms@" + o.Hosts
	opts := options.Client().ApplyURI(uri)
	if o.Credential != nil {
		opts.SetAuth(o.Credential.ToCredential())
	}
	if o.ConnectionOptions != nil {
		o.ConnectionOptions.LoadTo(opts)
	}
	return opts
}
func ParseMongodbConfig(app *core.App, section string) (*MongodbOptions, error) {
	hosts, err := tryGetSectionCfg(app, "mongodb", section, "hosts")
	if err != nil {
		return nil, err
	}
	protocol, _ := tryGetSectionCfg(app, "mongodb", section, "protocol")
	if protocol == "" {
		protocol = "mongodb"
	}
	db, _ := tryGetSectionCfg(app, "mongodb", section, "db")
	// credential
	var credential *MongodbCredential
	username, _ := tryGetSectionCfg(app, "mongodb", section, "username")
	if username != "" {
		authMechanism, _ := tryGetSectionCfg(app, "mongodb", section, "auth_mechanism")
		authSource, _ := tryGetSectionCfg(app, "mongodb", section, "auth_source")
		password, _ := tryGetSectionCfg(app, "mongodb", section, "password")

		serviceName, _ := tryGetSectionCfg(app, "mongodb", section, "SERVICE_NAME")
		canonHostName, _ := tryGetSectionCfg(app, "mongodb", section, "CANONICALIZE_HOST_NAME")
		serviceRealm, _ := tryGetSectionCfg(app, "mongodb", section, "SERVICE_REALM")
		serviceHost, _ := tryGetSectionCfg(app, "mongodb", section, "SERVICE_HOST")
		awsSessionToken, _ := tryGetSectionCfg(app, "mongodb", section, "AWS_SESSION_TOKEN")
		var properties map[string]string
		if !allEmpty(serviceName, canonHostName, serviceRealm, serviceHost, awsSessionToken) {
			properties = make(map[string]string)
			if serviceName != "" {
				properties["SERVICE_NAME"] = serviceName
			}
			if canonHostName != "" {
				properties["CANONICALIZE_HOST_NAME"] = canonHostName
			}
			if serviceRealm != "" {
				properties["SERVICE_REALM"] = serviceRealm
			}
			if serviceHost != "" {
				properties["SERVICE_HOST"] = serviceHost
			}
			if awsSessionToken != "" {
				properties["AWS_SESSION_TOKEN"] = awsSessionToken
			}
		}

		credential = &MongodbCredential{
			AuthMechanism:           authMechanism,
			AuthMechanismProperties: properties,
			AuthSource:              authSource,
			Username:                username,
			Password:                password,
		}
	}

	// connection options
	var opts *MongodbConnectionOptions
	connectTimeout, _ := tryGetSectionCfg(app, "mongodb", section, "connect_timeout")
	direct, _ := tryGetSectionCfg(app, "mongodb", section, "direct")
	heartbeatFrequency, _ := tryGetSectionCfg(app, "mongodb", section, "heartbeat_frequency")
	maxIdleTime, _ := tryGetSectionCfg(app, "mongodb", section, "max_idle_time")
	maxPoolSize, _ := tryGetSectionCfg(app, "mongodb", section, "max_pool_size")
	minPoolSize, _ := tryGetSectionCfg(app, "mongodb", section, "min_pool_size")
	replicaSet, _ := tryGetSectionCfg(app, "mongodb", section, "replica_set")
	serverSelectionTimeout, _ := tryGetSectionCfg(app, "mongodb", section, "server_selection_timeout")
	timeout, _ := tryGetSectionCfg(app, "mongodb", section, "timeout")
	tls, _ := tryGetSectionCfg(app, "mongodb", section, "tls")
	writerConcern, _ := tryGetSectionCfg(app, "mongodb", section, "writer_concern")
	writeConcernJournal, _ := tryGetSectionCfg(app, "mongodb", section, "writer_concern_journal")
	noOpts := allEmpty(connectTimeout, direct, heartbeatFrequency, maxIdleTime, maxPoolSize, minPoolSize)
	noOpts = noOpts && allEmpty(replicaSet, serverSelectionTimeout, timeout, tls, writerConcern, writeConcernJournal)
	if !noOpts {
		opts = &MongodbConnectionOptions{
			ConnectTimeout:         types.ParseDuration(connectTimeout),
			Direct:                 types.ToBool(direct),
			HeartbeatFrequency:     types.ParseDuration(heartbeatFrequency),
			MaxIdleTime:            types.ParseDuration(maxIdleTime),
			MaxPoolSize:            types.ToUint64(maxPoolSize),
			MinPoolSize:            types.ToUint64(minPoolSize),
			ReplicaSet:             replicaSet,
			ServerSelectionTimeout: types.ParseDuration(serverSelectionTimeout),
			Timeout:                types.ParseDuration(timeout),
			WriteConcern:           writerConcern,
			WriteConcernJournal:    types.ToBool(writeConcernJournal),
		}

	}

	return &MongodbOptions{
		Protocol:          protocol,
		Hosts:             strings.ReplaceAll(hosts, " ", ""),
		DB:                db,
		Credential:        credential,
		ConnectionOptions: opts,
	}, nil
}
func NewMongodbError(err error, details ...any) *ae.Error {
	if err == nil {
		return nil
	}

	msg := err.Error()
	caller := utils.Caller(1)

	errorMapping := map[error]func() *ae.Error{
		mongo.ErrClientDisconnected: func() *ae.Error { return ae.NewE(caller + sqlBadConnMsg + msg).WithDetail(details...) },
		mongo.ErrNoDocuments:        func() *ae.Error { return ae.ErrorNotFound }, // can't WithDetail, locked
		mongo.ErrFileNotFound:       func() *ae.Error { return ae.ErrorNotFound },
	}

	for errType, handler := range errorMapping {
		if errors.Is(err, errType) {
			return handler()
		}
	}

	return ae.NewError(err).WithDetail(details...)
}
