package driver

import (
	"errors"
	"github.com/aarioai/airis/aa"
	"github.com/aarioai/airis/aa/ae"
	"github.com/aarioai/airis/aa/alog"
	"github.com/aarioai/airis/aa/atype"
	"github.com/aarioai/airis/pkg/types"
	"github.com/aarioai/airis/pkg/utils"
	"github.com/redis/go-redis/v9"
	"sync"
	"time"
)

var (
	redisClients sync.Map
)

// NewRedis
// Note: better use NewRedisPool instead
func NewRedis(app *aa.App, section string) (*redis.Client, *ae.Error) {
	opts, err := ParseRedisConfig(app, section)
	if err != nil {
		return nil, newConfigError(section, err)
	}
	return redis.NewClient(opts), nil
}

// NewRedisPool go-redis 是redis官方推出的，自带连接池、线程安全，不必手动操作
// https://redis.uptrace.dev/guide/go-redis-debugging.html#connection-pool-size
// Warning: Do not unset the returned client as it is managed by the pool
// Warning: 使用完不要unset client，释放是错误人为操作，可能会导致其他正在使用该client的线程panic，这里不做过度处理。
func NewRedisPool(app *aa.App, section string) (*redis.Client, *ae.Error) {
	cli, ok := redisClients.Load(section)
	if ok {
		if cli != nil {
			return cli.(*redis.Client), nil
		}
		redisClients.Delete(section)
	}

	client, e := NewRedis(app, section)
	if e != nil {
		return nil, e
	}
	redisClients.LoadOrStore(section, client)
	return client, nil
}

// CloseRedisPool
// Each process should utilize a single connection, which is managed by the main function.
// This connection should be closed when the main function terminates.
func CloseRedisPool() {
	redisClients.Range(func(k, v interface{}) bool {
		client := v.(*redis.Client)
		if client != nil {
			alog.Stop("redis client: %s", k)
			return client.Close() == nil
		}
		return true
	})
}

func ParseRedisConfig(app *aa.App, section string) (*redis.Options, error) {
	var connTimeout, readTimeout, writeTimeout time.Duration
	addr, err := tryGetSectionCfg(app, "redis", section, "addr")
	if err != nil {
		return nil, err
	}
	network, _ := tryGetSectionCfg(app, "redis", section, "network")
	clientName, _ := tryGetSectionCfg(app, "redis", section, "client_name")
	protocol, _ := tryGetSectionCfg(app, "redis", section, "protocol")
	username, _ := tryGetSectionCfg(app, "redis", section, "username") // username 可以为空
	password, _ := tryGetSectionCfg(app, "redis", section, "password") // password 可以为空
	db, _ := tryGetSectionCfg(app, "redis", section, "db")
	maxRetries, _ := tryGetSectionCfg(app, "redis", section, "max_retries")
	minRetryBackoff, _ := tryGetSectionCfg(app, "redis", section, "min_retry_backoff")
	maxRetryBackoff, _ := tryGetSectionCfg(app, "redis", section, "max_retry_backoff")
	if timeout, err := tryGetSectionCfg(app, "redis", section, "max_retry_backoff"); err == nil {
		connTimeout, readTimeout, writeTimeout = ParseTimeouts(timeout)
	}

	contextTimeoutEnabled, _ := tryGetSectionCfg(app, "redis", section, "context_timeout_enabled")
	poolFIFO, _ := tryGetSectionCfg(app, "redis", section, "pool_fifo")
	poolSize, _ := tryGetSectionCfg(app, "redis", section, "pool_size")
	poolTimeout, _ := tryGetSectionCfg(app, "redis", section, "pool_timeout")
	minIdleConns, _ := tryGetSectionCfg(app, "redis", section, "min_idle_conns")
	maxIdleConns, _ := tryGetSectionCfg(app, "redis", section, "max_idle_conns")
	maxActiveConns, _ := tryGetSectionCfg(app, "redis", section, "max_active_conns")
	connMaxIdleTime, _ := tryGetSectionCfg(app, "redis", section, "conn_max_idle_time")
	connMaxLifetime, _ := tryGetSectionCfg(app, "redis", section, "conn_max_lifetime")
	disableIdentity, _ := tryGetSectionCfg(app, "redis", section, "disable_identity")
	identitySuffix, _ := tryGetSectionCfg(app, "redis", section, "identity_suffix")
	unstableResp3, _ := tryGetSectionCfg(app, "redis", section, "unstable_resp3")
	newV := atype.New()
	defer newV.Close()

	opt := redis.Options{
		Network: network,
		Addr:    addr, //  127.0.0.1:6379
		// ClientName will execute the `CLIENT SETNAME ClientName` command for each conn.
		ClientName:                 clientName,
		Dialer:                     nil,
		OnConnect:                  nil,
		Protocol:                   types.ToInt(protocol),
		Username:                   username,
		Password:                   password,
		CredentialsProvider:        nil,
		CredentialsProviderContext: nil,
		DB:                         types.ToInt(db),
		MaxRetries:                 types.ToInt(maxRetries),
		MinRetryBackoff:            types.ParseDuration(minRetryBackoff),
		MaxRetryBackoff:            types.ParseDuration(maxRetryBackoff),
		DialTimeout:                connTimeout,
		ReadTimeout:                readTimeout,
		WriteTimeout:               writeTimeout,
		ContextTimeoutEnabled:      types.ToBool(contextTimeoutEnabled),
		PoolFIFO:                   types.ToBool(poolFIFO),
		PoolSize:                   types.ToInt(poolSize),
		PoolTimeout:                types.ParseDuration(poolTimeout),
		MinIdleConns:               types.ToInt(minIdleConns),
		MaxIdleConns:               types.ToInt(maxIdleConns),
		MaxActiveConns:             types.ToInt(maxActiveConns),
		ConnMaxIdleTime:            types.ParseDuration(connMaxIdleTime),
		ConnMaxLifetime:            types.ParseDuration(connMaxLifetime),
		TLSConfig:                  nil,
		Limiter:                    nil,
		// 官方写错，会在 v10 更正过来
		DisableIndentity: types.ToBool(disableIdentity),
		IdentitySuffix:   identitySuffix,
		UnstableResp3:    types.ToBool(unstableResp3),
	}
	return &opt, nil
}

// NewRedisError 处理 Redis 错误
// @TODO
func NewRedisError(err error, details ...any) *ae.Error {
	if err == nil {
		return nil
	}
	if errors.Is(err, redis.Nil) {
		return ae.New(ae.NotFound, "redis key not found").WithDetail(details...)
	}
	msg := err.Error()
	caller := utils.Caller(1)
	return ae.New(ae.InternalServerError, caller+" redis: "+msg).WithDetail(details...)
}
