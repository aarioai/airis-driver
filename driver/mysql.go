package driver

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/aarioai/airis/aa"
	"github.com/aarioai/airis/aa/ae"
	"github.com/aarioai/airis/aa/alog"
	"github.com/aarioai/airis/aa/atype"
	"github.com/aarioai/airis/pkg/types"
	"github.com/aarioai/airis/pkg/utils"
	_ "github.com/go-sql-driver/mysql" // 需要引入
	"regexp"
	"sync"
	"time"
)

const (
	sqlBadConnMsg   = "sql bad conn: "
	sqlSkipMsg      = "sql skip: "
	sqlRemoveArgMsg = "sql remove argument: "
	sqlConnDoneMsg  = "sql conn done: "
	sqlTxDoneMsg    = "sql tx done: "
	sqlErrorMsg     = "sql error: "
)

var (
	duplicateKeyPattern = regexp.MustCompile(`Duplicate\s+entry\s+'([^']*)'\s+for\s+key\s+'([^']*)'`)
)

type MysqlPoolOptions struct {
	MaxIdleConns    int
	MaxOpenConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
}

// https://github.com/go-sql-driver/mysql/
type MysqlOptions struct {
	Schema   string // dbname
	User     string
	Password string
	// Scheme   string // tcp|unix，只支持tcp，unix仅本地可用
	TLS  string // 默认 false，Valid Values:   true, false, skip-verify, preferred, <name>
	Host string
	// Charset  string  不建议用，应该服务器默认设置

	// mysql客户端在尝试与mysql服务器建立连接时，mysql服务器返回错误握手协议前等待客户端数据包的最大时限。默认10秒。
	ConnectTimeout time.Duration // 使用时，需要设置单位，s, ms等。Timeout for establishing connections, aka dial timeout
	ReadTimeout    time.Duration // 使用时，需要设置单位，s, ms等。I/O read timeout.
	WriteTimeout   time.Duration // 使用时，需要设置单位，s, ms等。I/O write timeout.

	Pool MysqlPoolOptions
}

type MysqlClientData struct {
	Client *sql.DB
	Schema string
}

var (
	mysqlClients sync.Map
)

// NewMysql
// Note: better use NewMysqlPool instead
func NewMysql(app *aa.App, cfgSection string) (string, *sql.DB, error) {
	f, err := ParseMysqlConfig(app, cfgSection)

	if err != nil {
		return "", nil, err
	}
	ct := f.ConnectTimeout.Seconds()
	rt := f.ReadTimeout.Seconds()
	wt := f.WriteTimeout.Seconds()
	src := fmt.Sprintf("%s:%s@tcp(%s)/%s?timeout=%.1fs&readTimeout=%.1fs&writeTimeout=%.1fs", f.User, f.Password, f.Host, f.Schema, ct, rt, wt)
	alog.Console("connect mysql: %s@%s %s", f.User, f.Host, f.Schema)
	// sql.Open并不会立即建立一个数据库的网络连接, 也不会对数据库链接参数的合法性做检验, 它仅仅是初始化一个sql.DB对象. 当真正进行第一次数据库查询操作时, 此时才会真正建立网络连接;
	// sql.Open返回的sql.DB对象是协程并发安全的.
	// sql.DB表示操作数据库的抽象接口的对象，但不是所谓的数据库连接对象，sql.DB对象只有当需要使用时才会创建连接，如果想立即验证连接，需要用Ping()方法;
	// 每次db.Query操作后, 都建议调用rows.Close(). 因为 db.Query() 会从数据库连接池中获取一个连接, 这个底层连接在结果集(rows)未关闭前会被标记为处于繁忙状态。当遍历读到最后一条记录时，会发生一个内部EOF错误，自动调用rows.Close(),但如果提前退出循环，rows不会关闭，连接不会回到连接池中，连接也不会关闭, 则此连接会一直被占用. 因此通常我们使用 defer rows.Close() 来确保数据库连接可以正确放回到连接池中; 不过阅读源码发现rows.Close()操作是幂等操作，即一个幂等操作的特点是其任意多次执行所产生的影响均与一次执行的影响相同, 所以即便对已关闭的rows再执行close()也没关系.
	// 需要import 	_ "github.com/go-sql-driver/mysql"
	conn, err := sql.Open("mysql", src)

	if err != nil {
		return "", conn, fmt.Errorf("mysql connection(%s) open error: %s", src, err)
	}

	// It is rare to Close a db, as the db handle is meant to be long-lived and shared between many goroutines.
	conn.SetMaxIdleConns(f.Pool.MaxIdleConns) // 设置闲置的连接数
	conn.SetMaxOpenConns(f.Pool.MaxOpenConns) // 设置最大打开的连接数，默认值为0表示不限制
	conn.SetConnMaxLifetime(f.Pool.ConnMaxLifetime)
	conn.SetConnMaxIdleTime(f.Pool.ConnMaxIdleTime)

	return f.Schema, conn, err
}

// NewMysqlPool
// Warning: Do not unset the returned client as it is managed by the pool
// Warning: 使用完不要unset client，释放是错误人为操作，可能会导致其他正在使用该client的线程panic，这里不做过度处理。
func NewMysqlPool(app *aa.App, cfgSection string) (string, *sql.DB, error) {
	d, ok := mysqlClients.Load(cfgSection)
	if ok {
		clientData := d.(MysqlClientData)
		if clientData.Client != nil {
			return clientData.Schema, clientData.Client, nil
		}
		mysqlClients.Delete(cfgSection)
	}
	schema, db, err := NewMysql(app, cfgSection)
	if err != nil {
		return "", nil, err
	}
	mysqlClients.LoadOrStore(cfgSection, MysqlClientData{
		Schema: schema,
		Client: db,
	})
	return schema, db, nil
}

// CloseMysqlPool
// Each process should utilize a single connection, which is managed by the main function.
// This connection should be closed when the main function terminates.
func CloseMysqlPool() {
	mysqlClients.Range(func(k, v interface{}) bool {
		clientData := v.(MysqlClientData)
		client := clientData.Client
		if client != nil {
			alog.Stop("mysql client: %s", k)
			return client.Close() == nil
		}
		return true
	})
}

// func keepalive(app *aa.App) {
// 	tick := time.NewTicker(60 * time.Second)
// 	var err error
// 	for {
// 		select {
// 		case <-tick.C:
// 			if err = mysqlPool1.Ping(); err != nil {
// 				mysqlPool1.Close()
// 				mysqlPool1, err = connectMysql1(app)
// 			}
// 		}
// 		runtime.Gosched()
// 	}
// }

func ParseMysqlConfig(app *aa.App, section string) (MysqlOptions, error) {
	if section == "" {
		section = "mysql"
	}
	host, err := tryGetSectionCfg(app, "mysql", section, "host")
	if err != nil {
		return MysqlOptions{}, err
	}
	schema, err := tryGetSectionCfg(app, "mysql", section, "schema")
	if err != nil {
		// schema 如果不存在，那么就跟section保持一致
		schema = section
	}
	user, err := tryGetSectionCfg(app, "mysql", section, "user")
	if err != nil {
		return MysqlOptions{}, err
	}
	password, err := tryGetSectionCfg(app, "mysql", section, "password")
	if err != nil {
		return MysqlOptions{}, err
	}

	tls, _ := tryGetSectionCfg(app, "mysql", section, "tls")
	timeout, _ := tryGetSectionCfg(app, "mysql", section, "timeout")
	ct, rt, wt := ParseTimeouts(timeout)
	poolMaxIdleConns, _ := tryGetSectionCfg(app, "mysql", section, "pool_max_idle_conns")
	poolMaxOpenConns, _ := tryGetSectionCfg(app, "mysql", section, "pool_max_open_conns")
	poolConnMaxLifetime, _ := tryGetSectionCfg(app, "mysql", section, "pool_conn_max_life_time")
	poolConnMaxIdleTime, _ := tryGetSectionCfg(app, "mysql", section, "pool_conn_max_idle_time")

	newV := atype.New()
	defer newV.Close()

	cf := MysqlOptions{
		Schema:         schema,
		User:           user,
		Password:       password,
		TLS:            tls,
		Host:           host,
		ConnectTimeout: ct,
		ReadTimeout:    rt,
		WriteTimeout:   wt,
		Pool: MysqlPoolOptions{
			MaxIdleConns:    newV.Reload(poolMaxIdleConns).DefaultInt(0),
			MaxOpenConns:    newV.Reload(poolMaxOpenConns).DefaultInt(0),
			ConnMaxLifetime: types.ParseDuration(poolConnMaxLifetime),
			ConnMaxIdleTime: types.ParseDuration(poolConnMaxIdleTime),
		},
	}
	return cf, nil
}

// NewSQLError 处理 SQL 错误
func NewSQLError(err error, details ...any) *ae.Error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	caller := utils.Caller(1)

	errorMapping := map[error]func() *ae.Error{
		driver.ErrBadConn:        func() *ae.Error { return ae.NewE(caller + sqlBadConnMsg + msg).WithDetail(details...) },
		driver.ErrSkip:           func() *ae.Error { return ae.NewE(caller + sqlSkipMsg + msg).WithDetail(details...) },
		driver.ErrRemoveArgument: func() *ae.Error { return ae.NewE(caller + sqlRemoveArgMsg + msg).WithDetail(details...) },
		sql.ErrNoRows:            func() *ae.Error { return ae.ErrorNotFound }, // can't WithDetail, locked
		sql.ErrConnDone:          func() *ae.Error { return ae.NewE(caller + sqlConnDoneMsg + msg).WithDetail(details...) },
		sql.ErrTxDone:            func() *ae.Error { return ae.NewE(caller + sqlTxDoneMsg + msg).WithDetail(details...) },
	}

	for errType, handler := range errorMapping {
		if errors.Is(err, errType) {
			return handler()
		}
	}

	// 处理重复键错误
	if matches := duplicateKeyPattern.FindStringSubmatch(msg); len(matches) == 3 {
		return ae.NewConflict("sql key").WithDetail(details...)
	}

	return ae.NewE(caller + sqlErrorMsg + msg).WithDetail(details...)
}
