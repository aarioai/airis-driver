package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/aarioai/airis/core"
	"github.com/aarioai/airis/core/ae"
	"github.com/aarioai/airis/core/airis"
	"github.com/aarioai/airis/core/atype"
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

/*
一个sql.DB是包含许多’open’和’idle’连接的数据库连接池的对象。当你使用sql.DB执行数据库任务时，它将首先检查池中是否有空闲连接。 如果有一个可用，那么Go将重新使用现有连接并在任务持续期间将其标记为打开。 如果游泳池中没有空闲的连接，那么Go会创建一个新的连接并“打开”它。

保持空闲连接是有代价的，它需要占用的内存的，这点需要注意。设置多大的Idle应该根据自身应用程序来定。如果连接闲置时间过长，则可能无法使用。例如，MySQL的wait_timeout设置会自动关闭任何8小时未使用的连接（默认情况下），所以我们看到上面的代码我们设置了自己的超时时间。发生这种情况时，sql.DB会优雅地的关掉它。在关闭之前，连接会自动重试两次，此时Go将从池中移除连接并创建一个新连接。因此，如果将MaxIdleConns设置得太高，实际上可能会导致连接变得无法使用，并且比使用更少空闲连接池（使用更频繁的连接数更少）时使用的资源更多。

1、对于大多数使用SetMaxOpenConns（）来限制打开连接的最大数量的程序，都会对性能产生负面影响，但如果数据库资源比较紧张的情况下，这么做还是有好处的。db.SetMaxOpenConns(n int) 设置打开数据库的最大连接数。包含正在使用的连接和连接池的连接。如果你的函数调用需要申请一个连接，并且连接池已经没有了连接或者连接数达到了最大连接数。此时的函数调用将会被block，直到有可用的连接才会返回。设置这个值可以避免并发太高导致连接mysql出现too many connections的错误。该函数的默认设置是0，表示无限制。
2、如果程序突发或定期同时执行两个以上的数据库任务，那么通过SetMaxIdleConns（）增加空闲连接池的大小可能会产生积极的性能影响。 但是需要注意的是设置过大可能会适得其反。上线之前最好做个压测已到达最佳性能。
设置连接池中的保持连接的最大连接数。默认也是0，表示连接池不会保持释放会连接池中的连接的连接状态：即当连接释放回到连接池的时候，连接将会被关闭。这会导致连接再连接池中频繁的关闭和创建。
3、对于大多数通过SetConnMaxLifetime（）设置连接超时的应用程序，都会对性能产生负面影响。 但是，如果你的数据库本身强制实现一个短的连接生命周期，那么在sql.DB对它进行设置是有价值的，以避免尝试和重试错误连接的开销。
4、如果希望程序在数据库达到硬连接限制时等待连接释放（而不是返回错误），则应该明确设置SetMaxOpenConns（）和SetMaxIdleConns（）。
*/
var (
	mysqlPools sync.Map
)

func NewMysql(app *core.App, cfgSection string) (schema string, db *sql.DB, close bool, err error) {
	if v, ok := mysqlPools.Load(cfgSection); ok {
		if db, ok = v.(*sql.DB); ok {
			return
		}
	}
	schema, db, err = connectMysql(app, cfgSection)
	if err != nil {
		return
	}
	v, _ := mysqlPools.LoadOrStore(cfgSection, db)
	var ok bool
	if db, ok = v.(*sql.DB); ok {
		return
	}
	err = errors.New("bad mysql connection pool " + cfgSection)
	return
}

func connectMysql(app *core.App, cfgSection string) (string, *sql.DB, error) {
	f, err := ParseMysqlConfig(app, cfgSection)

	if err != nil {
		return "", nil, err
	}
	ct := f.ConnectTimeout.Seconds()
	rt := f.ReadTimeout.Seconds()
	wt := f.WriteTimeout.Seconds()
	src := fmt.Sprintf("%s:%s@tcp(%s)/%s?timeout=%.1fs&readTimeout=%.1fs&writeTimeout=%.1fs", f.User, f.Password, f.Host, f.Schema, ct, rt, wt)
	ctx := airis.JobContext(context.Background())
	app.Log.Debug(ctx, "MySQL schema:%s, %s@%s", f.Schema, f.User, f.Host)
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
	//defer conn.Close() // 这是长连接
	conn.SetMaxIdleConns(f.Pool.MaxIdleConns) // 设置闲置的连接数
	conn.SetMaxOpenConns(f.Pool.MaxOpenConns) // 设置最大打开的连接数，默认值为0表示不限制
	conn.SetConnMaxLifetime(f.Pool.ConnMaxLifetime)
	conn.SetConnMaxIdleTime(f.Pool.ConnMaxIdleTime)
	return f.Schema, conn, err
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

func ParseMysqlConfig(app *core.App, section string) (MysqlOptions, error) {
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
