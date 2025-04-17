package sqlhelper

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/aarioai/airis-driver/driver"
	"github.com/aarioai/airis/aa/ae"
	"github.com/aarioai/airis/aa/alog"
	"github.com/aarioai/airis/pkg/afmt"
)

type DB struct {
	Schema string
	DB     *sql.DB
	error  *ae.Error
}

func NewDriver(schema string, db *sql.DB, e *ae.Error) *DB {
	return &DB{
		Schema: schema,
		DB:     db,
		error:  e,
	}
}

// 批处理 prepare 性能会更好，但需要支持 mysqli；非批处理，不要使用 prepare，会造成多余开销
// 不要忘记 stmt.Close() 释放连接池资源
// Prepared statements take up server resources and should be closed after use.
func (d *DB) Prepare(ctx context.Context, query string) (*sql.Stmt, *ae.Error) {
	if d.error != nil {
		return nil, d.error
	}
	stmt, err := d.DB.PrepareContext(ctx, query)
	if err != nil {
		if stmt != nil {
			alog.LogOnError(stmt.Close())
		}
		return nil, driver.NewMysqlError(err, query)
	}
	return stmt, nil
}

/*
stmt close 必须要等到相关都执行完（包括  res.LastInsertId()  ,  row.Scan()
*/
func (d *DB) Execute(ctx context.Context, query string, args ...any) (sql.Result, *ae.Error) {
	if d.error != nil {
		return nil, d.error
	}
	res, err := d.DB.ExecContext(ctx, query, args...)
	return res, driver.NewMysqlError(err, afmt.Sprintf(query, args...))
}

func (d *DB) Exec(ctx context.Context, query string, args ...any) *ae.Error {
	if d.error != nil {
		return d.error
	}
	_, e := d.Execute(ctx, query, args...)
	return e
}

func (d *DB) Insert(ctx context.Context, query string, args ...any) (uint, *ae.Error) {
	if d.error != nil {
		return 0, d.error
	}
	res, e := d.Execute(ctx, query, args...)
	if e != nil {
		return 0, e
	}
	// 由于事务是先执行，后回滚或提交，所以可以先获取插入的ID，后commit()
	id, err := res.LastInsertId()
	return uint(id), driver.NewMysqlError(err, afmt.Sprintf(query, args...))
}

func (d *DB) Update(ctx context.Context, query string, args ...any) (int64, *ae.Error) {
	if d.error != nil {
		return 0, d.error
	}
	res, e := d.Execute(ctx, query, args...)
	if e != nil {
		return 0, e
	}
	// 由于事务是先执行，后回滚或提交，所以可以先获取更新结果，后commit()
	id, err := res.RowsAffected()
	return id, driver.NewMysqlError(err, afmt.Sprintf(query, args...))
}

// 批量查询
/*
	stmt,_ := db.Prepare("select count(*) from tb where id=?")
	defer stmt.Close()
	for i:=0;i<1000;i++{
		stmt.QueryRowContext(ctx, i).&Scan()
	}
*/
//func (d *DB) BatchQueryRow(ctx context.Context, query string, margs ...[]any) ([]*sql.Row, *ae.Error) {
//	stmt, e := d.Prepare(ctx, query)
//	if e != nil {
//		return nil, e
//	}
//	defer stmt.Close()
//	rows := make([]*sql.Row, len(margs))
//	for i, args := range margs {
//		rows[i] = stmt.QueryRowContext(ctx, args...)
//	}
//	return rows, nil
//}

func (d *DB) QueryRow(ctx context.Context, query string, args ...any) (*sql.Row, *ae.Error) {
	if d.error != nil {
		return nil, d.error
	}
	row := d.DB.QueryRowContext(ctx, query, args...)
	return row, driver.NewMysqlError(row.Err(), afmt.Sprintf(query, args...))
}

func (d *DB) ScanArgs(ctx context.Context, query string, args []any, dest ...any) *ae.Error {
	if d.error != nil {
		return d.error
	}
	row, e := d.QueryRow(ctx, query, args...)
	if e != nil {
		return e
	}
	return driver.NewMysqlError(row.Scan(dest...), afmt.Sprintf(query, args...))
}
func (d *DB) ScanRow(ctx context.Context, query string, dest ...any) *ae.Error {
	if d.error != nil {
		return d.error
	}
	row, e := d.QueryRow(ctx, query)
	if e != nil {
		return e
	}
	return driver.NewMysqlError(row.Scan(dest...), query)
}

func (d *DB) Scan(ctx context.Context, query string, id uint64, dest ...any) *ae.Error {
	if d.error != nil {
		return d.error
	}
	row, e := d.QueryRow(ctx, query, id)
	if e != nil {
		return e
	}
	return driver.NewMysqlError(row.Scan(dest...), fmt.Sprintf(query, id))
}
func (d *DB) ScanX(ctx context.Context, query string, id string, dest ...any) *ae.Error {
	if d.error != nil {
		return d.error
	}
	row, e := d.QueryRow(ctx, query, id)
	if e != nil {
		return e
	}
	return driver.NewMysqlError(row.Scan(dest...), fmt.Sprintf(query, id))
}

// Query
// do not forget to close *sql.Rows
// 只有 QueryRow 找不到才会返回 ae.ErrorNotFound；Query 即使不存在，也是 nil
func (d *DB) Query(ctx context.Context, query string, args ...any) (*sql.Rows, *ae.Error) {
	if d.error != nil {
		return nil, d.error
	}
	rows, err := d.DB.QueryContext(ctx, query, args...)
	if err != nil {
		if rows != nil {
			alog.LogOnError(rows.Close())
		}
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ae.ErrorNoRowsAvailable
		}
		return nil, driver.NewMysqlError(err, afmt.Sprintf(query, args...))
	}
	return rows, nil
}
