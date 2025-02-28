package sqlhelper

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/aarioai/airis-driver/driver"
	"github.com/aarioai/airis/core/ae"
	"github.com/aarioai/airis/core/alog"
	"github.com/aarioai/airis/pkg/afmt"
	"log"
)

type txResult uint8
type Tx struct {
	result txResult
	Tx     *sql.Tx
}

const (
	rollback txResult = 1
	commit   txResult = 2
)

func (d *DB) Begin(ctx context.Context, opts *sql.TxOptions) (*Tx, *ae.Error) {
	if d.err != nil {
		return nil, driver.NewSQLError(d.err)
	}
	tx, err := d.DB.BeginTx(ctx, opts)
	if err != nil {
		return nil, driver.NewSQLError(err)
	}
	t := Tx{Tx: tx}
	return &t, nil
}

func (t *Tx) Rollback() *ae.Error {
	t.result = rollback
	return driver.NewSQLError(t.Tx.Rollback())
}

func (t *Tx) Commit() *ae.Error {
	t.result = commit
	return driver.NewSQLError(t.Tx.Commit())
}

// defer tx.Recover
func (t *Tx) Recover() func() {
	return func() {
		if p := recover(); p != nil {
			alog.LogOnError(t.Tx.Rollback())
		}
		if t.result == 0 {
			log.Println("[waring] tx not commit")
			alog.LogOnError(t.Tx.Commit())
		}
	}
}

func (t *Tx) Prepare(ctx context.Context, query string) (*sql.Stmt, *ae.Error) {
	stmt, err := t.Tx.PrepareContext(ctx, query)
	if err != nil {
		if stmt != nil {
			alog.LogOnError(stmt.Close())
		}
		return nil, driver.NewSQLError(err, query)
	}
	return stmt, nil
}

func (t *Tx) Execute(ctx context.Context, query string, args ...any) (sql.Result, *ae.Error) {
	res, err := t.Tx.ExecContext(ctx, query, args...)
	return res, driver.NewSQLError(err, afmt.Sprintf(query, args...))
}

func (t *Tx) Exec(ctx context.Context, query string, args ...any) *ae.Error {
	_, e := t.Execute(ctx, query, args...)
	return e
}

func (t *Tx) Insert(ctx context.Context, query string, args ...any) (uint, *ae.Error) {
	res, e := t.Execute(ctx, query, args...)
	if e != nil {
		return 0, e
	}
	// 由于事务是先执行，后回滚或提交，所以可以先获取插入的ID，后commit()
	id, err := res.LastInsertId()
	return uint(id), driver.NewSQLError(err, afmt.Sprintf(query, args...))
}

func (t *Tx) Update(ctx context.Context, query string, args ...any) (int64, *ae.Error) {
	res, e := t.Execute(ctx, query, args...)
	if e != nil {
		return 0, e
	}
	// 由于事务是先执行，后回滚或提交，所以可以先获取更新结果，后commit()
	id, err := res.RowsAffected()
	return id, driver.NewSQLError(err, afmt.Sprintf(query, args...))
}

// 批量查询
/*
	stmt,_ := db.Prepare("select count(*) from tb where id=?")
	defer stmt.Close()
	for i:=0;i<1000;i++{
		stmt.QueryRowContext(ctx, i).&Scan()
	}
*/
//func (t *Tx) BatchQueryRow(ctx context.Context, query string, margs ...[]any) (*sql.Stmt, []*sql.Row, *ae.Error) {
//	stmt, e := t.Prepare(ctx, query)
//	if e != nil {
//		return stmt, nil, e
//	}
//	rows := make([]*sql.Row, len(margs))
//	for i, args := range margs {
//		rows[i] = stmt.QueryRowContext(ctx, args...)
//	}
//	return stmt, rows, nil
//}

func (t *Tx) QueryRow(ctx context.Context, query string, args ...any) (*sql.Row, *ae.Error) {
	row := t.Tx.QueryRowContext(ctx, query, args...)
	return row, driver.NewSQLError(row.Err(), afmt.Sprintf(query, args...))
}

func (t *Tx) ScanArgs(ctx context.Context, query string, args []any, dest ...any) *ae.Error {
	row, e := t.QueryRow(ctx, query, args...)
	if e != nil {
		return e
	}
	return driver.NewSQLError(row.Scan(dest...), afmt.Sprintf(query, args...))
}

func (t *Tx) ScanRow(ctx context.Context, query string, dest ...any) *ae.Error {
	row, e := t.QueryRow(ctx, query)
	if e != nil {
		return e
	}
	return driver.NewSQLError(row.Scan(dest...), query)
}

func (t *Tx) Scan(ctx context.Context, query string, id uint64, dest ...any) *ae.Error {
	row, e := t.QueryRow(ctx, query, id)
	if e != nil {
		return e
	}
	return driver.NewSQLError(row.Scan(dest...), fmt.Sprintf(query, id))
}

func (t *Tx) ScanX(ctx context.Context, query string, id string, dest ...any) *ae.Error {
	row, e := t.QueryRow(ctx, query, id)
	if e != nil {
		return e
	}
	return driver.NewSQLError(row.Scan(dest...), fmt.Sprintf(query, id))
}

// do not forget to close *sql.Rows
// 不要忘了关闭 rows
func (t *Tx) Query(ctx context.Context, query string, args ...any) (*sql.Rows, *ae.Error) {
	rows, err := t.Tx.QueryContext(ctx, query, args...)
	if err != nil {
		if rows != nil {
			alog.LogOnError(rows.Close())
		}
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ae.ErrorNoRows
		}
		return nil, driver.NewSQLError(err, afmt.Sprintf(query, args...))
	}
	return rows, nil
}
