package mysqli

import (
	"context"
	"fmt"
	"github.com/aarioai/airis-driver/driver/index"
	"github.com/aarioai/airis/aa/ae"
	"strings"
)

type ORMS struct {
	db *DB
	t  index.Entity
}

func ORM(db *DB, t index.Entity) *ORMS {
	return &ORMS{db, t}
}

func (d *DB) ORM(t index.Entity) *ORMS {
	return &ORMS{d, t}
}

func (d *ORMS) DeleteMany(ctx context.Context, field string, value any) *ae.Error {
	qs := fmt.Sprintf("DELETE FROM `%s` WHERE `%s`=?", d.t.Table(), field)
	return d.db.Exec(ctx, qs, value)
}

func (d *ORMS) DeleteOne(ctx context.Context, field string, value any) *ae.Error {
	qs := fmt.Sprintf("DELETE FROM `%s` WHERE `%s`=? LIMIT 1", d.t.Table(), field)
	return d.db.Exec(ctx, qs, value)
}

func (d *ORMS) DeletePK(ctx context.Context, id any) *ae.Error {
	primary, e := d.t.Indexes().PrimaryKey()
	if e != nil {
		return e
	}
	return d.DeleteOne(ctx, primary, id)
}

func (d *ORMS) ExistsOne(ctx context.Context, field string, value any) *ae.Error {
	qs := fmt.Sprintf("SELECT 1 FROM `%s` WHERE `%s`=? LIMIT 1", d.t.Table(), field)
	var newId uint8
	e := d.db.ScanAny(ctx, qs, value, &newId)
	if e != nil {
		return e
	}
	if newId == 1 {
		return nil
	}
	return ae.ErrorNotFound
}

func (d *ORMS) ExistsPK(ctx context.Context, id any) *ae.Error {
	primary, e := d.t.Indexes().PrimaryKey()
	if e != nil {
		return e
	}
	return d.ExistsOne(ctx, primary, id)
}

func (d *ORMS) AlterMany(ctx context.Context, field string, value any, data map[string]any) *ae.Error {
	if len(data) == 0 {
		return ae.ErrorInputTooShort
	}

	var s strings.Builder
	args := make([]any, 0, len(data)+1)
	for k, v := range data {
		args = append(args, v)
		if s.Len() > 0 {
			s.WriteString(",")
		}
		s.WriteByte('`')
		s.WriteString(k)
		s.WriteByte('`')
		s.WriteString("=?")
	}
	args = append(args, value)
	qs := fmt.Sprintf("UPDATE `%s` SET %s WHERE `%s`=?", d.t.Table(), s.String(), field)
	return d.db.Exec(ctx, qs, args...)
}

func (d *ORMS) AlterOne(ctx context.Context, field string, value any, data map[string]any) *ae.Error {
	if len(data) == 0 {
		return ae.ErrorInputTooShort
	}

	var s strings.Builder
	args := make([]any, 0, len(data)+1)
	for k, v := range data {
		args = append(args, v)
		if s.Len() > 0 {
			s.WriteString(",")
		}
		s.WriteByte('`')
		s.WriteString(k)
		s.WriteByte('`')
		s.WriteString("=?")
	}
	args = append(args, value)
	qs := fmt.Sprintf("UPDATE `%s` SET %s WHERE `%s`=? LIMIT 1", d.t.Table(), s.String(), field)
	return d.db.Exec(ctx, qs, args...)
}

func (d *ORMS) Alter(ctx context.Context, id any, data map[string]any) *ae.Error {
	primary, e := d.t.Indexes().PrimaryKey()
	if e != nil {
		return e
	}
	return d.AlterOne(ctx, primary, id, data)
}

func (d *ORMS) Find(ctx context.Context, id any, dst map[string]any) *ae.Error {
	var fields strings.Builder
	dest := make([]any, 0, len(dst))
	for k, v := range dst {
		dest = append(dest, v)
		if fields.Len() > 0 {
			fields.WriteByte(',')
		}
		fields.WriteByte('`')
		fields.WriteString(k)
		fields.WriteByte('`')
	}
	qs := fmt.Sprintf("SELECT %s FROM `%s` WHERE `%s`=?", fields.String(), d.t.Table(), id)
	return d.db.ScanAny(ctx, qs, id, dest...)
}
