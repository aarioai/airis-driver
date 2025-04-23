package mongodb

import (
	"github.com/aarioai/airis-driver/driver/index"
	"github.com/aarioai/airis-driver/driver/mongodb/bson2"
	"github.com/aarioai/airis-driver/driver/mongodb/bson3"
	"github.com/aarioai/airis/aa/ae"
	"github.com/aarioai/airis/aa/atype"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type connector bool

const (
	and connector = false
	or  connector = true
)

type filter struct {
	connector connector
	value     any
}

type ORMs struct {
	db         *mongo.Database
	entity     index.Entity
	update     any
	baseFilter any
	filters    []filter
	sort       bson.D
	offset     int64
	limit      int64
	error      *ae.Error
}

func ErrorORM(e *ae.Error) *ORMs {
	return &ORMs{error: e}
}

func ORM(db *mongo.Database, entity index.Entity) *ORMs {
	return &ORMs{
		db:      db,
		entity:  entity,
		filters: make([]filter, 0),
		sort:    make(bson.D, 0),
	}
}

// parseFilter(bson.M)
// parseFilter("filed", 100)
// parseFilter("filed", "=", 100)
// operator:
// = > >= < <= != $eq $gt $gte $lt $lte $ne
// & | ^
// $in $nin $all $size $elemMatch $addToSet $pop $pull $push $pushAll $each $position $slice $sort
func parseFilter(args ...any) any {
	switch len(args) {
	case 0:
		return nil
	case 1:
		return args[0]
	case 2:
		return bson.M{args[0].(string): args[1]}
	case 3:
		return bson3.C(args[0].(string), args[1].(string), args[2])
	}
	panic("parseFilter: invalid arguments")
}

func (o *ORMs) WithError(e *ae.Error) *ORMs {
	if o.error == nil {
		o.error = e
	}
	return o
}

func (o *ORMs) Filter() any {
	if len(o.filters) == 0 {
		return o.baseFilter
	}
	result := make([]any, 0)
	conn := o.filters[0].connector
	values := make([]any, 0)
	if o.baseFilter != nil {
		values = append(values, o.baseFilter)
	}
	values = append(values, o.filters[0])
	for i := 1; i < len(o.filters); i++ {
		f := o.filters[i]
		if conn == f.connector {
			values = append(values, f.value)
			continue
		}
		if conn == and {
			result = append(result, bson2.And(values...))
		} else {
			result = append(result, bson2.Or(values...))
		}
		values = values[:0]
		conn = f.connector
	}

	if len(values) > 0 {
		if conn == and {
			result = append(result, bson2.And(values...))
		} else {
			result = append(result, bson2.Or(values...))
		}
	}
	if len(result) == 1 {
		return result[0]
	}
	return bson2.And(result...)
}

func (o *ORMs) Where(args ...any) *ORMs {
	o.baseFilter = parseFilter(args...)
	return o
}

func (o *ORMs) WhereExists(field string) *ORMs {
	o.baseFilter = bson3.Exists(field)
	return o
}

func (o *ORMs) WhereNotExists(field string) *ORMs {
	o.baseFilter = bson3.NotExists(field)
	return o
}

// And
// E.g. Where("a",100).And("b", "nin" bson.A{10,20,30}).And("c", "$all", bson.A{1,2,3})
func (o *ORMs) And(args ...any) *ORMs {
	value := parseFilter(args...)
	o.filters = append(o.filters, filter{and, value})
	return o
}

func (o *ORMs) AndExists(field string) *ORMs {
	value := bson3.Exists(field)
	o.filters = append(o.filters, filter{and, value})
	return o
}

func (o *ORMs) AndNotExists(field string) *ORMs {
	value := bson3.NotExists(field)
	o.filters = append(o.filters, filter{and, value})
	return o
}

func (o *ORMs) Or(args ...any) *ORMs {
	value := parseFilter(args...)
	o.filters = append(o.filters, filter{or, value})
	return o
}

func (o *ORMs) OrExists(field string) *ORMs {
	value := bson3.Exists(field)
	o.filters = append(o.filters, filter{or, value})
	return o
}

func (o *ORMs) NotExists(field string) *ORMs {
	value := bson3.NotExists(field)
	o.filters = append(o.filters, filter{or, value})
	return o
}

func (o *ORMs) DescBy(field string) *ORMs {
	o.sort = append(o.sort, bson.E{Key: field, Value: -1})
	return o
}

func (o *ORMs) AscBy(field string) *ORMs {
	o.sort = append(o.sort, bson.E{Key: field, Value: 1})
	return o
}

// OrderBy
// E.g. OrderBy("id", "DESC", "age", "ASC")
func (o *ORMs) OrderBy(pairs ...string) *ORMs {
	for i := 0; i < len(pairs); i += 2 {
		value := -1
		if pairs[i+1] == "ASC" {
			value = 1
		}
		o.sort = append(o.sort, bson.E{Key: pairs[i], Value: value})
	}
	return o
}

func (o *ORMs) Sort(sort ...bson.E) *ORMs {
	o.sort = append(o.sort, sort...)
	return o
}

func (o *ORMs) Limit(offset, limit int64) *ORMs {
	o.offset = offset
	o.limit = limit
	return o
}

func (o *ORMs) LimitN(offset, limit int) *ORMs {
	o.offset = int64(offset)
	o.limit = int64(limit)
	return o
}

func (o *ORMs) Paging(paging atype.Paging) *ORMs {
	return o.LimitN(paging.Offset, paging.Limit)
}
