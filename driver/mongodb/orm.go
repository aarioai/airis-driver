package mongodb

import (
	"github.com/aarioai/airis-driver/driver/index"
	"github.com/aarioai/airis-driver/driver/mongodb/bson2"
	"github.com/aarioai/airis-driver/driver/mongodb/bson3"
	"github.com/aarioai/airis/aa/ae"
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

type ORMS struct {
	db         *mongo.Database
	entity     index.Entity
	update     any
	baseFilter any
	filters    []filter
	error      *ae.Error
}

func ErrorORM(e *ae.Error) *ORMS {
	return &ORMS{error: e}
}

func ORM(db *mongo.Database, entity index.Entity) *ORMS {
	return &ORMS{
		db:      db,
		entity:  entity,
		filters: make([]filter, 0),
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

func (o *ORMS) WithError(e *ae.Error) *ORMS {
	if o.error == nil {
		o.error = e
	}
	return o
}

func (o *ORMS) Filter() any {
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

func (o *ORMS) Where(args ...any) *ORMS {
	o.baseFilter = parseFilter(args...)
	return o
}

func (o *ORMS) WhereExists(field string) *ORMS {
	o.baseFilter = bson3.Exists(field)
	return o
}

func (o *ORMS) WhereNotExists(field string) *ORMS {
	o.baseFilter = bson3.NotExists(field)
	return o
}

// And
// E.g. Where("a",100).And("b", "nin" bson.A{10,20,30}).And("c", "$all", bson.A{1,2,3})
func (o *ORMS) And(args ...any) *ORMS {
	value := parseFilter(args...)
	o.filters = append(o.filters, filter{and, value})
	return o
}

func (o *ORMS) AndExists(field string) *ORMS {
	value := bson3.Exists(field)
	o.filters = append(o.filters, filter{and, value})
	return o
}

func (o *ORMS) AndNotExists(field string) *ORMS {
	value := bson3.NotExists(field)
	o.filters = append(o.filters, filter{and, value})
	return o
}

func (o *ORMS) Or(args ...any) *ORMS {
	value := parseFilter(args...)
	o.filters = append(o.filters, filter{or, value})
	return o
}

func (o *ORMS) OrExists(field string) *ORMS {
	value := bson3.Exists(field)
	o.filters = append(o.filters, filter{or, value})
	return o
}

func (o *ORMS) NotExists(field string) *ORMS {
	value := bson3.NotExists(field)
	o.filters = append(o.filters, filter{or, value})
	return o
}
