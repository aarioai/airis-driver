package mongodb

import (
	"github.com/aarioai/airis-driver/driver/index"
	"github.com/aarioai/airis-driver/driver/mongodb/bson3"
	"github.com/aarioai/airis/aa/ae"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type ORMS struct {
	db     *mongo.Database
	entity index.Entity
	update any
	filter any
	error  *ae.Error
}

func ErrorORM(e *ae.Error) *ORMS {
	return &ORMS{error: e}
}

func ORM(db *mongo.Database, entity index.Entity) *ORMS {
	return &ORMS{
		db:     db,
		entity: entity,
	}
}

func (o *ORMS) WithError(e *ae.Error) *ORMS {
	if o.error == nil {
		o.error = e
	}
	return o
}

func (o *ORMS) Where(filter any) *ORMS {
	o.filter = filter
	return o
}

func (o *ORMS) WhereObjectIs(id string) *ORMS {
	o.filter = bson.M{"_id": id}
	return o
}

func (o *ORMS) WhereIs(key string, value any) *ORMS {
	o.filter = bson.M{key: value}
	return o
}
func (o *ORMS) WhereIn(key string, values ...any) *ORMS {
	o.filter = bson3.In(key, values...)
	return o
}
func (o *ORMS) WhereNotIn(key string, values ...any) *ORMS {
	o.filter = bson3.Nin(key, values...)
	return o
}
