package mongodbhelper

import (
	"context"
	"github.com/aarioai/airis-driver/driver"
	"github.com/aarioai/airis-driver/driver/index"
	"github.com/aarioai/airis/aa"
	"github.com/aarioai/airis/aa/ae"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"time"
)

type EntityInterface interface {
	Table() string
}

type Model struct {
	app     *aa.App
	section string
	loc     *time.Location
}

func NewDB(app *aa.App, section string) *Model {
	return &Model{app: app, section: section, loc: app.Config.TimeLocation}
}

func (m *Model) DB() (*mongo.Client, *mongo.Database, *ae.Error) {
	client, db, e := driver.NewMongodbPool(m.app, m.section)
	if e != nil {
		return nil, nil, e
	}
	return client, client.Database(db), nil
}

func (m *Model) CreateIndexes(ctx context.Context, t index.Entity) *ae.Error {
	_, db, e := m.DB()
	if e != nil {
		return e
	}
	return CreateIndexes(ctx, db, t)
}

func (m *Model) AggregateRaw(ctx context.Context, t EntityInterface, pipeline interface{}, opts ...options.Lister[options.AggregateOptions]) (*mongo.Cursor, *ae.Error) {
	_, db, e := m.DB()
	if e != nil {
		return nil, e
	}
	return AggregateRaw(ctx, db, t, pipeline, opts...)
}

func (m *Model) Aggregate(ctx context.Context, results interface{}, t EntityInterface, pipeline interface{}, opts ...options.Lister[options.AggregateOptions]) *ae.Error {
	_, db, e := m.DB()
	if e != nil {
		return e
	}
	return Aggregate(ctx, results, db, t, pipeline, opts...)
}

func (m *Model) CountDocuments(ctx context.Context, t EntityInterface, filter interface{}, opts ...options.Lister[options.CountOptions]) (int64, *ae.Error) {
	_, db, e := m.DB()
	if e != nil {
		return 0, e
	}
	return CountDocuments(ctx, db, t, filter, opts...)
}

func (m *Model) DeleteOne(ctx context.Context, t EntityInterface, filter interface{}, opts ...options.Lister[options.DeleteOneOptions]) (*mongo.DeleteResult, *ae.Error) {
	_, db, e := m.DB()
	if e != nil {
		return nil, e
	}
	return DeleteOne(ctx, db, t, filter, opts...)
}

func (m *Model) DeleteMany(ctx context.Context, t EntityInterface, filter interface{}, opts ...options.Lister[options.DeleteManyOptions]) (*mongo.DeleteResult, *ae.Error) {
	_, db, e := m.DB()
	if e != nil {
		return nil, e
	}
	return DeleteMany(ctx, db, t, filter, opts...)
}

func (m *Model) Distinct(ctx context.Context, t EntityInterface, field string, filter interface{}, opts ...options.Lister[options.DistinctOptions]) (*mongo.DistinctResult, *ae.Error) {
	_, db, e := m.DB()
	if e != nil {
		return nil, e
	}
	return Distinct(ctx, db, t, field, filter, opts...)
}

func (m *Model) Drop(ctx context.Context, t EntityInterface, opts ...options.Lister[options.DropCollectionOptions]) *ae.Error {
	_, db, e := m.DB()
	if e != nil {
		return e
	}
	return Drop(ctx, db, t, opts...)
}

func (m *Model) EstimatedDocumentCount(ctx context.Context, t EntityInterface, opts ...options.Lister[options.EstimatedDocumentCountOptions]) (int64, *ae.Error) {
	_, db, e := m.DB()
	if e != nil {
		return 0, e
	}
	return EstimatedDocumentCount(ctx, db, t, opts...)
}

func (m *Model) FindOneRaw(ctx context.Context, t EntityInterface, filter interface{}, opts ...options.Lister[options.FindOneOptions]) (*mongo.SingleResult, *ae.Error) {
	_, db, e := m.DB()
	if e != nil {
		return nil, e
	}
	return FindOneRaw(ctx, db, t, filter, opts...)
}

func (m *Model) FindOne(ctx context.Context, result interface{}, t EntityInterface, filter interface{}, opts ...options.Lister[options.FindOneOptions]) *ae.Error {
	_, db, e := m.DB()
	if e != nil {
		return e
	}
	return FindOne(ctx, result, db, t, filter, opts...)
}

func (m *Model) FindRaw(ctx context.Context, t EntityInterface, filter interface{}, opts ...options.Lister[options.FindOptions]) (*mongo.Cursor, *ae.Error) {
	_, db, e := m.DB()
	if e != nil {
		return nil, e
	}
	return FindRaw(ctx, db, t, filter, opts...)
}

func (m *Model) Find(ctx context.Context, results interface{}, t EntityInterface, filter interface{}, opts ...options.Lister[options.FindOptions]) *ae.Error {
	_, db, e := m.DB()
	if e != nil {
		return e
	}
	return Find(ctx, results, db, t, filter, opts...)
}

func (m *Model) FindOneAndDelete(ctx context.Context, t EntityInterface, filter interface{}, opts ...options.Lister[options.FindOneAndDeleteOptions]) (*mongo.SingleResult, *ae.Error) {
	_, db, e := m.DB()
	if e != nil {
		return nil, e
	}
	return FindOneAndDelete(ctx, db, t, filter, opts...)
}

func (m *Model) FindOneAndReplace(ctx context.Context, t EntityInterface, filter, replace interface{}, opts ...options.Lister[options.FindOneAndReplaceOptions]) (*mongo.SingleResult, *ae.Error) {
	_, db, e := m.DB()
	if e != nil {
		return nil, e
	}
	return FindOneAndReplace(ctx, db, t, filter, replace, opts...)
}

func (m *Model) FindOneAndUpdate(ctx context.Context, t EntityInterface, filter interface{}, update bson.D, opts ...options.Lister[options.FindOneAndUpdateOptions]) (*mongo.SingleResult, *ae.Error) {
	_, db, e := m.DB()
	if e != nil {
		return nil, e
	}
	return FindOneAndUpdate(ctx, db, t, filter, update, opts...)
}

func (m *Model) InsertOne(ctx context.Context, t EntityInterface) (*mongo.InsertOneResult, *ae.Error) {
	_, db, e := m.DB()
	if e != nil {
		return nil, e
	}
	return InsertOne(ctx, db, t)
}

func (m *Model) ReplaceOne(ctx context.Context, t EntityInterface, filter interface{}, opts ...options.Lister[options.ReplaceOptions]) (*mongo.UpdateResult, *ae.Error) {
	_, db, e := m.DB()
	if e != nil {
		return nil, e
	}
	return ReplaceOne(ctx, db, t, filter, opts...)
}

func (m *Model) UpdateOne(ctx context.Context, t EntityInterface, filter interface{}, update bson.D, opts ...options.Lister[options.UpdateOneOptions]) (*mongo.UpdateResult, *ae.Error) {
	_, db, e := m.DB()
	if e != nil {
		return nil, e
	}
	return UpdateOne(ctx, db, t, filter, update, opts...)
}

func (m *Model) UpdateByObjectId(ctx context.Context, t EntityInterface, id string, update bson.D, opts ...options.Lister[options.UpdateOneOptions]) (*mongo.UpdateResult, *ae.Error) {
	return m.UpdateOne(ctx, t, bson.D{{"_id", id}}, update, opts...)
}

func (m *Model) UpdateMany(ctx context.Context, t EntityInterface, filter interface{}, update bson.D, opts ...options.Lister[options.UpdateManyOptions]) (*mongo.UpdateResult, *ae.Error) {
	_, db, e := m.DB()
	if e != nil {
		return nil, e
	}
	return UpdateMany(ctx, db, t, filter, update, opts...)
}

// UpsertOne update or insert one
func (m *Model) UpsertOne(ctx context.Context, t EntityInterface, filter interface{}, update bson.D, opts ...options.Lister[options.UpdateOneOptions]) (*mongo.UpdateResult, *ae.Error) {
	_, db, e := m.DB()
	if e != nil {
		return nil, e
	}
	return UpdateOne(ctx, db, t, filter, update, opts...)
}

// UpsertMany update or insert many
func (m *Model) UpsertMany(ctx context.Context, t EntityInterface, filter interface{}, update bson.D, opts ...options.Lister[options.UpdateManyOptions]) (*mongo.UpdateResult, *ae.Error) {
	_, db, e := m.DB()
	if e != nil {
		return nil, e
	}
	return UpdateMany(ctx, db, t, filter, update, opts...)
}

func (m *Model) InsertOrUpdate(ctx context.Context, t index.Entity, opts ...options.Lister[options.UpdateOneOptions]) (*mongo.UpdateResult, *ae.Error) {
	_, db, e := m.DB()
	if e != nil {
		return nil, e
	}
	return InsertOrUpdate(ctx, db, t, opts...)
}
