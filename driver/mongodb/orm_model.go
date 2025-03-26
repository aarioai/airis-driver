package mongodb

import (
	"context"
	"github.com/aarioai/airis-driver/driver/index"
	"github.com/aarioai/airis/aa/ae"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func (o *ORMS) CreateIndexes(ctx context.Context) *ae.Error {
	if o.error != nil {
		return o.error
	}
	return CreateIndexes(ctx, o.db, o.entity)
}

func (o *ORMS) AggregateRaw(ctx context.Context, pipeline any, opts ...options.Lister[options.AggregateOptions]) (
	*mongo.Cursor, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	return AggregateRaw(ctx, o.db, o.entity, pipeline, opts...)
}

func (o *ORMS) Aggregate(ctx context.Context, results any, pipeline any, opts ...options.Lister[options.AggregateOptions]) *ae.Error {
	if o.error != nil {
		return o.error
	}
	return Aggregate(ctx, results, o.db, o.entity, pipeline, opts...)
}

func (o *ORMS) CountDocuments(ctx context.Context, opts ...options.Lister[options.CountOptions]) (int64, *ae.Error) {
	if o.error != nil {
		return 0, o.error
	}
	return CountDocuments(ctx, o.db, o.entity, o.filter, opts...)
}

func (o *ORMS) DeleteOne(ctx context.Context, opts ...options.Lister[options.DeleteOneOptions]) (*mongo.DeleteResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	return DeleteOne(ctx, o.db, o.entity, o.filter, opts...)
}

func (o *ORMS) DeleteMany(ctx context.Context, opts ...options.Lister[options.DeleteManyOptions]) (*mongo.DeleteResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	return DeleteMany(ctx, o.db, o.entity, o.filter, opts...)
}

func (o *ORMS) Distinct(ctx context.Context, field string, opts ...options.Lister[options.DistinctOptions]) (*mongo.DistinctResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	return Distinct(ctx, o.db, o.entity, field, o.filter, opts...)
}

func (o *ORMS) Drop(ctx context.Context, opts ...options.Lister[options.DropCollectionOptions]) *ae.Error {
	if o.error != nil {
		return o.error
	}
	return Drop(ctx, o.db, o.entity, opts...)
}

func (o *ORMS) EstimatedDocumentCount(ctx context.Context, opts ...options.Lister[options.EstimatedDocumentCountOptions]) (int64, *ae.Error) {
	if o.error != nil {
		return 0, o.error
	}
	return EstimatedDocumentCount(ctx, o.db, o.entity, opts...)
}

func (o *ORMS) FindOneRaw(ctx context.Context, opts ...options.Lister[options.FindOneOptions]) (*mongo.SingleResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	return FindOneRaw(ctx, o.db, o.entity, o.filter, opts...)
}

func (o *ORMS) FindOne(ctx context.Context, result any, opts ...options.Lister[options.FindOneOptions]) *ae.Error {
	if o.error != nil {
		return o.error
	}
	return FindOne(ctx, result, o.db, o.entity, o.filter, opts...)
}

func (o *ORMS) FindRaw(ctx context.Context, opts ...options.Lister[options.FindOptions]) (*mongo.Cursor, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	return FindRaw(ctx, o.db, o.entity, o.filter, opts...)
}

func (o *ORMS) Find(ctx context.Context, result any, opts ...options.Lister[options.FindOptions]) *ae.Error {
	if o.error != nil {
		return o.error
	}
	return Find(ctx, result, o.db, o.entity, o.filter, opts...)
}

func (o *ORMS) FindOneAndDelete(ctx context.Context, opts ...options.Lister[options.FindOneAndDeleteOptions]) (*mongo.SingleResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	return FindOneAndDelete(ctx, o.db, o.entity, o.filter, opts...)
}

func (o *ORMS) FindOneAndReplace(ctx context.Context, opts ...options.Lister[options.FindOneAndReplaceOptions]) (*mongo.SingleResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	return FindOneAndReplace(ctx, o.db, o.entity, o.filter, opts...)
}

func (o *ORMS) FindOneAndUpdate(ctx context.Context, opts ...options.Lister[options.FindOneAndUpdateOptions]) (*mongo.SingleResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	return FindOneAndUpdate(ctx, o.db, o.entity, o.filter, o.update, opts...)
}

func (o *ORMS) Insert(ctx context.Context, opts ...options.Lister[options.InsertOneOptions]) (*mongo.InsertOneResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	return InsertOne(ctx, o.db, o.entity, opts...)
}

func (o *ORMS) InsertMany(ctx context.Context, ts []index.Entity, opts ...options.Lister[options.InsertManyOptions]) (*mongo.InsertManyResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	return InsertMany(ctx, o.db, ts, opts...)
}

func (o *ORMS) ReplaceOne(ctx context.Context, opts ...options.Lister[options.ReplaceOptions]) (*mongo.UpdateResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	return ReplaceOne(ctx, o.db, o.entity, o.filter, opts...)
}

func (o *ORMS) UpdateOne(ctx context.Context, update any, opts ...options.Lister[options.UpdateOneOptions]) (*mongo.UpdateResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	return UpdateOne(ctx, o.db, o.entity, o.filter, update, opts...)
}

func (o *ORMS) UpdateMany(ctx context.Context, update any, opts ...options.Lister[options.UpdateManyOptions]) (*mongo.UpdateResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	return UpdateMany(ctx, o.db, o.entity, o.filter, update, opts...)
}

func (o *ORMS) UpsertOne(ctx context.Context, update any, opts ...options.Lister[options.UpdateOneOptions]) (*mongo.UpdateResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	return UpsertOne(ctx, o.db, o.entity, o.filter, update, opts...)
}

func (o *ORMS) UpsertMany(ctx context.Context, update any, opts ...options.Lister[options.UpdateManyOptions]) (*mongo.UpdateResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	return UpsertMany(ctx, o.db, o.entity, o.filter, update, opts...)
}

func (o *ORMS) InsertOrUpdate(ctx context.Context, opts ...options.Lister[options.UpdateOneOptions]) (*mongo.UpdateResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	return InsertOrUpdate(ctx, o.db, o.entity, opts...)
}
