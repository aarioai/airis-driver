package mongodb

import (
	"context"
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
	if o.comment != nil {
		opts = append(opts, options.Aggregate().SetComment(o.comment))
	}
	return AggregateRaw(ctx, o.db, o.entity, pipeline, opts...)
}

func (o *ORMS) Aggregate(ctx context.Context, results any, pipeline any, opts ...options.Lister[options.AggregateOptions]) *ae.Error {
	if o.error != nil {
		return o.error
	}
	if o.comment != nil {
		opts = append(opts, options.Aggregate().SetComment(o.comment))
	}
	return Aggregate(ctx, results, o.db, o.entity, pipeline, opts...)
}

func (o *ORMS) CountDocuments(ctx context.Context, opts ...options.Lister[options.CountOptions]) (int64, *ae.Error) {
	if o.error != nil {
		return 0, o.error
	}
	if o.comment != nil {
		opts = append(opts, options.Count().SetComment(o.comment))
	}
	return CountDocuments(ctx, o.db, o.entity, o.Filter(), opts...)
}

func (o *ORMS) DeleteOne(ctx context.Context, opts ...options.Lister[options.DeleteOneOptions]) (*mongo.DeleteResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	if o.comment != nil {
		opts = append(opts, options.DeleteOne().SetComment(o.comment))
	}
	return DeleteOne(ctx, o.db, o.entity, o.Filter(), opts...)
}

func (o *ORMS) DeleteMany(ctx context.Context, opts ...options.Lister[options.DeleteManyOptions]) (*mongo.DeleteResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	if o.comment != nil {
		opts = append(opts, options.DeleteMany().SetComment(o.comment))
	}
	return DeleteMany(ctx, o.db, o.entity, o.Filter(), opts...)
}

func (o *ORMS) Distinct(ctx context.Context, field string, opts ...options.Lister[options.DistinctOptions]) (*mongo.DistinctResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	if o.comment != nil {
		opts = append(opts, options.Distinct().SetComment(o.comment))
	}
	return Distinct(ctx, o.db, o.entity, field, o.Filter(), opts...)
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
	if o.comment != nil {
		opts = append(opts, options.EstimatedDocumentCount().SetComment(o.comment))
	}
	return EstimatedDocumentCount(ctx, o.db, o.entity, opts...)
}

func (o *ORMS) FindOneRaw(ctx context.Context, opts ...options.Lister[options.FindOneOptions]) (*mongo.SingleResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	if o.comment != nil {
		opts = append(opts, options.FindOne().SetComment(o.comment))
	}
	if len(o.sort) > 0 {
		opts = append(opts, options.FindOne().SetSort(o.sort))
	}
	return FindOneRaw(ctx, o.db, o.entity, o.Filter(), opts...)
}

func (o *ORMS) FindOne(ctx context.Context, result any, opts ...options.Lister[options.FindOneOptions]) *ae.Error {
	if o.error != nil {
		return o.error
	}
	if o.comment != nil {
		opts = append(opts, options.FindOne().SetComment(o.comment))
	}
	if len(o.sort) > 0 {
		opts = append(opts, options.FindOne().SetSort(o.sort))
	}
	return FindOne(ctx, result, o.db, o.entity, o.Filter(), opts...)
}

func (o *ORMS) FindRaw(ctx context.Context, opts ...options.Lister[options.FindOptions]) (*mongo.Cursor, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	if o.comment != nil {
		opts = append(opts, options.Find().SetComment(o.comment))
	}
	if len(o.sort) > 0 {
		opts = append(opts, options.Find().SetSort(o.sort))
	}
	return FindRaw(ctx, o.db, o.entity, o.Filter(), opts...)
}

func (o *ORMS) Find(ctx context.Context, result any, opts ...options.Lister[options.FindOptions]) *ae.Error {
	if o.error != nil {
		return o.error
	}
	if o.comment != nil {
		opts = append(opts, options.Find().SetComment(o.comment))
	}
	if len(o.sort) > 0 {
		opts = append(opts, options.Find().SetSort(o.sort))
	}
	return Find(ctx, result, o.db, o.entity, o.Filter(), opts...)
}

func (o *ORMS) FindOneAndDelete(ctx context.Context, opts ...options.Lister[options.FindOneAndDeleteOptions]) (*mongo.SingleResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	if o.comment != nil {
		opts = append(opts, options.FindOneAndDelete().SetComment(o.comment))
	}
	if len(o.sort) > 0 {
		opts = append(opts, options.FindOneAndDelete().SetSort(o.sort))
	}
	return FindOneAndDelete(ctx, o.db, o.entity, o.Filter(), opts...)
}

func (o *ORMS) FindOneAndReplace(ctx context.Context, opts ...options.Lister[options.FindOneAndReplaceOptions]) (*mongo.SingleResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	if o.comment != nil {
		opts = append(opts, options.FindOneAndReplace().SetComment(o.comment))
	}
	if len(o.sort) > 0 {
		opts = append(opts, options.FindOneAndReplace().SetSort(o.sort))
	}
	return FindOneAndReplace(ctx, o.db, o.entity, o.Filter(), opts...)
}

func (o *ORMS) FindOneAndUpdate(ctx context.Context, opts ...options.Lister[options.FindOneAndUpdateOptions]) (*mongo.SingleResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	if o.comment != nil {
		opts = append(opts, options.FindOneAndUpdate().SetComment(o.comment))
	}
	if len(o.sort) > 0 {
		opts = append(opts, options.FindOneAndUpdate().SetSort(o.sort))
	}
	return FindOneAndUpdate(ctx, o.db, o.entity, o.Filter(), o.update, opts...)
}

func (o *ORMS) Insert(ctx context.Context, opts ...options.Lister[options.InsertOneOptions]) (*mongo.InsertOneResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	if o.comment != nil {
		opts = append(opts, options.InsertOne().SetComment(o.comment))
	}
	return InsertOne(ctx, o.db, o.entity, opts...)
}

func (o *ORMS) ReplaceOne(ctx context.Context, opts ...options.Lister[options.ReplaceOptions]) (*mongo.UpdateResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	if o.comment != nil {
		opts = append(opts, options.Replace().SetComment(o.comment))
	}
	if len(o.sort) > 0 {
		opts = append(opts, options.Replace().SetSort(o.sort))
	}
	return ReplaceOne(ctx, o.db, o.entity, o.Filter(), opts...)
}

func (o *ORMS) UpdateOne(ctx context.Context, update any, opts ...options.Lister[options.UpdateOneOptions]) (*mongo.UpdateResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	if o.comment != nil {
		opts = append(opts, options.UpdateOne().SetComment(o.comment))
	}
	if len(o.sort) > 0 {
		opts = append(opts, options.UpdateOne().SetSort(o.sort))
	}
	return UpdateOne(ctx, o.db, o.entity, o.Filter(), update, opts...)
}

func (o *ORMS) UpdateMany(ctx context.Context, update any, opts ...options.Lister[options.UpdateManyOptions]) (*mongo.UpdateResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	if o.comment != nil {
		opts = append(opts, options.UpdateMany().SetComment(o.comment))
	}
	return UpdateMany(ctx, o.db, o.entity, o.Filter(), update, opts...)
}

func (o *ORMS) UpsertOne(ctx context.Context, update any, opts ...options.Lister[options.UpdateOneOptions]) (*mongo.UpdateResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	if o.comment != nil {
		opts = append(opts, options.UpdateOne().SetComment(o.comment))
	}
	if len(o.sort) > 0 {
		opts = append(opts, options.UpdateOne().SetSort(o.sort))
	}
	return UpsertOne(ctx, o.db, o.entity, o.Filter(), update, opts...)
}

func (o *ORMS) UpsertMany(ctx context.Context, update any, opts ...options.Lister[options.UpdateManyOptions]) (*mongo.UpdateResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	if o.comment != nil {
		opts = append(opts, options.UpdateMany().SetComment(o.comment))
	}
	return UpsertMany(ctx, o.db, o.entity, o.Filter(), update, opts...)
}

func (o *ORMS) InsertOrUpdate(ctx context.Context, opts ...options.Lister[options.UpdateOneOptions]) (*mongo.UpdateResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	if o.comment != nil {
		opts = append(opts, options.UpdateOne().SetComment(o.comment))
	}
	if len(o.sort) > 0 {
		opts = append(opts, options.UpdateOne().SetSort(o.sort))
	}
	return InsertOrUpdate(ctx, o.db, o.entity, opts...)
}
