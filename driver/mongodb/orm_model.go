package mongodb

import (
	"context"
	"github.com/aarioai/airis/aa/ae"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func (o *ORMs) CreateIndexes(ctx context.Context) *ae.Error {
	if o.error != nil {
		return o.error
	}
	return CreateIndexes(ctx, o.db, o.entity)
}

func (o *ORMs) AggregateRaw(ctx context.Context, pipeline any, opts ...options.Lister[options.AggregateOptions]) (
	*mongo.Cursor, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	return AggregateRaw(ctx, o.db, o.entity, pipeline, opts...)
}

func (o *ORMs) Aggregate(ctx context.Context, results any, pipeline any, opts ...options.Lister[options.AggregateOptions]) *ae.Error {
	if o.error != nil {
		return o.error
	}
	return Aggregate(ctx, results, o.db, o.entity, pipeline, opts...)
}

func (o *ORMs) CountDocuments(ctx context.Context, opts ...options.Lister[options.CountOptions]) (int64, *ae.Error) {
	if o.error != nil {
		return 0, o.error
	}
	return CountDocuments(ctx, o.db, o.entity, o.Filter(), opts...)
}

func (o *ORMs) DeleteOne(ctx context.Context, opts ...options.Lister[options.DeleteOneOptions]) (*mongo.DeleteResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	return DeleteOne(ctx, o.db, o.entity, o.Filter(), opts...)
}

func (o *ORMs) DeleteMany(ctx context.Context, opts ...options.Lister[options.DeleteManyOptions]) (*mongo.DeleteResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	return DeleteMany(ctx, o.db, o.entity, o.Filter(), opts...)
}

func (o *ORMs) Distinct(ctx context.Context, field string, opts ...options.Lister[options.DistinctOptions]) (*mongo.DistinctResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	return Distinct(ctx, o.db, o.entity, field, o.Filter(), opts...)
}

func (o *ORMs) Drop(ctx context.Context, opts ...options.Lister[options.DropCollectionOptions]) *ae.Error {
	if o.error != nil {
		return o.error
	}
	return Drop(ctx, o.db, o.entity, opts...)
}

func (o *ORMs) EstimatedDocumentCount(ctx context.Context, opts ...options.Lister[options.EstimatedDocumentCountOptions]) (int64, *ae.Error) {
	if o.error != nil {
		return 0, o.error
	}
	return EstimatedDocumentCount(ctx, o.db, o.entity, opts...)
}

func (o *ORMs) findOneOptions(opts ...options.Lister[options.FindOneOptions]) []options.Lister[options.FindOneOptions] {
	if len(o.sort) == 0 && o.offset == 0 {
		return opts
	}
	opt := options.FindOne()
	if len(o.sort) > 0 {
		opt.SetSort(o.sort)
	}
	if o.offset > 0 {
		opt.SetSkip(o.offset)
	}
	return append(opts, opt)
}

func (o *ORMs) FindOneRaw(ctx context.Context, opts ...options.Lister[options.FindOneOptions]) (*mongo.SingleResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	opts = o.findOneOptions(opts...)
	return FindOneRaw(ctx, o.db, o.entity, o.Filter(), opts...)
}

func (o *ORMs) FindOne(ctx context.Context, result any, opts ...options.Lister[options.FindOneOptions]) *ae.Error {
	if o.error != nil {
		return o.error
	}
	opts = o.findOneOptions(opts...)
	return FindOne(ctx, result, o.db, o.entity, o.Filter(), opts...)
}

func (o *ORMs) findOptions(opts ...options.Lister[options.FindOptions]) []options.Lister[options.FindOptions] {
	if len(o.sort) == 0 && o.offset == 0 && o.limit == 0 {
		return opts
	}
	opt := options.Find()
	if len(o.sort) > 0 {
		opt.SetSort(o.sort)
	}
	if o.offset > 0 {
		opt.SetSkip(o.offset)
	}
	if o.limit > 0 {
		opt.SetLimit(o.limit)
	}
	return append(opts, opt)
}

func (o *ORMs) FindManyRaw(ctx context.Context, opts ...options.Lister[options.FindOptions]) (*mongo.Cursor, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	opts = o.findOptions(opts...)
	return FindManyRaw(ctx, o.db, o.entity, o.Filter(), opts...)
}

func (o *ORMs) FindMany(ctx context.Context, result any, opts ...options.Lister[options.FindOptions]) *ae.Error {
	if o.error != nil {
		return o.error
	}
	opts = o.findOptions(opts...)
	return FindMany(ctx, result, o.db, o.entity, o.Filter(), opts...)
}

func (o *ORMs) findOneAndDeleteOptions(opts ...options.Lister[options.FindOneAndDeleteOptions]) []options.Lister[options.FindOneAndDeleteOptions] {
	if len(o.sort) == 0 {
		return opts
	}
	opt := options.FindOneAndDelete()
	if len(o.sort) > 0 {
		opt.SetSort(o.sort)
	}
	return append(opts, opt)
}

func (o *ORMs) FindOneAndDelete(ctx context.Context, opts ...options.Lister[options.FindOneAndDeleteOptions]) (*mongo.SingleResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	opts = o.findOneAndDeleteOptions(opts...)
	return FindOneAndDelete(ctx, o.db, o.entity, o.Filter(), opts...)
}

func (o *ORMs) findOneAndReplaceOptions(opts ...options.Lister[options.FindOneAndReplaceOptions]) []options.Lister[options.FindOneAndReplaceOptions] {
	if len(o.sort) == 0 {
		return opts
	}
	opt := options.FindOneAndReplace()
	if len(o.sort) > 0 {
		opt.SetSort(o.sort)
	}
	return append(opts, opt)
}

func (o *ORMs) FindOneAndReplace(ctx context.Context, opts ...options.Lister[options.FindOneAndReplaceOptions]) (*mongo.SingleResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	opts = o.findOneAndReplaceOptions(opts...)
	return FindOneAndReplace(ctx, o.db, o.entity, o.Filter(), opts...)
}

func (o *ORMs) findOneAndUpdateOptions(opts ...options.Lister[options.FindOneAndUpdateOptions]) []options.Lister[options.FindOneAndUpdateOptions] {
	if len(o.sort) == 0 {
		return opts
	}
	opt := options.FindOneAndUpdate()
	if len(o.sort) > 0 {
		opt.SetSort(o.sort)
	}
	return append(opts, opt)
}

func (o *ORMs) FindOneAndUpdate(ctx context.Context, opts ...options.Lister[options.FindOneAndUpdateOptions]) (*mongo.SingleResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	opts = o.findOneAndUpdateOptions(opts...)
	return FindOneAndUpdate(ctx, o.db, o.entity, o.Filter(), o.update, opts...)
}

func (o *ORMs) Insert(ctx context.Context, opts ...options.Lister[options.InsertOneOptions]) (*mongo.InsertOneResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	return InsertOne(ctx, o.db, o.entity, opts...)
}

func (o *ORMs) replaceOptions(opts ...options.Lister[options.ReplaceOptions]) []options.Lister[options.ReplaceOptions] {
	if len(o.sort) == 0 {
		return opts
	}
	opt := options.Replace()
	if len(o.sort) > 0 {
		opt.SetSort(o.sort)
	}
	return append(opts, opt)
}

func (o *ORMs) ReplaceOne(ctx context.Context, opts ...options.Lister[options.ReplaceOptions]) (*mongo.UpdateResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	opts = o.replaceOptions(opts...)
	return ReplaceOne(ctx, o.db, o.entity, o.Filter(), opts...)
}

func (o *ORMs) updateOneOptions(opts ...options.Lister[options.UpdateOneOptions]) []options.Lister[options.UpdateOneOptions] {
	if len(o.sort) == 0 {
		return opts
	}
	opt := options.UpdateOne()
	if len(o.sort) > 0 {
		opt.SetSort(o.sort)
	}
	return append(opts, opt)
}

func (o *ORMs) UpdateOne(ctx context.Context, update any, opts ...options.Lister[options.UpdateOneOptions]) (*mongo.UpdateResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	opts = o.updateOneOptions(opts...)
	return UpdateOne(ctx, o.db, o.entity, o.Filter(), update, opts...)
}

func (o *ORMs) UpdateMany(ctx context.Context, update any, opts ...options.Lister[options.UpdateManyOptions]) (*mongo.UpdateResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	return UpdateMany(ctx, o.db, o.entity, o.Filter(), update, opts...)
}

func (o *ORMs) UpsertOne(ctx context.Context, update any, opts ...options.Lister[options.UpdateOneOptions]) (*mongo.UpdateResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	o.updateOneOptions(opts...)
	return UpsertOne(ctx, o.db, o.entity, o.Filter(), update, opts...)
}

func (o *ORMs) UpsertMany(ctx context.Context, update any, opts ...options.Lister[options.UpdateManyOptions]) (*mongo.UpdateResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	return UpsertMany(ctx, o.db, o.entity, o.Filter(), update, opts...)
}

func (o *ORMs) InsertOrUpdate(ctx context.Context, opts ...options.Lister[options.UpdateOneOptions]) (*mongo.UpdateResult, *ae.Error) {
	if o.error != nil {
		return nil, o.error
	}
	opts = o.updateOneOptions(opts...)
	return InsertOrUpdate(ctx, o.db, o.entity, opts...)
}
