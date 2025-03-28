package mongodb

import (
	"context"
	"github.com/aarioai/airis-driver/driver"
	"github.com/aarioai/airis-driver/driver/index"
	"github.com/aarioai/airis/aa/ae"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"reflect"
	"strings"
)

var ErrMongodbInsertManyMustInTheSameCollection = ae.NewE("mongodb insert many must in the same collection")

func CreateIndexes(ctx context.Context, db *mongo.Database, t index.Entity) *ae.Error {
	coll := db.Collection(t.Table())
	// Creating indexes in MongoDB is an idempotent operation. So running db.names.createIndex({name:1}) would create the index only if it didn't already exist.
	models := index.ToMongoDBIndexModels(t)
	if len(models) == 0 {
		return nil
	}
	_, err := coll.Indexes().CreateMany(ctx, models)
	return driver.NewMongodbError(err)
}

func AggregateRaw(ctx context.Context, db *mongo.Database, t index.Entity, pipeline any, opts ...options.Lister[options.AggregateOptions]) (*mongo.Cursor, *ae.Error) {
	coll := db.Collection(t.Table())
	cursor, err := coll.Aggregate(ctx, pipeline, opts...)
	if err != nil {
		return nil, driver.NewMongodbError(err)
	}
	return cursor, nil
}

func Aggregate(ctx context.Context, results any, db *mongo.Database, t index.Entity, pipeline any, opts ...options.Lister[options.AggregateOptions]) *ae.Error {
	cursor, e := AggregateRaw(ctx, db, t, pipeline, opts...)
	if e != nil {
		return e
	}
	defer cursor.Close(ctx)
	err := cursor.All(ctx, results)
	return driver.NewMongodbError(err)
}

func CountDocuments(ctx context.Context, db *mongo.Database, t index.Entity, filter any, opts ...options.Lister[options.CountOptions]) (int64, *ae.Error) {
	coll := db.Collection(t.Table())
	result, err := coll.CountDocuments(ctx, filter, opts...)
	if err != nil {
		return 0, driver.NewMongodbError(err)
	}
	return result, nil
}

func DeleteOne(ctx context.Context, db *mongo.Database, t index.Entity, filter any, opts ...options.Lister[options.DeleteOneOptions]) (*mongo.DeleteResult, *ae.Error) {
	coll := db.Collection(t.Table())
	result, err := coll.DeleteOne(ctx, filter, opts...)
	if err != nil {
		return nil, driver.NewMongodbError(err)
	}
	return result, nil
}

func DeleteMany(ctx context.Context, db *mongo.Database, t index.Entity, filter any, opts ...options.Lister[options.DeleteManyOptions]) (*mongo.DeleteResult, *ae.Error) {
	coll := db.Collection(t.Table())
	result, err := coll.DeleteMany(ctx, filter, opts...)
	if err != nil {
		return nil, driver.NewMongodbError(err)
	}
	return result, nil
}

func Distinct(ctx context.Context, db *mongo.Database, t index.Entity, field string, filter any, opts ...options.Lister[options.DistinctOptions]) (*mongo.DistinctResult, *ae.Error) {
	coll := db.Collection(t.Table())
	return coll.Distinct(ctx, field, filter, opts...), nil
}

func Drop(ctx context.Context, db *mongo.Database, t index.Entity, opts ...options.Lister[options.DropCollectionOptions]) *ae.Error {
	coll := db.Collection(t.Table())
	err := coll.Drop(ctx, opts...)
	return driver.NewMongodbError(err)
}

func EstimatedDocumentCount(ctx context.Context, db *mongo.Database, t index.Entity, opts ...options.Lister[options.EstimatedDocumentCountOptions]) (int64, *ae.Error) {
	coll := db.Collection(t.Table())
	result, err := coll.EstimatedDocumentCount(ctx, opts...)
	if err != nil {
		return 0, driver.NewMongodbError(err)
	}
	return result, nil
}

func FindOneRaw(ctx context.Context, db *mongo.Database, t index.Entity, filter any, opts ...options.Lister[options.FindOneOptions]) (*mongo.SingleResult, *ae.Error) {
	coll := db.Collection(t.Table())
	result := coll.FindOne(ctx, filter, opts...)
	if result.Err() != nil {
		return nil, driver.NewMongodbError(result.Err())
	}
	return result, nil
}

func FindOne(ctx context.Context, result any, db *mongo.Database, t index.Entity, filter any, opts ...options.Lister[options.FindOneOptions]) *ae.Error {
	r, e := FindOneRaw(ctx, db, t, filter, opts...)
	if e != nil {
		return e
	}
	err := r.Decode(result)
	if err != nil {
		return driver.NewMongodbError(err)
	}
	return nil
}

func FindManyRaw(ctx context.Context, db *mongo.Database, t index.Entity, filter any, opts ...options.Lister[options.FindOptions]) (*mongo.Cursor, *ae.Error) {
	coll := db.Collection(t.Table())
	cursor, err := coll.Find(ctx, filter, opts...)
	if err != nil {
		e := driver.NewMongodbError(err)
		if e != nil && e.IsNotFound() {
			e = ae.ErrorNoRows
		}
		return nil, e
	}
	return cursor, nil
}

func FindMany(ctx context.Context, results any, db *mongo.Database, t index.Entity, filter any, opts ...options.Lister[options.FindOptions]) *ae.Error {
	cursor, e := FindManyRaw(ctx, db, t, filter, opts...)
	if e != nil {
		return e
	}
	defer cursor.Close(ctx)
	err := cursor.All(ctx, results)
	e = driver.NewMongodbError(err)
	if (e != nil && e.IsNotFound()) || reflect.ValueOf(results).Elem().Len() == 0 {
		e = ae.ErrorNoRows
	}
	return e
}

func FindOneAndDelete(ctx context.Context, db *mongo.Database, t index.Entity, filter any, opts ...options.Lister[options.FindOneAndDeleteOptions]) (*mongo.SingleResult, *ae.Error) {
	coll := db.Collection(t.Table())
	return coll.FindOneAndDelete(ctx, filter, opts...), nil
}

func FindOneAndReplace(ctx context.Context, db *mongo.Database, t index.Entity, filter any, opts ...options.Lister[options.FindOneAndReplaceOptions]) (*mongo.SingleResult, *ae.Error) {
	coll := db.Collection(t.Table())
	return coll.FindOneAndReplace(ctx, filter, t, opts...), nil
}

func FindOneAndUpdate(ctx context.Context, db *mongo.Database, t index.Entity, filter any, update any, opts ...options.Lister[options.FindOneAndUpdateOptions]) (*mongo.SingleResult, *ae.Error) {
	coll := db.Collection(t.Table())
	return coll.FindOneAndUpdate(ctx, filter, update, opts...), nil
}

func InsertOne(ctx context.Context, db *mongo.Database, t index.Entity, opts ...options.Lister[options.InsertOneOptions]) (*mongo.InsertOneResult, *ae.Error) {
	coll := db.Collection(t.Table())
	result, err := coll.InsertOne(ctx, t, opts...)
	if err != nil {
		return nil, driver.NewMongodbError(err)
	}
	return result, nil
}

func InsertMany(ctx context.Context, db *mongo.Database, ts []index.Entity, opts ...options.Lister[options.InsertManyOptions]) (*mongo.InsertManyResult, *ae.Error) {
	if len(ts) == 0 {
		return nil, ae.ErrorEmptyInput
	}
	table := ts[0].Table()
	for _, t := range ts[1:] {
		if t.Table() != table {
			return nil, ErrMongodbInsertManyMustInTheSameCollection
		}
	}
	coll := db.Collection(table)
	result, err := coll.InsertMany(ctx, ts, opts...)
	if err != nil {
		return nil, driver.NewMongodbError(err)
	}
	return result, nil
}

func ReplaceOne(ctx context.Context, db *mongo.Database, t index.Entity, filter any, opts ...options.Lister[options.ReplaceOptions]) (*mongo.UpdateResult, *ae.Error) {
	coll := db.Collection(t.Table())
	result, err := coll.ReplaceOne(ctx, filter, t, opts...)
	if err != nil {
		return nil, driver.NewMongodbError(err)
	}
	return result, nil
}

func UpdateOne(ctx context.Context, db *mongo.Database, t index.Entity, filter any, update any, opts ...options.Lister[options.UpdateOneOptions]) (*mongo.UpdateResult, *ae.Error) {
	coll := db.Collection(t.Table())
	result, err := coll.UpdateOne(ctx, filter, update, opts...)
	if err != nil {
		return nil, driver.NewMongodbError(err)
	}
	return result, nil
}

func UpdateMany(ctx context.Context, db *mongo.Database, t index.Entity, filter any, update any, opts ...options.Lister[options.UpdateManyOptions]) (*mongo.UpdateResult, *ae.Error) {
	coll := db.Collection(t.Table())
	result, err := coll.UpdateMany(ctx, filter, update, opts...)
	if err != nil {
		return nil, driver.NewMongodbError(err)
	}
	return result, nil
}

func UpsertOne(ctx context.Context, db *mongo.Database, t index.Entity, filter any, update any, opts ...options.Lister[options.UpdateOneOptions]) (*mongo.UpdateResult, *ae.Error) {
	opts = append(opts, options.UpdateOne().SetUpsert(true))
	return UpdateOne(ctx, db, t, filter, update, opts...)
}

func UpsertMany(ctx context.Context, db *mongo.Database, t index.Entity, filter any, update any, opts ...options.Lister[options.UpdateManyOptions]) (*mongo.UpdateResult, *ae.Error) {
	if len(opts) == 0 {
		opts = make([]options.Lister[options.UpdateManyOptions], 0, 1)
	}
	opts = append(opts, options.UpdateMany().SetUpsert(true))
	return UpdateMany(ctx, db, t, filter, update, opts...)
}

func InsertOrUpdate(ctx context.Context, db *mongo.Database, t index.Entity, opts ...options.Lister[options.UpdateOneOptions]) (*mongo.UpdateResult, *ae.Error) {
	if len(opts) == 0 {
		opts = make([]options.Lister[options.UpdateOneOptions], 0, 1)
	}
	opts = append(opts, options.UpdateOne().SetUpsert(true))
	uniqueKeys := t.Indexes().List(index.PrimaryT, index.UniqueT)
	p := reflect.TypeOf(t)
	v := reflect.ValueOf(t)

	fi := bson.A{}
	update := bson.D{}
	for _, ukeys := range uniqueKeys {
		uf := bson.A{}
		for _, ukey := range ukeys {
			for i := 0; i < p.NumField(); i++ {
				field := p.Field(i).Tag.Get("bson")
				value := v.Field(i).Interface()
				if ukey == field {
					uf = append(uf, bson.D{{field, value}})
					continue
				}
				if field == "created_at" {
					continue
				}
				ops := p.Field(i).Tag.Get("options")
				if ops != "" && strings.Index(ops, "no_update") > -1 {
					continue
				}
				update = append(update, bson.E{Key: field, Value: value})

			}
		}
		if len(uf) == 1 {
			fi = append(fi, uf[0])
		} else if len(uf) > 1 {
			fi = append(fi, bson.D{{"$and", uf}})
		}
	}
	if len(fi) == 0 {
		return nil, ae.NewE("missing primary or unique key")
	}
	filter := bson.D{}
	if len(fi) == 1 {
		filter = fi[0].(bson.D)
	} else {
		filter = bson.D{{"$or", fi}}
	}
	return UpsertOne(ctx, db, t, filter, bson.D{{"$set", update}}, opts...)
}
