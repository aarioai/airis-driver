# Mongodb helper 说明文档

* replace 替换全部
* update 替换局部

```
mongodb.ORM(db, Entity)
    .CreateIndexes(ctx)
    .AggregateRaw(ctx, pipeline, opts...)
    .Aggregate(ctx, &results, pipeline, opts...)
    .Drop(ctx, opts...)
    .EstimatedDocumentCount(ctx, opts...)
    .InsertOne(ctx, opts...)
    .InsertMany(ctx, entities, opts...)
    .InsertOrUpdate(ctx, opts...)
    
mongodb.ORM(db, Entity).Where(filter)
    .CountDocuments(ctx, opts...)
    .DeleteOne(ctx, opts...)
    .DeleteMany(ctx, opts...)
    .Distinct(ctx, field, opts...)
    .FindOneRaw(ctx, opts...)
    .FindOne(ctx, &result, opts...)
    .FindRaw(ctx, opts...)
    .Find(ctx, &result, opts...)
    .FindOneAndDelete(ctx, opts...)
    .FindOneAndReplace(ctx, opts...)
    .FindOneAndUpdate(ctx, opts...)
    .ReplaceOne(ctx, opts...)
    .UpdateOne(ctx, update, opts...)
    .UpdateMany(ctx, update, opts...)
    .UpsertOne(ctx, update, opts...)
    .UpsertMany(ctx, update, opts...)
```