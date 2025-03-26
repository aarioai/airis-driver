package mongodb

import (
	"context"
	"github.com/aarioai/airis-driver/driver"
	"github.com/aarioai/airis-driver/driver/index"
	"github.com/aarioai/airis/aa/ae"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func FindOrInsert(ctx context.Context, client *mongo.Client, db *mongo.Database,
	t index.Entity, filter any, newEntity func() index.Entity) (*index.Entity,
	*ae.Error) {
	sess, err := client.StartSession()
	if err != nil {
		return nil, driver.NewMongodbError(err)
	}
	defer sess.EndSession(ctx)
	if err = sess.StartTransaction(); err != nil {
		return nil, driver.NewMongodbError(err)
	}

	e := FindOne(ctx, &t, db, t, filter)
	if e == nil {
		return &t, driver.NewMongodbError(sess.CommitTransaction(ctx))
	}
	if !e.IsNotFound() {
		_ = sess.AbortTransaction(ctx)
		return nil, e
	}
	newT := newEntity()
	if _, e = InsertOne(ctx, db, newT); e != nil {
		_ = sess.AbortTransaction(ctx)
		return nil, e
	}
	return &newT, driver.NewMongodbError(sess.CommitTransaction(ctx))
}
