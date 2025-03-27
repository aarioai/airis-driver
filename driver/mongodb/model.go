package mongodb

import (
	"context"
	"github.com/aarioai/airis-driver/driver"
	"github.com/aarioai/airis-driver/driver/index"
	"github.com/aarioai/airis/aa"
	"github.com/aarioai/airis/aa/ae"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"time"
)

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

func (m *Model) InsertMany(ctx context.Context, ts []index.Entity, opts ...options.Lister[options.InsertManyOptions]) (*mongo.InsertManyResult, *ae.Error) {
	_, db, e := m.DB()
	if e != nil {
		return nil, e
	}
	return InsertMany(ctx, db, ts, opts...)
}

func (m *Model) ORM(t index.Entity) *ORMS {
	_, db, e := m.DB()
	if e != nil {
		return ErrorORM(e)
	}
	return ORM(db, t)
}
