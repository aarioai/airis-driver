package mongodb

import (
	"github.com/aarioai/airis-driver/driver"
	"github.com/aarioai/airis-driver/driver/index"
	"github.com/aarioai/airis/aa"
	"github.com/aarioai/airis/aa/ae"
	"go.mongodb.org/mongo-driver/v2/mongo"
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

func (m *Model) ORM(t index.Entity) *ORMS {
	_, db, e := m.DB()
	if e != nil {
		return ErrorORM(e)
	}
	return ORM(db, t)
}
