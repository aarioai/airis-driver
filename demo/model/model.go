package model

import (
	"sync"
	"time"

	"github.com/aarioai/airis-driver/driver"
	"github.com/aarioai/airis-driver/driver/sqlx"
	"github.com/aarioai/airis/aa"
)

type Model struct {
	app *aa.App
	loc *time.Location
}

var (
	modelOnce sync.Once
	modelObj  *Model
)

func New(app *aa.App) *Model {
	modelOnce.Do(func() {
		modelObj = &Model{app: app, loc: app.Config.TimeLocation}
	})
	return modelObj
}

func (m *Model) db() *sqlx.DB {
	return sqlx.NewDriver(driver.NewMysqlPool(m.app, "mysql"))
}
