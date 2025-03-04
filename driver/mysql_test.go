package driver_test

import (
	"github.com/aarioai/airis-driver/driver"
	"github.com/aarioai/airis/aa"
	"github.com/aarioai/airis/aa/aconfig"
	"testing"
	"time"
)

func TestParseIni(t *testing.T) {
	c, err := aconfig.New("./test_config.ini", nil)
	if err != nil {
		t.Fatal(err)
	}
	app := aa.New(c, nil)

	mysqlTestOpt := driver.MysqlOptions{
		Schema:       "test",
		Host:         "luexu.com",
		User:         "Aario",
		Password:     "Luexu.com",
		WriteTimeout: time.Second * 5,
	}
	mysqlHelloOpt := driver.MysqlOptions{
		Schema:       "helloworld",
		Host:         "luexu.com",
		User:         "Aario",
		Password:     "Luexu.com",
		WriteTimeout: time.Second * 5,
	}
	testMySQLConfig(t, app, "mysql_test", mysqlTestOpt)
	testMySQLConfig(t, app, "test", mysqlTestOpt)
	testMySQLConfig(t, app, "hello", mysqlHelloOpt)
}
func testMySQLConfig(t *testing.T, app *aa.App, want string, suppose driver.MysqlOptions) {
	mysqlConfig, err := driver.ParseMysqlConfig(app, want)
	if err != nil {
		t.Fatal(err.Error())
	}
	if mysqlConfig.Host != suppose.Host {
		t.Errorf("test mysql host %s not match %s", mysqlConfig.Host, suppose.Host)
	}
	if mysqlConfig.User != suppose.User {
		t.Errorf("test mysql user %s not match %s", mysqlConfig.User, suppose.User)
	}
	if mysqlConfig.Password != suppose.Password {
		t.Errorf("test mysql password %s not match %s", mysqlConfig.Password, suppose.Password)
	}
	if mysqlConfig.WriteTimeout != suppose.WriteTimeout {
		t.Errorf("test mysql password %d not match %d", mysqlConfig.WriteTimeout, suppose.WriteTimeout)
	}
}
