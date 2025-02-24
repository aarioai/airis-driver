package driver_test

import (
	"github.com/aarioai/airis-driver/driver"
	"github.com/aarioai/airis/core"
	"github.com/aarioai/airis/core/aconfig"
	"github.com/redis/go-redis/v9"
	"testing"
)

func TestRedis(t *testing.T) {
	c, err := aconfig.New("./test_config.ini", nil)
	if err != nil {
		t.Fatal(err)
	}
	app := core.New(c, nil)

	testRedisOpt := redis.Options{
		Password: "Luexu.com",
		DB:       1,
		Addr:     "https://luexu.com",
	}
	test2RedisOpt := redis.Options{
		Password: "Aario",
		DB:       2,
		Addr:     "luexu.com",
	}

	testRedisConfig(t, app, "redis_test", testRedisOpt)
	testRedisConfig(t, app, "test", testRedisOpt)
	testRedisConfig(t, app, "redis_test2", test2RedisOpt)
}
func testRedisConfig(t *testing.T, app *core.App, section string, want redis.Options) {
	test, err := driver.ParseRedisConfig(app, section)
	if err != nil {
		t.Fatal(err.Error())
	}
	if test.Password != want.Password {
		t.Errorf("test redis password %s not match %s", test.Password, want.Password)
	}
	if test.DB != want.DB {
		t.Errorf("test redis db %d not match %d", test.DB, want.DB)
	}
	if test.Addr != want.Addr {
		t.Errorf("test redis addr %s not match %s", test.Addr, want.Addr)
	}

}
