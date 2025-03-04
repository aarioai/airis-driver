package driver

import (
	"context"
	"github.com/aarioai/airis/aa"
	"github.com/aarioai/airis/aa/alog"
	"github.com/aarioai/airis/pkg/afmt"
	"strings"
	"sync"
	"time"
)

func allEmpty(values ...string) bool {
	for _, v := range values {
		if v != "" {
			return false
		}
	}
	return true
}

// ParseTimeouts parses connection, read, write timeout
// 10s, 1000ms
func ParseTimeouts(t string, defaultTimeouts ...time.Duration) (conn time.Duration, read time.Duration, write time.Duration) {
	for i, t := range defaultTimeouts {
		switch i {
		case 0:
			conn = t
		case 1:
			read = t
		case 2:
			write = t
		}
	}

	ts := strings.Split(strings.Replace(t, " ", "", -1), ",")
	for i, t := range ts {
		switch i {
		case 0:
			if conn2, err := time.ParseDuration(t); err == nil && conn2 > 0 {
				conn = conn2
			}
		case 1:
			if read2, err := time.ParseDuration(t); err == nil && read2 > 0 {
				read = read2
			}
		case 2:
			if write2, err := time.ParseDuration(t); err == nil && write2 > 0 {
				write = write2
			}
		}
	}

	return
}

func tryGetSectionCfg(app *aa.App, base, section string, key string, defaultValue ...string) (string, error) {
	k := section + "." + key
	v, err := app.Config.MustGetString(k)
	defaultV := afmt.First(defaultValue)
	if err != nil {
		if section != base {
			if !strings.HasPrefix(section, base+"_") {
				// 尝试section加 mysql_/redis_/mongodb_... 开头
				return tryGetSectionCfg(app, base, base+"_"+section, key)
			}
			// 读取默认值，即 mysql.$key/redis.$key/mongodb.$key...
			v, err = app.Config.MustGetString(base + "." + key)
		}
	}
	if v == "" {
		v = defaultV
	}
	return v, err
}

func CloseAllPools(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		alog.Console("closing mongodb pool")
		CloseMongodbPool(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		alog.Console("closing mysql pool")
		CloseMysqlPool()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		alog.Console("closing rabbitmq pool")
		CloseRabbitmqPool()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		alog.Console("closing redis pool")
		CloseRedisPool()
	}()

	wg.Wait()
}
