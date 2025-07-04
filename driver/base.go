package driver

import (
	"context"
	"github.com/aarioai/airis/aa"
	"github.com/aarioai/airis/aa/ae"
	"github.com/aarioai/airis/pkg/afmt"
	"github.com/aarioai/airis/pkg/types"
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
	if section == "" {
		section = base
	}
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
		CloseInfluxdbPool()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		CloseMongodbPool(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		CloseMysqlPool()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		CloseRabbitmqPool()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		CloseRedisPool()
	}()

	wg.Wait()
}

func newConfigError(section string, err error) *ae.Error {
	return ae.NewF(ae.VariantAlsoNegotiates, "config section [%s] error: %s", section, err.Error())
}

func parseStrings(s, separator string) []string {
	if s == "" {
		return nil
	}
	s = strings.ReplaceAll(s, " ", "")
	return strings.Split(s, separator)
}
func parseUint16s(s, separator string) []uint16 {
	arr := parseStrings(s, separator)
	if len(arr) == 0 {
		return nil
	}
	ret := make([]uint16, len(arr))
	for i, v := range arr {
		ret[i] = types.ToUint16(v)
	}
	return ret
}
