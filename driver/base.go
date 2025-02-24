package driver

import (
	"github.com/aarioai/airis/core"
	"strings"
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

func tryGetSectionCfg(app *core.App, base, section string, key string) (string, error) {
	k := section + "." + key
	v, err := app.Config.MustGetString(k)
	if err == nil {
		return v, nil
	}
	if section != base {
		if !strings.HasPrefix(section, base+"_") {
			// 尝试section加 mysql_/redis_/mongodb_... 开头
			return tryGetSectionCfg(app, base, base+"_"+section, key)
		}
		// 读取默认值，即 mysql.$key/redis.$key/mongodb.$key...
		return app.Config.MustGetString(base + "." + key)
	}
	return "", err
}
