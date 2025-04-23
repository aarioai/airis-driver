package redisx

import (
	"context"
	"github.com/aarioai/airis-driver/driver"
	"github.com/aarioai/airis/aa/ae"
	"github.com/redis/go-redis/v9"
	"time"
)

// 申请原子性的锁
func ApplyLock(ctx context.Context, rdb *redis.Client, expires time.Duration, k string) *ae.Error {
	err := rdb.SetNX(ctx, k+":lock", 1, expires).Err()
	return driver.NewRedisError(err)
}
