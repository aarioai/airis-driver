package redisx

import (
	"context"
	"github.com/aarioai/airis-driver/driver"
	"github.com/aarioai/airis/pkg/types"
	"github.com/redis/go-redis/v9"
	"log"
	"time"

	"github.com/aarioai/airis/aa/ae"
)

// 一般用于 SUnion
func Uint64s(vs []string, err error) ([]uint64, *ae.Error) {
	if err != nil {
		return nil, driver.NewRedisError(err)
	}
	if len(vs) == 0 {
		return nil, ae.ErrorNoRowsAvailable
	}
	ids := make([]uint64, len(vs))
	for i, v := range vs {
		ids[i], err = types.ParseUint64(v)
		if err != nil {
			log.Printf("redis uint64s %s is not uint64\n", v)
		}
	}
	return ids, nil
}
func HIncrBy(ctx context.Context, rdb *redis.Client, expires time.Duration, k string, field string, incr int64) (int64, *ae.Error) {
	var reply int64
	var err error
	ttl, _ := rdb.TTL(ctx, k).Result()
	if ttl > 0 && ttl < expires {
		reply, err = rdb.HIncrBy(ctx, k, field, incr).Result()
	} else {
		_, err = rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			var err1 error
			reply, err1 = pipe.HIncrBy(ctx, k, field, incr).Result()
			err2 := pipe.Expire(ctx, k, expires).Err()
			return ae.FirstError(err1, err2)
		})
	}
	return reply, driver.NewRedisError(err)
}

func HIncr(ctx context.Context, rdb *redis.Client, ttl time.Duration, k string, field string) (int64, *ae.Error) {
	return HIncrBy(ctx, rdb, ttl, k, field, 1)
}

func HMIncr(ctx context.Context, rdb *redis.Client, expires time.Duration, k string, fields []string) ([]int64, *ae.Error) {
	replies := make([]int64, len(fields))
	var err error
	ttl, _ := rdb.TTL(ctx, k).Result()

	_, err = rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		var err1 error
		for i, field := range fields {
			if replies[i], err1 = pipe.HIncrBy(ctx, k, field, 1).Result(); err1 != nil {
				return err1
			}
		}
		if ttl <= 0 {
			err1 = pipe.Expire(ctx, k, expires).Err()
		}
		return err1
	})

	return replies, driver.NewRedisError(err)
}

func HMIncrIds(ctx context.Context, rdb *redis.Client, expires time.Duration, k string, ids []uint64) ([]int64, *ae.Error) {
	fields := make([]string, len(ids))
	for i, id := range ids {
		fields[i] = types.FormatUint(id)
	}
	return HMIncr(ctx, rdb, expires, k, fields)
}
