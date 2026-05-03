package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisService struct {
	client *redis.Client
	prefix string
	limit  int64
}

func NewRedisService(addr string, prefix string, limit int64) *RedisService {
	rdb := redis.NewClient(&redis.Options{
		Addr: addr,
	})

	return &RedisService{
		client: rdb,
		prefix: prefix,
		limit:  limit,
	}
}

func (r *RedisService) getCurrentKey() string {
	now := time.Now()
	return fmt.Sprintf("%s:%d-%02d", r.prefix, now.Year(), now.Month())
}

func (r *RedisService) calculateTTL() time.Duration {
	now := time.Now()

	nextMonth := time.Date(
		now.Year(),
		now.Month()+1,
		1,
		0, 0, 0, 0,
		now.Location(),
	)

	// 2-day buffer into next month
	expiry := nextMonth.Add(48 * time.Hour)

	return time.Until(expiry)
}

func (r *RedisService) GetCurrentCount(ctx context.Context) (int64, error) {
	key := r.getCurrentKey()

	val, err := r.client.Get(ctx, key).Int64()
	if err == redis.Nil {
		return 0, nil
	}

	return val, err
}

func (r *RedisService) Increment(ctx context.Context) (int64, error) {
	key := r.getCurrentKey()

	count, err := r.client.Incr(ctx, key).Result()
	if err != nil {
		return 0, err
	}

	// Set TTL only when key is first created
	if count == 1 {
		ttl := r.calculateTTL()
		err = r.client.Expire(ctx, key, ttl).Err()
		if err != nil {
			return 0, err
		}
	}

	return count, nil
}

func (r *RedisService) CanSend(ctx context.Context) (bool, int64, error) {
	count, err := r.GetCurrentCount(ctx)
	if err != nil {
		return false, 0, err
	}

	if count > r.limit {
		return false, count, nil
	}

	return true, count, nil
}
