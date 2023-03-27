package redis

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/aluka-7/cache"
	"github.com/aluka-7/utils"
	"github.com/go-redis/redis/v8"
)

func init() {
	fmt.Println("Register Redis Cache Engine")
	cache.Register("redis", &cacheRedisDriver{})
}

// cacheRedisDriver导出以直接访问驱动程序.
// 通常,通过base/cache软件包使用驱动程序
type cacheRedisDriver struct{}

func (c cacheRedisDriver) New(ctx context.Context, cfg map[string]string) cache.Provider {
	var database = 0
	if v, ok := cfg["database"]; ok {
		database = utils.StrTo(v).MustInt()
	}
	ping, _ := strconv.ParseBool(cfg["ping"])
	client := redis.NewClient(&redis.Options{Addr: cfg["host"] + ":" + cfg["port"], Password: cfg["password"], DB: database})
	if ping {
		if pin, err := client.Ping(ctx).Result(); err != nil {
			log.Panicf("Redis Ping:%+v\n", err)
		} else {
			log.Printf("Redis Pong:%s\n", pin)
		}
	}
	r := &SingleRedisProvider{client: client}
	return r
}

/**
 * SingleRedisProvider
 * 基于Redis单实例的缓存实现(单机），配置中心或构造方法中参数的配置格式如下：
 * <pre>
 * {
 *   "host" : "缓存服务器主机地址，必须",
 *   "port" : "缓存服务器端口号",
 *   "sasl" : "是否开启安全认证，true|false，可选，默认是没有",
 *   "password" : "开启安全认证后的登录密码，sasl如果指定为true则必须"
 * }
 * </pre>
 */
type SingleRedisProvider struct {
	client *redis.Client
}

// Exists
// 判断缓存中是否存在指定的key
func (r SingleRedisProvider) Exists(ctx context.Context, key string) bool {
	num, err := r.client.Exists(ctx, key).Result()
	return num > 0 && err == nil
}

// String
// 根据给定的key从分布式缓存中读取数据并返回，如果不存在或已过期则返回Null。
func (r SingleRedisProvider) String(ctx context.Context, key string) string {
	return r.client.Get(ctx, key).Val()
}

// Set 使用指定的key将对象存入分布式缓存中，并使用缓存的默认过期设置，注意，存入的对象必须是可序列化的。
func (r SingleRedisProvider) Set(ctx context.Context, key, value string) bool {
	return r.SetExpires(ctx, key, value, -1)
}

// SetExpires 使用指定的key将对象存入分部式缓存中，并指定过期时间，注意，存入的对象必须是可序列化的
func (r SingleRedisProvider) SetExpires(ctx context.Context, key, value string, expires time.Duration) bool {
	err := r.client.Set(ctx, key, value, expires).Err()
	return err == nil
}

// Delete 从缓存中删除指定key的缓存数据。
func (r SingleRedisProvider) Delete(ctx context.Context, key string) bool {
	err := r.client.Del(ctx, key).Err()
	return err == nil
}

// BatchDelete 批量删除缓存中的key。
func (r SingleRedisProvider) BatchDelete(ctx context.Context, keys ...string) bool {
	err := r.client.Del(ctx, keys...).Err()
	return err == nil
}

// HSet 将指定key的map数据的某个字段设置为给定的值。
func (r SingleRedisProvider) HSet(ctx context.Context, key, field, value string) bool {
	err := r.client.HSet(ctx, key, field, value).Err()
	return err == nil
}

// HGet 获取指定key的map数据某个字段的值，如果不存在则返回Null
func (r SingleRedisProvider) HGet(ctx context.Context, key, field string) string {
	return r.client.HGet(ctx, key, field).Val()

}

// HGetAll 获取指定key的map对象，如果不存在则返回Null
func (r SingleRedisProvider) HGetAll(ctx context.Context, key string) map[string]string {
	return r.client.HGetAll(ctx, key).Val()
}

// HDelete 将指定key的map数据中的某个字段删除。
func (r SingleRedisProvider) HDelete(ctx context.Context, key string, fields ...string) bool {
	err := r.client.HDel(ctx, key, fields...).Err()
	return err == nil
}

// HExists 判断缓存中指定key的map是否存在指定的字段，如果key或字段不存在则返回false。
func (r SingleRedisProvider) HExists(ctx context.Context, key, field string) bool {
	v, _ := r.client.HExists(ctx, key, field).Result()
	return v
}

// Val 对指定的key结果集执行指定的脚本并返回最终脚本执行的结果。
func (r SingleRedisProvider) Val(ctx context.Context, script string, keys []string, args ...interface{}) string {
	v, _ := r.client.Eval(ctx, script, keys, args...).Text()
	return v
}

// Incr 对指定的key结果集执行自增操作
func (r SingleRedisProvider) Incr(ctx context.Context, key string) bool {
	err := r.client.Incr(ctx, key).Err()
	return err == nil
}

func (r SingleRedisProvider) IncrExpires(ctx context.Context, key string, expires time.Duration) bool {
	tx := r.client.TxPipeline()
	err := tx.Incr(ctx, key).Err()
	if err != nil {
		return false
	}
	err = tx.Expire(ctx, key, expires).Err()
	if err != nil {
		return false
	}
	_, err = tx.Exec(ctx)
	return err == nil
}

func (r SingleRedisProvider) IncrByExpires(ctx context.Context, key string, value int64, expires time.Duration) bool {
	tx := r.client.TxPipeline()
	err := tx.IncrBy(ctx, key, value).Err()
	if err != nil {
		return false
	}
	err = tx.Expire(ctx, key, expires).Err()
	if err != nil {
		return false
	}
	_, err = tx.Exec(ctx)
	return err == nil
}

// Operate 通过直接调用缓存客户端进行缓存操作，该操作适用于高级操作，如果执行失败会返回Null。
func (r *SingleRedisProvider) Operate(ctx context.Context, cmd interface{}) error {
	_cmd := cmd.(*redis.Cmd)
	return r.client.Process(ctx, _cmd)
}

// Close 关闭客户端
func (r SingleRedisProvider) Close() {
	_ = r.client.Close()
}
