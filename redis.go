package redis

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/aluka-7/cache"
	"github.com/aluka-7/utils"
	"github.com/go-redis/redis"
)

func init() {
	fmt.Println("Register Redis Cache Engine")
	cache.Register("redis", &cacheRedisDriver{})
}

// cacheRedisDriver导出以直接访问驱动程序.
// 通常,通过base/cache软件包使用驱动程序
type cacheRedisDriver struct{}

func (c cacheRedisDriver) New(cfg map[string]string) cache.Provider {
	var database = 0
	if v, ok := cfg["database"]; ok {
		database = utils.StrTo(v).MustInt()
	}
	ping, _ := strconv.ParseBool(cfg["ping"])
	client := redis.NewClient(&redis.Options{Addr: cfg["host"] + ":" + cfg["port"], Password: cfg["password"], DB: database})
	if ping {
		if pin, err := client.Ping().Result(); err != nil {
			log.Panicf("Redis Ping:%+v\n", err)
		} else {
			log.Printf("Redis Pong:%s\n", pin)
		}
	}
	r := &SingleRedisProvider{client: client}
	return r
}

/**
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

/**
 * 判断缓存中是否存在指定的key
 *
 * @param key
 * @return
 */
func (r SingleRedisProvider) Exists(key string) bool {
	num, err := r.client.Exists(key).Result()
	return num > 0 && err == nil
}

/**
 * 根据给定的key从分布式缓存中读取数据并返回，如果不存在或已过期则返回Null。
 *
 * @param key 缓存唯一键
 * @return
 */

func (r SingleRedisProvider) String(key string) string {
	return r.client.Get(key).Val()
}

/**
 * 使用给定的key从缓存中查询数据，如果查询不到则使用给定的数据提供器来查询数据，然后将数据存入缓存中再返回。
 * @param key 缓存唯一键
 * @param dataProvider 数据提供器
 * @return
 */
func (r SingleRedisProvider) GetByProvider(key string, provider cache.DataProvider) string {
	v, err := r.client.Get(key).Result()
	if err == redis.Nil {
		v = provider.Data(key)
		if v != "null" {
			expiry := provider.Expires()
			if expiry > 0 {
				r.SetExpires(key, v, time.Duration(expiry))
			} else {
				r.Set(key, v)
			}
		}
	}
	return v
}

/**
 * 使用指定的key将对象存入分布式缓存中，并使用缓存的默认过期设置，注意，存入的对象必须是可序列化的。
 *
 * @param key   缓存唯一键
 * @param value 对应的值
 */
func (r SingleRedisProvider) Set(key, value string) {
	r.SetExpires(key, value, -1)
}

/**
 * 使用指定的key将对象存入分部式缓存中，并指定过期时间，注意，存入的对象必须是可序列化的
 *
 * @param key     缓存唯一键
 * @param value   对应的值
 * @param expires 过期时间，单位秒
 * @return
 */
func (r SingleRedisProvider) SetExpires(key, value string, expires time.Duration) bool {
	err := r.client.Set(key, value, expires).Err()
	return err == nil
}

/**
 * 从缓存中删除指定key的缓存数据。
 *
 * @param key
 * @return
 */
func (r SingleRedisProvider) Delete(key string) {
	r.client.Del(key)
}

/**
 * 批量删除缓存中的key。
 *
 * @param keys
 */
func (r SingleRedisProvider) BatchDelete(keys ...string) {
	r.client.Del(keys...)
}

/**
 * 将指定key的map数据的某个字段设置为给定的值。
 *
 * @param key   map数据的键
 * @param field map的字段名称
 * @param value 要设置的字段值
 */
func (r SingleRedisProvider) HSet(key, field, value string) {
	r.client.HSet(key, field, value)
}

/**
 * 获取指定key的map数据某个字段的值，如果不存在则返回Null
 *
 * @param key   map数据的键
 * @param field map的字段名称
 * @return
 */
func (r SingleRedisProvider) HGet(key, field string) string {
	return r.client.HGet(key, field).Val()
}

/**
 * 获取指定key的map对象，如果不存在则返回Null
 *
 * @param key map数据的键
 * @return
 */
func (r SingleRedisProvider) HGetAll(key string) map[string]string {
	return r.client.HGetAll(key).Val()
}

/**
 * 将指定key的map数据中的某个字段删除。
 *
 * @param key   map数据的键
 * @param field map中的key名称
 */

func (r SingleRedisProvider) HDelete(key string, fields ...string) {
	r.client.HDel(key, fields...)
}

/**
 * 判断缓存中指定key的map是否存在指定的字段，如果key或字段不存在则返回false。
 *
 * @param key
 * @param field
 * @return
 */
func (r SingleRedisProvider) HExists(key, field string) bool {
	v, _ := r.client.HExists(key, field).Result()
	return v
}

/**
 * 对指定的key结果集执行指定的脚本并返回最终脚本执行的结果。
 *
 * @param script 脚本
 * @param key    要操作的缓存key
 * @param args   脚本的参数列表
 * @return
 */
func (r SingleRedisProvider) Val(script string, keys []string, args ...interface{}) {
	r.client.Eval(script, keys, args...)
}

/**
 * 通过直接调用缓存客户端进行缓存操作，该操作适用于高级操作，如果执行失败会返回Null。
 *
 * @param operator
 * @return
 */
func (r *SingleRedisProvider) Operate(cmd interface{}) error {
	_cmd := cmd.(*redis.Cmd)
	return r.client.Process(_cmd)
}

/**
 * 关闭客户端
 */
func (r SingleRedisProvider) Close() {
	_ = r.client.Close()
}
