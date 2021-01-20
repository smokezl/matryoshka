package matryoshka

import (
	"context"
	"errors"
	"github.com/gomodule/redigo/redis"
	"math/rand"
	"time"
)

type RedisConfig struct {
	Addr         string `toml:"addr"`
	MaxRetry     int    `toml:"max_retry"`
	Pwd          string `toml:"pwd"`
	MaxIdle      int    `toml:"max_idle"`
	MaxActive    int    `toml:"max_active"`
	IdleTimeout  int    `toml:"idle_timeout"`  //s
	ConnTimeout  int    `toml:"conn_timeout"`  //ms
	WriteTimeout int    `toml:"write_timeout"` //ms
	ReadTimeout  int    `toml:"read_timeout"`  //ms
}

func (c *RedisConfig) init() {
	if c.MaxRetry == 0 {
		c.MaxRetry = defaultMaxRetry
	}
	if c.MaxIdle == 0 {
		c.MaxIdle = defaultMaxIdle
	}
	if c.MaxActive == 0 {
		c.MaxActive = defaultMaxActive
	}
	if c.IdleTimeout == 0 {
		c.IdleTimeout = defaultIdleTimeout
	}
	if c.ConnTimeout == 0 {
		c.ConnTimeout = defaultConnTimeout
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = defaultWriteTimeout
	}
	if c.ReadTimeout == 0 {
		c.ReadTimeout = defaultReadTimeout
	}
}

type redisHandler struct {
	sharing *sharing
}

func newRedisHandler(s *sharing) *redisHandler {
	h := &redisHandler{
		sharing: s,
	}
	return h
}

func (rh *redisHandler) getCacheTtl(ttl, factor int) int {
	return ttl * factor
}

func (rh *redisHandler) batchGetFromCache(ctx context.Context, keys []string) (valMap map[string]string) {
	fn := moduleName + ".batchGetFromCache"
	if rh.sharing.cacheI == nil {
		rh.sharing.dealErrHandler(ctx, errors.New(fn+":batch get from redis err,"+moduleName+" not init distributed cache"))
		return
	}
	lk := len(keys)
	if lk == 0 {
		return
	}
	valMap, err := rh.sharing.cacheI.MGet(keys)
	if err != nil {
		// 缓存遇到错误，记录日志
		rh.sharing.dealErrHandler(ctx, errors.New(fn+":batch get from redis err,"+err.Error()))
		return
	}
	return
}

// 上游需要根据 err 做判断，所以必须要返回
func (rh *redisHandler) getFromCache(ctx context.Context, key string) (val string, err error) {
	fn := moduleName + ".getFromCache"
	if rh.sharing.cacheI == nil {
		err = errors.New(moduleName + " not init distributed cache")
		// 缓存遇到错误，记录日志
		rh.sharing.dealErrHandler(ctx, errors.New(fn+":get from redis err,"+err.Error()))
		return
	}
	val, err = rh.sharing.cacheI.Get(key)
	if err == nil {
		return
	}
	if rh.sharing.cacheI.IsNilErr(err) {
		err = ErrNotFound
		return
	}
	// 缓存遇到错误，记录日志
	rh.sharing.dealErrHandler(ctx, errors.New(fn+":get from redis err,"+err.Error()))
	return
}

func (rh *redisHandler) setFromCache(ctx context.Context, key string, val string, ttl int) {
	fn := moduleName + ".setFromCache"
	if rh.sharing.cacheI == nil {
		rh.sharing.dealErrHandler(ctx, errors.New(fn+":set redis err,"+moduleName+" not init distributed cache"))
		return
	}
	redisTtlMin := ttl
	redisTtlN := redisTtlMin / 2
	rand.Seed(time.Now().UnixNano())
	redisTtl := rand.Intn(redisTtlN) + redisTtlMin
	err := rh.sharing.cacheI.SetEx(key, val, redisTtl)
	if err != nil {
		rh.sharing.dealErrHandler(ctx, errors.New(fn+":set redis err,"+err.Error()))
	}
	return
}

func (rh *redisHandler) delFromCache(ctx context.Context, keys []string) {
	fn := moduleName + ".delFromCache"
	if rh.sharing.cacheI == nil {
		rh.sharing.dealErrHandler(ctx, errors.New(fn+":delete redis err,"+moduleName+" not init distributed cache"))
		return
	}

	err := rh.sharing.cacheI.Del(keys)
	if err != nil {
		rh.sharing.dealErrHandler(ctx, errors.New(fn+":delete redis err,"+err.Error()))
	}
}

func (rh *redisHandler) addQpsTotal(n int) {
	rh.sharing.qps.addRedisTotal(n)
}

func (rh *redisHandler) addQpsHit(n int) {
	rh.sharing.qps.addRedisHit(n)
}

/****************************
 * 默认分布式缓存和pub-sub通道
 ****************************/

type defaultCache struct {
	c    *RedisConfig
	pool *redis.Pool
}

func NewDefaultCache(c *RedisConfig) DistributedCacheI {
	c.init()
	return &defaultCache{
		c: c,
		pool: &redis.Pool{
			MaxIdle:     c.MaxIdle,
			MaxActive:   c.MaxActive,
			IdleTimeout: time.Duration(c.IdleTimeout) * time.Second,
			Dial: func() (conn redis.Conn, e error) {
				return redis.Dial("tcp", c.Addr,
					redis.DialPassword(c.Pwd),
					redis.DialConnectTimeout(time.Duration(c.ConnTimeout)*time.Millisecond),
					redis.DialReadTimeout(time.Duration(c.ReadTimeout)*time.Millisecond),
					redis.DialWriteTimeout(time.Duration(c.WriteTimeout)*time.Millisecond),
				)
			},
			TestOnBorrow: func(c redis.Conn, t time.Time) error {
				_, err := c.Do("PING")
				return err
			},
		},
	}
}

func (d *defaultCache) IsNilErr(err error) bool {
	return err == redis.ErrNil
}

func (d *defaultCache) Get(key string) (val string, err error) {
	for i := 0; i < d.c.MaxRetry; i++ {
		err = redisDo(d.pool, func(conn redis.Conn) error {
			val, err = redis.String(conn.Do("GET", key))
			return err
		})
		if err == nil {
			return
		}
		if d.IsNilErr(err) {
			return
		}
	}
	return
}

func (d *defaultCache) MGet(keys []string) (valMap map[string]string, err error) {
	var args []interface{}
	valMap = make(map[string]string, len(keys))
	for _, key := range keys {
		args = append(args, key)
	}
	for i := 0; i < d.c.MaxRetry; i++ {
		var vals []string
		err = redisDo(d.pool, func(conn redis.Conn) error {
			vals, err = redis.Strings(conn.Do("MGET", args...))
			return err
		})
		if err != nil {
			continue
		}
		lv := len(vals)
		for i := 0; i < lv; i++ {
			valMap[keys[i]] = vals[i]
		}
		break
	}
	return
}

func (d *defaultCache) SetEx(key string, val string, ttl int) (err error) {
	for i := 0; i < d.c.MaxRetry; i++ {
		err = redisDo(d.pool, func(conn redis.Conn) error {
			_, err = redis.String(conn.Do("SETEX", key, ttl, val))
			return err
		})
		if err == nil {
			return
		}
	}
	return
}

func (d *defaultCache) Del(keys []string) (err error) {
	args := make([]interface{}, 0, len(keys))
	for _, key := range keys {
		args = append(args, key)
	}
	for i := 0; i < d.c.MaxRetry; i++ {
		err = redisDo(d.pool, func(conn redis.Conn) error {
			_, err := redis.Int(conn.Do("DEL", args...))
			return err
		})
		if err == nil {
			return
		}
	}
	return
}

type defaultPubSub struct {
	c    *RedisConfig
	key  string
	pool *redis.Pool
}

func NewDefaultPubSub(key string, c *RedisConfig) PubSubChannelI {
	c.init()
	return &defaultPubSub{
		c:   c,
		key: key,
		pool: &redis.Pool{
			MaxIdle:     pubSubMaxConn,
			MaxActive:   pubSubMaxConn,
			IdleTimeout: time.Duration(c.IdleTimeout) * time.Second,
			Dial: func() (conn redis.Conn, e error) {
				return redis.Dial("tcp", c.Addr,
					redis.DialPassword(c.Pwd),
					redis.DialConnectTimeout(time.Duration(c.ConnTimeout)*time.Millisecond),
					redis.DialWriteTimeout(time.Duration(c.WriteTimeout)*time.Millisecond),
				)
			},
			TestOnBorrow: func(c redis.Conn, t time.Time) error {
				_, err := c.Do("PING")
				return err
			},
		},
	}
}

func (d *defaultPubSub) GetKey() string {
	return d.key
}

func (d *defaultPubSub) Subscribe(key string, receiver func(message string, err error)) {
	fn := moduleName + ".subscribe"
	itemFn := func() {
		conn := d.pool.Get()
		defer conn.Close()
		psc := redis.PubSubConn{Conn: conn}
		if err := psc.Subscribe(key); err != nil {
			err = errors.New(fn + ":redis subscribe err," + err.Error())
			receiver("", err)
			return
		}
		for conn.Err() == nil {
			switch v := psc.Receive().(type) {
			case redis.Message:
				receiver(string(v.Data), nil)
			case error:
				receiver("", v)
				return
			}
		}
		connErr := conn.Err()
		if connErr != nil {
			receiver("", connErr)
		}
	}
	for {
		// 断线重连
		itemFn()
		time.Sleep(time.Millisecond * 5)
	}
}

func (d *defaultPubSub) Publish(key string, val string) (err error) {
	for i := 0; i < d.c.MaxRetry; i++ {
		err = redisDo(d.pool, func(conn redis.Conn) error {
			_, err := redis.Int(conn.Do("PUBLISH", key, val))
			return err
		})
		if err == nil {
			return
		}
	}
	return
}

func redisDo(pool *redis.Pool, fn func(conn redis.Conn) error) (err error) {
	conn := pool.Get()
	defer conn.Close()
	if err = conn.Err(); err != nil {
		return
	}
	err = fn(conn)
	return
}
