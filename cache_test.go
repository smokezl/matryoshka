package matryoshka

import (
	"context"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

const (
	noCacheNoPubSub       = "noCacheNoPubSub"
	cacheNoPubSub         = "cacheNoPubSub"
	cachePubSub           = "cachePubSub"
	extCacheDefaultPubSub = "extCacheDefaultPubSub"
)

type TestM struct {
	key string
	val *Matryoshka
}

func initTest() []*TestM {
	return []*TestM{
		{
			key: noCacheNoPubSub,
			val: Init(
				WithErrHandler(func(ctx context.Context, err error) {
					fmt.Println("test print,", err)
				}),
			),
		},
		{
			key: cacheNoPubSub,
			val: Init(
				WithErrHandler(func(ctx context.Context, err error) {
					fmt.Println("test print,", err)
				}),
				WithDistributedCache(NewDefaultCache(conf)),
			),
		},
		{
			key: cachePubSub,
			val: Init(
				WithErrHandler(func(ctx context.Context, err error) {
					fmt.Println("test print,", err)
				}),
				WithDistributedCache(NewDefaultCache(conf)),
				WithPubSubChannel(NewDefaultPubSub("sub_key", conf)),
			),
		},
		{
			key: extCacheDefaultPubSub,
			val: Init(
				WithErrHandler(func(ctx context.Context, err error) {
					fmt.Println("test print,", err)
				}),
				WithDistributedCache(NewExternalCache(conf)),
				WithPubSubChannel(NewDefaultPubSub("sub_key", conf)),
			),
		},
	}
}

var conf = &RedisConfig{
	Addr:        "127.0.0.1:6379",
	MaxRetry:    2,
	Pwd:         "",
	IdleTimeout: 10,
	ConnTimeout: 100,
	MaxIdle:     50,
	MaxActive:   500,
}

type externalCache struct {
	rp *redis.Pool
}

func NewExternalCache(conf *RedisConfig) *externalCache {
	return &externalCache{
		rp: &redis.Pool{
			MaxIdle:     pubSubMaxConn,
			MaxActive:   pubSubMaxConn,
			IdleTimeout: time.Duration(conf.IdleTimeout) * time.Second,
			Dial: func() (conn redis.Conn, e error) {
				return redis.Dial("tcp", conf.Addr,
					redis.DialPassword(conf.Pwd),
					redis.DialConnectTimeout(time.Duration(conf.ConnTimeout)*time.Millisecond),
				)
			},
		},
	}
}

func (e *externalCache) IsNilErr(err error) bool {
	return err == redis.ErrNil
}

func (e *externalCache) Get(key string) (val string, err error) {
	conn := e.rp.Get()
	defer conn.Close()
	val, err = redis.String(conn.Do("GET", key))
	return
}

func (e *externalCache) MGet(keys []string) (valMap map[string]string, err error) {
	conn := e.rp.Get()
	defer conn.Close()

	var args []interface{}
	for _, key := range keys {
		args = append(args, key)
	}
	vals, err := redis.Strings(conn.Do("MGET", args...))
	if err != nil {
		return nil, err
	}
	lv := len(vals)
	valMap = make(map[string]string, lv)
	for i := 0; i < lv; i++ {
		valMap[keys[i]] = vals[i]
	}
	return
}

func (e *externalCache) SetEx(key string, val string, ttl int) (err error) {
	conn := e.rp.Get()
	defer conn.Close()
	_, err = redis.String(conn.Do("SETEX", key, ttl, val))
	return
}

func (e *externalCache) Del(keys []string) error {
	conn := e.rp.Get()
	defer conn.Close()
	args := make([]interface{}, 0, len(keys))
	for _, key := range keys {
		args = append(args, key)
	}
	_, err := redis.Int(conn.Do("DEL", args...))
	return err
}

func TestSetGetCache(t *testing.T) {
	mList := initTest()
	ctx := context.Background()
	dv := "abcd"
	for i, item := range mList {
		key := fmt.Sprintf("%d_%s", i, "test_key")
		m := item.val
		h := m.NewCache("info", 20)
		genKey := h.genKey("", key)
		info, err := m.mh.getFromCache(ctx, genKey)
		if err != ErrNotFound {
			t.Error("get cache empty err, mem err,", key, info, err)
		}
		infoRedis, err := m.rh.getFromCache(ctx, genKey)
		if err != nil {
			switch item.key {
			case noCacheNoPubSub:
				if err.Error() != "matryoshka not init distributed cache" {
					t.Error("get cache empty err, redis err,", key, infoRedis, err)
				}
			case cacheNoPubSub, cachePubSub, extCacheDefaultPubSub:
				if err != ErrNotFound {
					t.Error("get cache empty err, redis err,", key, infoRedis, err)
				}
			default:
				t.Error("redis support err")
			}
		}
		val, err := h.Get(ctx, "", key, func(ctx context.Context, key string) (string, error) {
			time.Sleep(time.Millisecond * 200)
			return dv, nil
		})
		if err != nil {
			t.Error("get cache err, get err,", key, val, err)
		}
		if val != dv {
			t.Error("get cache err, get not compare,", key, val, err)
		}
		info, err = m.mh.getFromCache(ctx, genKey)
		if err != nil {
			t.Error("get cache err, mem err,", key, string(info), err)
		}
		if info != dv {
			t.Error("get cache err, get mem not compare,", key, string(info), err)
		}
		infoRedis, err = m.rh.getFromCache(ctx, genKey)
		if err != nil {
			switch item.key {
			case noCacheNoPubSub:
				if err.Error() != "matryoshka not init distributed cache" {
					t.Error("get cache err, redis err,", key, infoRedis, err)
				}
			case cacheNoPubSub, cachePubSub, extCacheDefaultPubSub:
				t.Error("get cache err, redis err,", key, infoRedis, err)
			default:
				t.Error("redis support err")
			}
		} else if infoRedis != dv {
			t.Error("get cache err, get redis not compare,", key, infoRedis, err)
		}
		time.Sleep(time.Millisecond * 50)
		h.Del(ctx, "", []string{key})
		info, err = m.mh.getFromCache(ctx, genKey)
		if err != ErrNotFound {
			t.Error("get cache err, mem err,", key, string(info), err)
		}
		infoRedis, err = m.rh.getFromCache(ctx, genKey)
		if err != nil {
			switch item.key {
			case noCacheNoPubSub:
				if err.Error() != "matryoshka not init distributed cache" {
					t.Error("get cache empty err, redis err,", key, infoRedis, err)
				}
			case cacheNoPubSub, cachePubSub, extCacheDefaultPubSub:
				if err != ErrNotFound {
					t.Error("get cache empty err, redis err,", key, infoRedis, err)
				}
			default:
				t.Error("redis support err")
			}
		}
	}
}

func TestSetGetCacheMem(t *testing.T) {
	mList := initTest()
	ctx := context.Background()
	dv := "abcd"
	redisErrFn := func(sceneKey, redisKey, redisVal string, err error) {
		switch sceneKey {
		case noCacheNoPubSub:
			if err.Error() != "matryoshka not init distributed cache" {
				t.Error("get cache empty err, redis err,", redisKey, redisVal, err)
			}
		case cacheNoPubSub, cachePubSub, extCacheDefaultPubSub:
			if err != ErrNotFound {
				t.Error("get cache empty err, redis err,", redisKey, redisVal, err)
			}
		default:
			t.Error("redis support err")
		}
	}

	for i, item := range mList {
		key := fmt.Sprintf("%d_%s", i, "test_key")
		m := item.val
		h := m.NewCache("info", 20, WithCacheType(CacheTypeMem))
		genKey := h.genKey("", key)
		info, err := m.mh.getFromCache(ctx, genKey)
		if err != ErrNotFound {
			t.Error("get cache empty err, mem err,", key, info, err)
		}
		infoRedis, err := m.rh.getFromCache(ctx, genKey)
		if err != nil {
			redisErrFn(item.key, key, infoRedis, err)
		}
		val, err := h.Get(ctx, "", key, func(ctx context.Context, key string) (string, error) {
			time.Sleep(time.Millisecond * 200)
			return dv, nil
		})
		if err != nil {
			t.Error("get cache err, get err,", key, val, err)
		}
		if val != dv {
			t.Error("get cache err, get not compare,", key, val, err)
		}
		info, err = m.mh.getFromCache(ctx, genKey)
		if err != nil {
			t.Error("get cache err, mem err,", key, string(info), err)
		}
		if info != dv {
			t.Error("get cache err, get mem not compare,", key, string(info), err)
		}
		infoRedis, err = m.rh.getFromCache(ctx, genKey)
		if err != nil {
			redisErrFn(item.key, key, infoRedis, err)
		}
		time.Sleep(time.Millisecond * 50)
		h.Del(ctx, "", []string{key})
		info, err = m.mh.getFromCache(ctx, genKey)
		if err != ErrNotFound {
			t.Error("get cache err, mem err,", key, string(info), err)
		}
		infoRedis, err = m.rh.getFromCache(ctx, genKey)
		if err != nil {
			redisErrFn(item.key, key, infoRedis, err)
		}
	}
}

func TestSetGetCacheMem2(t *testing.T) {
	mList := initTest()
	ctx := context.Background()
	dv := "abcd"
	for i, item := range mList {
		key := fmt.Sprintf("%d_%s", i, "test_key")
		m := item.val
		h := m.NewCache("info", 20, WithCacheType(CacheTypeMem))
		genKey := h.genKey("", key)
		info, err := m.mh.getFromCache(ctx, genKey)
		if err != ErrNotFound {
			t.Error("get cache empty err, mem err,", key, info, err)
		}
		val, err := h.Get(ctx, "", key, func(ctx context.Context, key string) (string, error) {
			time.Sleep(time.Millisecond * 200)
			return dv, nil
		})
		if err != nil {
			t.Error("get cache err, get err,", key, val, err)
		}
		if val != dv {
			t.Error("get cache err, get not compare,", key, val, err)
		}
		info, err = m.mh.getFromCache(ctx, genKey)
		if err != nil {
			t.Error("get cache err, mem err,", key, string(info), err)
		}
		if info != dv {
			t.Error("get cache err, get mem not compare,", key, string(info), err)
		}
		time.Sleep(time.Millisecond * 50)
		h.Del(ctx, "", []string{key})
		info, err = m.mh.getFromCache(ctx, genKey)
		if err != ErrNotFound {
			t.Error("get cache err, mem err,", key, string(info), err)
		}
	}
}

func TestGetCacheEmpty(t *testing.T) {
	mList := initTest()
	ctx := context.Background()
	for i, item := range mList {
		key := fmt.Sprintf("%d_%s", i, "aaaaaaaa")
		m := item.val
		info, err := m.mh.getFromCache(ctx, key)
		if err != ErrNotFound {
			t.Error("get cache empty err, mem err,", key, info, err)
		}
		infoRedis, err := m.rh.getFromCache(ctx, key)
		if err != nil {
			switch item.key {
			case noCacheNoPubSub:
				if err.Error() != "matryoshka not init distributed cache" {
					t.Error("get cache empty err, redis err,", key, infoRedis, err)
				}
			case cacheNoPubSub, cachePubSub, extCacheDefaultPubSub:
				if err != ErrNotFound {
					t.Error("get cache empty err, redis err,", key, infoRedis, err)
				}
			default:
				t.Error("redis support err")
			}
		}
		keys := []string{"aaaa", "bbb", "cccc"}
		infos := m.mh.batchGetFromCache(ctx, keys)
		for _, item := range infos {
			if item != "" {
				t.Error("batch get cache empty err, mem err,", keys, item, err)
			}
		}
		infos = m.rh.batchGetFromCache(ctx, keys)
		for _, item := range infos {
			if item != "" {
				t.Error("batch get cache empty err, redis err,", keys, item, err)
			}
		}
	}
}

func TestBatchGetCache(t *testing.T) {
	mList := initTest()
	ctx := context.Background()
	for i, item := range mList {
		tk := fmt.Sprintf("%d_%s", i, "test_key")
		tk1 := fmt.Sprintf("%d_%s", i, "test_key1")
		tk2 := fmt.Sprintf("%d_%s", i, "test_key2")
		tk3 := fmt.Sprintf("%d_%s", i, "test_key3")
		allKeys := []string{tk, tk1, tk2, tk3}
		m := item.val
		h := m.NewCache("info", 20)
		notEmptyKeys := map[string]string{
			h.genKey("", tk): tk,
		}
		vals, err := h.BatchGet(ctx, "", allKeys, func(ctx context.Context, keys []string) (map[string]string, error) {
			time.Sleep(time.Millisecond * 200)
			valMap := make(map[string]string)
			for _, key := range keys {
				if nek, ok := notEmptyKeys[h.genKey("", key)]; ok {
					valMap[key] = nek
				}
			}
			return valMap, nil
		})
		if err != nil {
			t.Error("batch cache empty err, batch err,", err)
		}
		if len(vals) != len(notEmptyKeys) {
			t.Error("batch cache empty err, vals len != emptyKeys, vals", vals, ",not empty keys", notEmptyKeys)
		}
		var genKeys []string
		for _, key := range allKeys {
			genKeys = append(genKeys, h.genKey("", key))
		}
		valMap := m.mh.batchGetFromCache(ctx, genKeys)
		for key, item := range valMap {
			if item == "" {
				if _, ok := notEmptyKeys[key]; ok {
					t.Error("batch cache empty err, not empty mem info empty,", key, item)
				}
			} else {
				if _, ok := notEmptyKeys[key]; !ok {
					t.Error("batch cache empty err, empty mem info not empty,", key, item)
				}
			}
		}
		valMap = m.rh.batchGetFromCache(ctx, genKeys)
		if err != nil {
			t.Error("batch cache err, redis err,", allKeys, vals, err)
		}
		for key, item := range valMap {
			if item == "" {
				if _, ok := notEmptyKeys[key]; ok {
					t.Error("batch cache empty err, not empty redis info empty,", key, item)
				}
			} else {
				if _, ok := notEmptyKeys[key]; !ok {
					t.Error("batch cache empty err, empty redis info not empty,", key, item)
				}
			}
		}
		h.Del(ctx, "", allKeys)
		time.Sleep(time.Second * 1)
	}
}

func TestBatchDel(t *testing.T) {
	mList := initTest()
	ctx := context.Background()
	for i, item := range mList {
		m := item.val
		if m.sharing.pubSubI != nil {
			m.sharing.pubSubI = (NewDefaultPubSub(fmt.Sprintf("%d_%s", i, "sub_key"), conf))
			go m.sharing.pubSubI.Subscribe(m.sharing.pubSubI.GetKey(), func(message string, err error) {
				keys := m.sharing.decodePubKey(message)
				t.Log(m.sharing.pubSubI.GetKey(), keys)
			})
		}
		tk := fmt.Sprintf("%d_%s", i, "test_key")
		tk1 := fmt.Sprintf("%d_%s", i, "test_key1")
		tk2 := fmt.Sprintf("%d_%s", i, "test_key2")
		tk3 := fmt.Sprintf("%d_%s", i, "test_key3")
		allKeys := []string{tk, tk1, tk2, tk3}
		h := m.NewCache("info", 20)
		vals, err := h.BatchGet(ctx, "", allKeys, func(ctx context.Context, keys []string) (map[string]string, error) {
			time.Sleep(time.Millisecond * 200)
			valMap := make(map[string]string)
			for _, key := range keys {
				valMap[key] = key
			}
			return valMap, nil
		})
		if err != nil {
			t.Error("batch delete err,", err)
		}
		var genKeys []string
		for _, key := range allKeys {
			genKeys = append(genKeys, h.genKey("", key))
		}
		valMap := m.mh.batchGetFromCache(ctx, genKeys)
		for key, item := range valMap {
			if item == "" {
				t.Error("batch delete, cache empty err, not empty mem info empty,", key, item)
			}
		}
		valMap = m.rh.batchGetFromCache(ctx, genKeys)
		if err != nil {
			t.Error("batch cache err, redis err,", allKeys, vals, err)
		}
		for key, item := range valMap {
			if item == "" {
				t.Error("batch delete, cache empty err, not empty redis info empty,", key, item)
			}
		}
		h.Del(ctx, "", allKeys)
		valMap = m.mh.batchGetFromCache(ctx, genKeys)
		for key, item := range valMap {
			if item != "" {
				t.Error("batch delete, cache empty err, not empty mem info empty,", key, item)
			}
		}
		valMap = m.rh.batchGetFromCache(ctx, genKeys)
		if err != nil {
			t.Error("batch cache err, redis err,", allKeys, vals, err)
		}
		for key, item := range valMap {
			if item != "" {
				t.Error("batch delete, cache empty err, not empty redis info empty,", key, item)
			}
		}
	}
	time.Sleep(time.Second * 2)
}

func TestGetCachePanic(t *testing.T) {
	mList := initTest()
	ctx := context.Background()
	dv := "abcd"
	for i, item := range mList {
		m := item.val
		h := m.NewCache("info", 20)
		key := fmt.Sprintf("%d_%s", i, "test_key")
		val, err := h.Get(ctx, "", key, func(ctx context.Context, key string) (string, error) {
			panic(21212)
			return dv, nil
		})
		if err == nil {
			t.Error("get cache panic,", key, val, err)
		}
	}
	time.Sleep(time.Second * 2)
}

func TestCacheTtl(t *testing.T) {
	mList := initTest()
	baseTtl := 20
	optionTtl := 30
	m := mList[3].val
	h1 := m.NewCache("info", baseTtl)
	memTtl1 := m.mh.getCacheTtl(h1.ttl, h1.redisTtlFactor)
	redisTtl1 := m.rh.getCacheTtl(h1.ttl, h1.redisTtlFactor)
	if memTtl1 != baseTtl {
		t.Error("memTtl1 err", baseTtl, memTtl1)
	}
	if redisTtl1 != baseTtl*defaultRedisTtlFactor {
		t.Error("redisTtl1 err", baseTtl*defaultRedisTtlFactor, redisTtl1)
	}
	h2 := m.NewCache("info", baseTtl, WithRedisTtlFactor(optionTtl))
	memTtl2 := m.mh.getCacheTtl(h2.ttl, h2.redisTtlFactor)
	redisTtl2 := m.rh.getCacheTtl(h2.ttl, h2.redisTtlFactor)
	if memTtl2 != baseTtl {
		t.Error("memTtl1 err", baseTtl, memTtl2)
	}
	if redisTtl2 != baseTtl*optionTtl {
		t.Error("redisTtl1 err", baseTtl*optionTtl, redisTtl2)
	}
}

func TestPubSubRetry(t *testing.T) {
	mList := initTest()
	m := mList[3].val
	m.sharing.pubSubI = &defaultPubSub{
		c:   conf,
		key: "kkkkkk",
		pool: &redis.Pool{
			MaxIdle:     pubSubMaxConn,
			MaxActive:   pubSubMaxConn,
			IdleTimeout: time.Duration(conf.IdleTimeout) * time.Second,
			Dial: func() (conn redis.Conn, e error) {
				return redis.Dial("tcp", conf.Addr,
					redis.DialPassword(conf.Pwd),
					redis.DialConnectTimeout(time.Duration(conf.ConnTimeout)*time.Millisecond),
					redis.DialReadTimeout(time.Duration(1)*time.Second),
					redis.DialWriteTimeout(time.Duration(conf.WriteTimeout)*time.Millisecond),
				)
			},
			TestOnBorrow: func(c redis.Conn, t time.Time) error {
				_, err := c.Do("PING")
				return err
			},
		},
	}
	go m.sharing.pubSubI.Subscribe(m.sharing.pubSubI.GetKey(), func(message string, err error) {
		fmt.Println(message, err)
	})
	time.Sleep(time.Second * 3)
}

func TestPubSubGet(t *testing.T) {
	mList := initTest()
	var msgNum int32 = 0
	maxNum := 3
	m := mList[3].val
	for i := 0; i < maxNum; i++ {
		go m.sharing.pubSubI.Subscribe(m.sharing.pubSubI.GetKey(), func(message string, err error) {
			atomic.AddInt32(&msgNum, 1)
		})
	}
	time.Sleep(time.Millisecond * 100)
	m.sharing.pubSubI.Publish(m.sharing.pubSubI.GetKey(), "aaaaa")
	time.Sleep(time.Second * 1)
	if msgNum != int32(maxNum) {
		t.Error("subscribe num not compare, msgNum=", msgNum, maxNum)
	}
}

func TestCacheGetAllQps(t *testing.T) {
	mList := initTest()
	m := mList[3].val
	runtime.GOMAXPROCS(runtime.NumCPU())
	var qps int64
	var dbQps int64
	ctx := context.Background()
	h := m.NewCache("info", 1)
	go func() {
		for {
			time.Sleep(time.Second)
			v := atomic.SwapInt64(&qps, 0)
			dbV := atomic.SwapInt64(&dbQps, 0)
			fmt.Println("qps:", v, dbV, m.GetStatistics())
		}
	}()
	max := 40
	for j := 0; j < runtime.NumCPU(); j++ {
		var i = 0
		go func(num int) {
			for {
				i++
				toKey := num*1000000 + (i % max)
				key := strconv.Itoa(toKey)
				_, err := h.Get(ctx, "", key, func(ctx context.Context, key string) (string, error) {
					atomic.AddInt64(&dbQps, 1)
					time.Sleep(time.Millisecond * 100)
					return key + "aaaaa", nil
				})
				if err != nil {
					fmt.Println(err)
				}
				atomic.AddInt64(&qps, 1)
			}
		}(j)
	}
	time.Sleep(time.Second * 70)
}

/**
* redis 写 + memery 写
* qps: 5902
* qps: 4757
* qps: 5492
* qps: 6904
* redis 读 + memery 写
* qps: 10751
* qps: 10220
* qps: 9583
 */
func TestCacheGetQps(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	var qps int64
	var dbQps int64
	mList := initTest()
	m := mList[3].val
	ctx := context.Background()
	h := m.NewCache("info", 10)
	go func() {
		for {
			time.Sleep(time.Second)
			v := atomic.SwapInt64(&qps, 0)
			fmt.Println("qps:", v)
		}
	}()
	for j := 0; j < runtime.NumCPU(); j++ {
		var i int64 = 0
		go func(num int) {
			for {
				i++
				toKey := num*1000000 + int(i)

				key := strconv.Itoa(toKey)
				_, err := h.Get(ctx, "", key, func(ctx context.Context, key string) (string, error) {
					atomic.AddInt64(&dbQps, 1)
					return key + "aaaaa", nil
				})
				if err != nil {
					fmt.Println(err)
				}
				atomic.AddInt64(&qps, 1)
			}
		}(j)
	}
	time.Sleep(time.Second * 5)
	fmt.Println("===>", dbQps)
}

//qps: 8642068
//qps: 9684711
//qps: 8477043
//qps: 8511494
//qps: 8024259
func TestMemeryGetQps(t *testing.T) {
	ctx := context.Background()
	runtime.GOMAXPROCS(runtime.NumCPU())
	var qps int64
	mList := initTest()
	m := mList[3].val
	go func() {
		for {
			time.Sleep(time.Second)
			v := atomic.SwapInt64(&qps, 0)
			fmt.Println("qps:", v)
		}
	}()
	for j := 0; j < runtime.NumCPU(); j++ {
		var i int64 = 0
		go func(num int) {
			for {
				i++
				toKey := num*1000000 + int(i)

				key := strconv.Itoa(toKey)
				_, _ = m.mh.getFromCache(ctx, key)
				atomic.AddInt64(&qps, 1)
			}
		}(j)
	}
	time.Sleep(time.Second * 10)
}

//qps: 1352668
//qps: 1310047
//qps: 1106321
//qps: 1062710
//qps: 1854091
func TestMemerySetQps(t *testing.T) {
	ctx := context.Background()
	runtime.GOMAXPROCS(runtime.NumCPU())
	var qps int64
	mList := initTest()
	m := mList[3].val
	go func() {
		for {
			time.Sleep(time.Second)
			v := atomic.SwapInt64(&qps, 0)
			fmt.Println("qps:", v)
		}
	}()
	for j := 0; j < runtime.NumCPU(); j++ {
		var i int64 = 0
		go func(num int) {
			for {
				i++
				toKey := num*1000000 + int(i)
				key := strconv.Itoa(toKey)
				m.mh.setFromCache(ctx, key, key, 10)
				atomic.AddInt64(&qps, 1)
			}
		}(j)
	}
	time.Sleep(time.Second * 10)
}
