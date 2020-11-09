golang多级缓存matryoshka
===========
### 支持分布式环境，基于redis和机器内存(memory)的多级缓存。
* 一级缓存使用 [freecache](https://github.com/coocood/freecache/)作为本地缓存，当数据在本地缓存中不存在时，会向第二级缓存请求数据。
* 二级缓存默认使用redis作为分布式缓存，当数据在二级缓存中不存在时，会向资源层请求数据。
* 当资源层某条数据更新，可以将缓存中对应的数据删除，二级分布式缓存会直接删除，一级内存缓存会默认利用redis的 sub/pub 机制，将所有机器下的数据删除。

### 功能
* 支持每级缓存的请求qps、命中qps、请求资源层qps的监控统计
* 支持只使用一级或二级缓存
* 支持自定义二级缓存和sub/pub的实现方式，只需要实现对应接口 WithDistributedCache、WithPubSubChannel
* 防缓存击穿，单机版的互斥锁，当从资源层同时加载同一数据时，每台机器最多只有一次请求会落到资源层，其他请求会等到数据同步到缓存后，直接从缓存中获取数据。
* 支持自定义错误处理，WithErrHandler

### 安装
go get github.com/smokezl/matryoshka

### 导入
import "github.com/smokezl/matryoshka"

### 基本使用方式
#### 初始化
 ```go
// 1、初始化全局缓存
conf := &matryoshka.RedisConfig{
    Addr:        "127.0.0.1:6379",
    MaxRetry:    2,
    Pwd:         "",
    IdleTimeout: 10,
    ConnTimeout: 100,
    MaxIdle:     50,
    MaxActive:   500,
}
cache := matryoshka.Init(
    matryoshka.WithErrHandler(func(ctx context.Context, err error) {
        fmt.Println("test print,", err)
    }),
    matryoshka.WithDistributedCache(matryoshka.NewDefaultCache(conf)),
    matryoshka.WithPubSubChannel(matryoshka.NewDefaultPubSub("sub_key", conf)),
)

// 2、根据不同的使用场景， 创建独立场景的缓存
scene := "productInfo"
// 内存 cache 超时时间(redis超时是内存的n倍，默认20倍，可以通过 WithRedisTtlFactor 设置倍数)
expire := 20
soureLoadFn:= func(ctx context.Context, "cachePre", key string) (string, error) {
    return "product info", nil
}
batchSoureLoadFn = func(ctx context.Context, "cachePre", keys []string) (map[string]string, error) {
    valMap := make(map[string]string)
    for _, key := range keys{
        valMap[key] = "product info"
    }
    return valMap, nil
}

h := cache.NewCache(scene, expire)

// 3、获取单个数据
val, err := h.Get(ctx, "key",soureLoadFn)
if err == matryoshka.ErrNotFound {
    //deal empty
}
if err != nil {
    //deal err
}

// 4、获取批量数据
valMap, err := h.BatchGet(ctx, []string{"key1","key2"},batchSoureLoadFn)
if err != nil {
    //deal err
}
if len(valMap) == 0 {
	//deal empty
}
```

#### 初始化全局缓存配置函数
##### 1、WithCacheSize(size int) 

设置全局缓存内存大小，单位 byte，默认为 1024 * 1024 * 100(100m)

##### 2、WithErrHandler(f errHandler) 

设置错误执行函数，默认为 nil
```go
type errHandler func(ctx context.Context, err error)
```

##### 3、WithDistributedCache(i DistributedCacheI) 

设置从redis缓存io数据的接口，如果不设置，redis缓存将无法使用，内置 NewDefaultCache

```go
type DistributedCacheI interface {
	IsNilErr(err error) bool
	Get(key string) (val string, err error)
	MGet(keys []string) (valMap map[string]string, err error)
	SetEx(key string, val string, ttl int) (err error)
	Del(keys []string) (err error)
}
```

##### 4、WithPubSubChannel(i PubSubChannelI) 

设置订阅发布的接口，如果不设置，内存缓存分布式同步功能将无法使用，内置 NewDefaultPubSub

```
type PubSubChannelI interface {
	GetKey() string
	Subscribe(key string, receiver func(message string, err error))
	Publish(key string, val string) (err error)
}
```

##### 5、使用
```go
conf := &matryoshka.RedisConfig{
    Addr:        "127.0.0.1:6379",
    MaxRetry:    2,
    Pwd:         "",
    IdleTimeout: 10,
    ConnTimeout: 100,
    MaxIdle:     50,
    MaxActive:   500,
    WriteTimeout:100, 
	ReadTimeout: 100,
}
type externalCache struct {
	rp *redis.Pool
}

func NewExternalCache() *externalCache {
	return &externalCache{
		rp: &redis.Pool{
			MaxIdle:     pubSubMaxConn,
			MaxActive:   pubSubMaxConn,
			IdleTimeout: time.Duration(idleTimeout) * time.Second,
			Dial: func() (conn redis.Conn, e error) {
				return redis.Dial("tcp", conf.Addr,
					redis.DialPassword(pwd),
					redis.DialConnectTimeout(time.Duration(connTimeout)*time.Millisecond),
                    redis.DialReadTimeout(time.Duration(c.ReadTimeout)*time.Millisecond),
                    redis.DialWriteTimeout(time.Duration(c.WriteTimeout)*time.Millisecond),
				)
			},
		},
	}
}

func (e *externalCache) IsNilErr(err error) bool {
	return err == redis.ErrNil
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

func (e *externalCache) Get(key string) (val string, err error) {
	conn := e.rp.Get()
	defer conn.Close()
	val, err = redis.String(conn.Do("GET", key))
	return
}

func (e *externalCache) SetEx(key string, val string, ttl int) (err error) {
	conn := e.rp.Get()
	defer conn.Close()
	_, err = redis.String(conn.Do("SETEX", key, ttl, val))
	return
}

func (e *externalCache) Del(key string) error {
	conn := e.rp.Get()
	defer conn.Close()
	_, err := redis.Int(conn.Do("DEL", key))
	return err
}


cache := matryoshka.Init(conf,
	matryoshka.WithCacheSize(1024*1024*1024),
	matryoshka.WithErrHandler(func(ctx context.Context, err error){
		//deal err
	}),
    //内置
    //matryoshka.WithDistributedCache(matryoshka.NewDefaultCache(conf)),

    matryoshka.WithDistributedCache(NewExternalCache(conf)),

    //内置
    matryoshka.WithPubSubChannel(matryoshka.NewDefaultPubSub("sub_key", conf)),
)

```

#### 创建独立场景缓存配置函数

##### 1、WithCacheType(ct CacheType)
设置开启的缓存，默认同时开启内存和redis缓存
```go
CacheTypeAll = 1 //同时开启内存和redis缓存
CacheTypeMem = 2 //只开启内部缓存
CacheTypeExt = 3 //只开启redis缓存
```

##### 2、WithRedisTtlFactor(factor int) 
设置redis缓存超时时间，默认为20倍expire时间
redis超时时间计算公式为: redisTtl = expire * factor

##### 2、使用
```go
h := cache.NewCache(scene, expire,
    matryoshka.WithCacheType(matryoshka.CacheTypeAll),
    matryoshka.WithRedisTtlFactor(30)
)
```