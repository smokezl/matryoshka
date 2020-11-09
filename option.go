package matryoshka

import (
	"context"
	"errors"
)

const (
	moduleName = "matryoshka"

	pubSubMaxConn = 10
	loadMaxRetry  = 2

	defaultCacheSize      = 1024 * 1024 * 100
	defaultRedisTtlFactor = 20

	// redis default
	defaultMaxRetry     = 2
	defaultMaxIdle      = 50
	defaultMaxActive    = 500
	defaultIdleTimeout  = 7200
	defaultConnTimeout  = 100
	defaultWriteTimeout = 250
	defaultReadTimeout  = 250
)

type errHandler func(ctx context.Context, err error)

var (
	ErrSourceTimeout = errors.New(moduleName + " source timeout")
	ErrNotFound      = errors.New(moduleName + " source nil")
)

type mainOptions struct {
	cacheSize  int
	errHandler errHandler
	cacheI     DistributedCacheI
	pubSubI    PubSubChannelI
}

type MainOption struct {
	f func(*mainOptions)
}

func WithCacheSize(size int) MainOption {
	return MainOption{func(do *mainOptions) {
		do.cacheSize = size
	}}
}

func WithErrHandler(f errHandler) MainOption {
	return MainOption{func(do *mainOptions) {
		do.errHandler = f
	}}
}

func WithDistributedCache(i DistributedCacheI) MainOption {
	return MainOption{func(do *mainOptions) {
		do.cacheI = i
	}}
}

func WithPubSubChannel(i PubSubChannelI) MainOption {
	return MainOption{func(do *mainOptions) {
		do.pubSubI = i
	}}
}

// item option
type CacheType int

const (
	CacheTypeAll = 1 //同时开启内存和外部缓存
	CacheTypeMem = 2 //只开启内部缓存
	CacheTypeExt = 3 //只开启外部缓存
)

type itemOptions struct {
	cacheType      CacheType
	redisTtlFactor int
}

type ItemOption struct {
	f func(*itemOptions)
}

func WithCacheType(ct CacheType) ItemOption {
	return ItemOption{func(do *itemOptions) {
		do.cacheType = ct
	}}
}

func WithRedisTtlFactor(factor int) ItemOption {
	return ItemOption{func(do *itemOptions) {
		do.redisTtlFactor = factor
	}}
}

type DistributedCacheI interface {
	IsNilErr(err error) bool
	Get(key string) (val string, err error)
	MGet(keys []string) (valMap map[string]string, err error)
	SetEx(key string, val string, ttl int) (err error)
	Del(keys []string) (err error)
}

type PubSubChannelI interface {
	GetKey() string
	Subscribe(key string, receiver func(message string, err error))
	Publish(key string, val string) (err error)
}

type cacheHandlerI interface {
	getCacheTtl(ttl, factor int) int
	batchGetFromCache(ctx context.Context, keys []string) (valMap map[string]string)
	getFromCache(ctx context.Context, key string) (val string, err error)
	setFromCache(ctx context.Context, key, val string, ttl int)
	delFromCache(ctx context.Context, keys []string)
	addQpsTotal(n int)
	addQpsHit(n int)
}
