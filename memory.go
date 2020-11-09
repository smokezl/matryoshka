package matryoshka

import (
	"context"
	"errors"
	"fmt"
	"github.com/coocood/freecache"
	"runtime/debug"
)

type memoryHandler struct {
	c       *freecache.Cache
	rh      *redisHandler
	sharing *sharing
}

func (mh *memoryHandler) getCacheTtl(ttl, factor int) int {
	return ttl
}

func (mh *memoryHandler) batchGetFromCache(ctx context.Context, keys []string) (valMap map[string]string) {
	fn := moduleName + ".batchGetFromCache"
	valMap = make(map[string]string)
	for _, key := range keys {
		var vByte []byte
		vByte, err := mh.c.Get([]byte(key))
		if err == nil {
			valMap[key] = string(vByte)
			continue
		}
		if err != freecache.ErrNotFound {
			// 缓存遇到错误，记录日志，继续执行不阻断
			mh.sharing.dealErrHandler(ctx, errors.New(fn+":batch get from memory cache err,"+err.Error()))
		}
		err = nil
	}
	return
}

// 上游需要根据 err 做判断，所以必须要返回
func (mh *memoryHandler) getFromCache(ctx context.Context, key string) (val string, err error) {
	var vByte []byte
	vByte, err = mh.c.Get([]byte(key))
	if err == nil {
		val = string(vByte)
		return
	}
	if err != freecache.ErrNotFound {
		err = errors.New("get memory cache err," + err.Error())
		return
	}
	err = ErrNotFound
	return
}

func (mh *memoryHandler) setFromCache(ctx context.Context, key string, val string, ttl int) {
	fn := moduleName + ".setFromCache"
	keyByte := []byte(key)
	valByte := []byte(val)
	err := mh.c.Set(keyByte, valByte, ttl)
	if err != nil {
		mh.sharing.dealErrHandler(ctx, errors.New(fn+":set memory cache err,"+err.Error()))
	}
	return
}

func (mh *memoryHandler) delFromCache(ctx context.Context, keys []string) {
	fn := moduleName + ".delFromCache"
	go func() {
		var err error
		defer func() {
			if e := recover(); e != nil {
				err = fmt.Errorf("PanicError#, err=%v,trace=%v", e, string(debug.Stack()))
			}
			if err != nil {
				mh.sharing.dealErrHandler(ctx, fmt.Errorf("%s:set publish cache err, keys=%v,err=%s",
					fn, keys, err.Error()))
			}
		}()
		if mh.sharing.pubSubI != nil {
			key := mh.sharing.pubSubI.GetKey()
			err = mh.sharing.pubSubI.Publish(key, mh.sharing.encodePubKey(keys))
		}
	}()
	for _, key := range keys {
		mh.c.Del([]byte(key))
	}
}

func (mh *memoryHandler) addQpsTotal(n int) {
	mh.sharing.qps.addMemoryTotal(n)
}

func (mh *memoryHandler) addQpsHit(n int) {
	mh.sharing.qps.addMemoryHit(n)
}
