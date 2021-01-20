package matryoshka

import (
	"context"
	"errors"
	"fmt"
	"github.com/coocood/freecache"
	"runtime/debug"
	"strings"
	"sync"
)

type Matryoshka struct {
	sharing   *sharing
	mh        *memoryHandler
	rh        *redisHandler
	cacheLock sync.Mutex
	cacheMap  map[string]*CacheHandler
}

func (m *Matryoshka) GetHandler(key string) *CacheHandler {
	m.cacheLock.Lock()
	defer m.cacheLock.Unlock()
	c, _ := m.cacheMap[key]
	return c
}

func (m *Matryoshka) SetHandler(key string, c *CacheHandler) {
	m.cacheLock.Lock()
	defer m.cacheLock.Unlock()
	m.cacheMap[key] = c
}

type sharing struct {
	qps        *qps
	errHandler errHandler
	cacheI     DistributedCacheI
	pubSubI    PubSubChannelI
}

type queryKey struct {
	cacheKeys   []string
	cacheKeyMap map[string]string
}

type batchSource struct {
	retryKeyCacheMap map[string]string
	loadKeyCacheMap  map[string]string
	valMap           map[string]string
	lock             sync.Mutex
}

type Statistics struct {
	EntryCount     int64
	QpsAll         int64
	QpsMemoryTotal int64
	QpsMemoryHit   int64
	QpsRedisTotal  int64
	QpsRedisHit    int64
	QpsSourceTotal int64
}

func (s *sharing) dealErrHandler(ctx context.Context, err error) {
	defer func() {
		if e := recover(); e != nil {
			return
		}
	}()
	if s.errHandler != nil {
		s.errHandler(ctx, err)
	}
}

func (s *sharing) encodePubKey(keys []string) string {
	return strings.Join(keys, "||")
}

func (s *sharing) decodePubKey(keysStr string) []string {
	return strings.Split(keysStr, "||")
}

type CacheHandler struct {
	sharing        *sharing
	lock           *sourceLock
	name           string
	ttl            int
	redisTtlFactor int
	cacheList      []cacheHandlerI
}

type LoadSourceFn func(ctx context.Context, key string) (string, error)
type BatchLoadSourceFn func(ctx context.Context, keys []string) (map[string]string, error)

func Init(options ...MainOption) *Matryoshka {
	mo := &mainOptions{
		cacheSize: defaultCacheSize,
	}
	for _, option := range options {
		option.f(mo)
	}
	s := &sharing{
		errHandler: mo.errHandler,
		qps:        NewQps(),
		cacheI:     mo.cacheI,
		pubSubI:    mo.pubSubI,
	}
	matryoshkaInfo := &Matryoshka{
		sharing:  s,
		cacheMap: make(map[string]*CacheHandler),
		mh: &memoryHandler{
			c:       freecache.NewCache(mo.cacheSize),
			sharing: s,
		},
		rh: newRedisHandler(s),
	}
	ctx := context.Background()
	// subscribe key deletion
	if matryoshkaInfo.sharing.pubSubI != nil {
		key := matryoshkaInfo.sharing.pubSubI.GetKey()
		go matryoshkaInfo.sharing.pubSubI.Subscribe(key, func(message string, err error) {
			if err != nil {
				s.dealErrHandler(ctx, err)
				return
			}
			keys := s.decodePubKey(message)
			for _, key := range keys {
				matryoshkaInfo.mh.c.Del([]byte(key))
			}
		})
	}
	return matryoshkaInfo
}

func (m *Matryoshka) GetStatistics() *Statistics {
	return &Statistics{
		EntryCount:     m.mh.c.EntryCount(),
		QpsAll:         m.sharing.qps.all.viewTotal,
		QpsMemoryTotal: m.sharing.qps.memoryTotal.viewTotal,
		QpsMemoryHit:   m.sharing.qps.memoryHit.viewTotal,
		QpsRedisTotal:  m.sharing.qps.redisTotal.viewTotal,
		QpsRedisHit:    m.sharing.qps.redisHit.viewTotal,
		QpsSourceTotal: m.sharing.qps.sourceTotal.viewTotal,
	}
}

func (m *Matryoshka) NewCache(name string, ttl int, options ...ItemOption) *CacheHandler {
	io := &itemOptions{
		cacheType:      CacheTypeAll,
		redisTtlFactor: defaultRedisTtlFactor,
	}
	for _, option := range options {
		option.f(io)
	}
	c := &CacheHandler{
		name:           name,
		ttl:            ttl,
		redisTtlFactor: io.redisTtlFactor,
		sharing:        m.sharing,
		lock: &sourceLock{
			content: make(map[string]chan int),
		},
	}
	switch io.cacheType {
	case CacheTypeAll:
		c.cacheList = []cacheHandlerI{m.mh, m.rh}
	case CacheTypeMem:
		c.cacheList = []cacheHandlerI{m.mh}
	case CacheTypeExt:
		c.cacheList = []cacheHandlerI{m.rh}
	}
	m.SetHandler(name, c)
	return c
}

func (c *CacheHandler) genKey(cachePre, key string) string {
	if cachePre != "" {
		cachePre = cachePre + "_"
	}
	return c.name + "_cache_" + cachePre + key
}

func (c *CacheHandler) getFromSource(ctx context.Context, key, cacheKey string, loadFn LoadSourceFn, retryNum int) (val string, err error) {
	fn := moduleName + ".getFromSource"
	defer func() {
		if e := recover(); e != nil {
			c.sharing.dealErrHandler(ctx, fmt.Errorf("%s #panic#:%v,%s", fn, e, string(debug.Stack())))
			err = fmt.Errorf("%s panic:%v", fn, e)
		}
	}()
	// 从资源层拿数据
	owner, ch := c.lock.lock(cacheKey)
	if owner {
		// 获取资源锁成功
		defer c.lock.unLock(cacheKey)
		c.sharing.qps.addSourceTotal(1)
		val, err = loadFn(ctx, key)
		if err != nil {
			err = errors.New(fn + ":load source from db err," + err.Error())
			return
		}
		for _, item := range c.cacheList {
			item.setFromCache(ctx, cacheKey, val, item.getCacheTtl(c.ttl, c.redisTtlFactor))
		}
		return
	}
	// 获取资源锁失败，等待通知
	select {
	case <-ch:
		val, err = c.cacheList[0].getFromCache(ctx, cacheKey)
		if err == nil {
			c.cacheList[0].addQpsHit(1)
			return
		}
		if err != ErrNotFound {
			return
		}
		if retryNum >= loadMaxRetry {
			err = ErrSourceTimeout
			return
		}
		// 通知后还没拿到数据，说明获取资源锁的那个操作没有拿到数据，继续重试
		return c.getFromSource(ctx, key, cacheKey, loadFn, retryNum+1)
	}
}

func (c *CacheHandler) batchGetFromSource(ctx context.Context, queryKeys *queryKey, loadFn BatchLoadSourceFn) (valMap map[string]string, err error) {
	fn := moduleName + ".batchGetFromSource"
	defer func() {
		if e := recover(); e != nil {
			c.sharing.dealErrHandler(ctx, fmt.Errorf("%s #panic#:%v,%s", fn, e, string(debug.Stack())))
			err = fmt.Errorf("%s panic:%v", fn, e)
		}
	}()
	batchSource := &batchSource{
		retryKeyCacheMap: make(map[string]string),
		loadKeyCacheMap:  make(map[string]string),
		valMap:           make(map[string]string),
	}
	wg := &sync.WaitGroup{}
	// 从资源层拿数据
	for cacheKey, key := range queryKeys.cacheKeyMap {
		owner, ch := c.lock.lock(cacheKey)
		if owner {
			// 获取资源锁成功
			batchSource.loadKeyCacheMap[key] = cacheKey
			c.sharing.qps.addSourceTotal(1)
			continue
		}
		wg.Add(1)
		go c.waitLoadFromSource(ctx, wg, ch, key, cacheKey, batchSource)
	}
	err = c.batchLoadFromSource(ctx, batchSource, loadFn)
	//先wait
	wg.Wait()
	if err != nil {
		return
	}
	if len(batchSource.retryKeyCacheMap) == 0 {
		valMap = batchSource.valMap
		return
	}
	batchSource.loadKeyCacheMap = batchSource.retryKeyCacheMap
	err = c.batchLoadFromSource(ctx, batchSource, loadFn)
	if err != nil {
		return
	}
	valMap = batchSource.valMap
	return
}

func (c *CacheHandler) waitLoadFromSource(ctx context.Context, wg *sync.WaitGroup, ch <-chan int, key, cacheKey string, batchSource *batchSource) {
	fn := "CacheHandler.waitLoadFromSource"
	var err error
	var val string
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("#panic#:%v,%s", e, string(debug.Stack()))
		}
		if err != nil {
			c.sharing.dealErrHandler(ctx, errors.New(fn+":"+err.Error()))
		}
		wg.Done()
	}()
	// 获取资源锁失败，等待通知
	select {
	case <-ch:
		val, err = c.cacheList[0].getFromCache(ctx, cacheKey)
		batchSource.lock.Lock()
		defer batchSource.lock.Unlock()
		if err == nil {
			c.cacheList[0].addQpsHit(1)
			batchSource.valMap[key] = val
			return
		}
		if err == ErrNotFound {
			err = nil
		}
		// 通知后查询缓存错误，或者没拿到数据，继续重试
		batchSource.retryKeyCacheMap[key] = cacheKey
	}
}

func (c *CacheHandler) batchLoadFromSource(ctx context.Context, batchSource *batchSource, loadFn BatchLoadSourceFn) (err error) {
	fn := "CacheHandler.batchLoadFromSource"
	lm := len(batchSource.loadKeyCacheMap)
	if lm == 0 {
		return
	}
	keys := make([]string, 0, lm)
	for key, cacheKey := range batchSource.loadKeyCacheMap {
		keys = append(keys, key)
		defer c.lock.unLock(cacheKey)
	}
	valMap, err := loadFn(ctx, keys)
	if err != nil {
		err = errors.New(fn + ":load source from db err," + err.Error())
		return
	}
	batchSource.lock.Lock()
	defer batchSource.lock.Unlock()
	for key, val := range valMap {
		batchSource.valMap[key] = val
		for _, item := range c.cacheList {
			item.setFromCache(ctx, batchSource.loadKeyCacheMap[key], val, item.getCacheTtl(c.ttl, c.redisTtlFactor))
		}
	}
	return
}

func (c *CacheHandler) batchGetFromCache(ctx context.Context, cachePre string, keys []string) (noCachedMap *queryKey, valMap map[string]string, err error) {
	lk := len(keys)
	qk := &queryKey{
		cacheKeys:   make([]string, 0, lk),
		cacheKeyMap: make(map[string]string, lk),
	}
	valMap = make(map[string]string, lk)
	for _, key := range keys {
		cacheKey := c.genKey(cachePre, key)
		//去重
		if _, ok := qk.cacheKeyMap[cacheKey]; ok {
			continue
		}
		qk.cacheKeyMap[cacheKey] = key
		qk.cacheKeys = append(qk.cacheKeys, cacheKey)
	}
	for i, item := range c.cacheList {
		lck := len(qk.cacheKeys)
		item.addQpsTotal(lck)
		tmpNoCachedMap := &queryKey{
			cacheKeys:   make([]string, 0, lk),
			cacheKeyMap: make(map[string]string, lk),
		}
		tmpValMap := item.batchGetFromCache(ctx, qk.cacheKeys)
		for cacheKey, key := range qk.cacheKeyMap {
			val, ok := tmpValMap[cacheKey]
			if !ok || val == "" {
				// 缓存无数据
				tmpNoCachedMap.cacheKeys = append(tmpNoCachedMap.cacheKeys, cacheKey)
				tmpNoCachedMap.cacheKeyMap[cacheKey] = key
				continue
			}
			valMap[key] = val
			for pi := i - 1; pi >= 0; pi-- {
				c.cacheList[pi].setFromCache(ctx, cacheKey, val, c.cacheList[pi].getCacheTtl(c.ttl, c.redisTtlFactor))
			}
		}
		item.addQpsHit(lck - len(tmpNoCachedMap.cacheKeys))
		qk = tmpNoCachedMap
		if len(qk.cacheKeys) == 0 {
			return nil, valMap, nil
		}
	}
	if len(qk.cacheKeys) == 0 {
		return nil, valMap, nil
	}
	return qk, valMap, nil
}

func (c *CacheHandler) Get(ctx context.Context, cachePre, key string, loadFn LoadSourceFn) (val string, err error) {
	c.sharing.qps.addAll(1)
	fn := moduleName + ".Get"
	noCachedMap, valMap, err := c.batchGetFromCache(ctx, cachePre, []string{key})
	if err != nil {
		err = errors.New(fn + ":" + err.Error())
		return
	}
	val, ok := valMap[key]
	if ok {
		return
	}
	val, err = c.getFromSource(ctx, key, noCachedMap.cacheKeys[0], loadFn, 1)
	if err != nil {
		err = errors.New(fn + ":" + err.Error())
		return
	}
	if val == "" {
		err = ErrNotFound
	}
	return
}

func (c *CacheHandler) BatchGet(ctx context.Context, cachePre string, keys []string, loadFn BatchLoadSourceFn) (valMap map[string]string, err error) {
	fn := moduleName + ".BatchGet"
	l := len(keys)
	if l < 1 {
		return
	}
	c.sharing.qps.addAll(l)
	noCachedMap, valMap, err := c.batchGetFromCache(ctx, cachePre, keys)
	if err != nil {
		err = errors.New(fn + ":" + err.Error())
		return
	}
	if noCachedMap == nil || len(noCachedMap.cacheKeys) == 0 {
		return
	}
	if valMap == nil {
		valMap = make(map[string]string)
	}
	// 从资源层中拿数据
	dbValMap, err := c.batchGetFromSource(ctx, noCachedMap, loadFn)
	if err != nil {
		err = errors.New(fn + ":batch get from source err," + err.Error())
		return
	}
	for k, v := range dbValMap {
		valMap[k] = v
	}
	return
}

func (c *CacheHandler) Del(ctx context.Context, cachePre string, keys []string) {
	lk := len(keys)
	delKeys := make([]string, 0, lk)
	for _, key := range keys {
		delKeys = append(delKeys, c.genKey(cachePre, key))
	}
	l := len(c.cacheList)
	for i := l - 1; i >= 0; i-- {
		c.cacheList[i].delFromCache(ctx, delKeys)
	}
}
