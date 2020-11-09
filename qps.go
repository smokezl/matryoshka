package matryoshka

import (
	"sync/atomic"
	"time"
)

type qps struct {
	all         qpsContent
	memoryTotal qpsContent
	memoryHit   qpsContent
	redisTotal  qpsContent
	redisHit    qpsContent
	sourceTotal qpsContent
}

type qpsContent struct {
	viewTotal int64
	total     int64
}

func NewQps() *qps {
	qps := &qps{}
	go qps.statisticsTotal()
	return qps
}

func (q *qps) addAll(n int) {
	atomic.AddInt64(&q.all.total, int64(n))
}

func (q *qps) addMemoryTotal(n int) {
	atomic.AddInt64(&q.memoryTotal.total, int64(n))
}

func (q *qps) addMemoryHit(n int) {
	atomic.AddInt64(&q.memoryHit.total, int64(n))
}

func (q *qps) addRedisTotal(n int) {
	atomic.AddInt64(&q.redisTotal.total, int64(n))
}

func (q *qps) addRedisHit(n int) {
	atomic.AddInt64(&q.redisHit.total, int64(n))
}

func (q *qps) addSourceTotal(n int) {
	atomic.AddInt64(&q.sourceTotal.total, int64(n))
}

func (q *qps) statisticsTotal() {
	defer func() {
		if e := recover(); e != nil {

		}
	}()
	ticker := time.NewTicker(time.Second)
	for range ticker.C {
		q.all.viewTotal = atomic.SwapInt64(&q.all.total, 0)
		q.memoryTotal.viewTotal = atomic.SwapInt64(&q.memoryTotal.total, 0)
		q.memoryHit.viewTotal = atomic.SwapInt64(&q.memoryHit.total, 0)
		q.redisTotal.viewTotal = atomic.SwapInt64(&q.redisTotal.total, 0)
		q.redisHit.viewTotal = atomic.SwapInt64(&q.redisHit.total, 0)
		q.sourceTotal.viewTotal = atomic.SwapInt64(&q.sourceTotal.total, 0)
	}
}
