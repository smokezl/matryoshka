package matryoshka

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSourceLock(t *testing.T) {
	lock := &sourceLock{
		content: make(map[string]chan int),
	}
	var maxKey = 10
	var maxNum = 1000
	var totalOwnNum int32
	var totalNotOwnNum int32
	for a := 0; a < maxKey; a++ {
		var ownNum int32
		var notOwnNum int32
		key := fmt.Sprintf("%s_%d", "aaa", a)
		go func() {
			for i := 0; i < maxNum; i++ {
				go func() {
					o, c := lock.lock(key)
					if o {
						atomic.AddInt32(&ownNum, 1)
						atomic.AddInt32(&totalOwnNum, 1)
						time.Sleep(time.Second * 1)
						lock.unLock(key)
						return
					}
					select {
					case <-c:
						atomic.AddInt32(&notOwnNum, 1)
						atomic.AddInt32(&totalNotOwnNum, 1)
					}
				}()
			}
			time.Sleep(time.Second * 2)
			if ownNum != 1 {
				t.Error("ownNum != 1")
			}
			if notOwnNum != int32(maxNum)-ownNum {
				t.Error("notOwnNum err")
			}
		}()
	}
	time.Sleep(time.Second * 3)
	if totalOwnNum != 10 {
		t.Error("totalOwnNum != 10")
	}
	if totalNotOwnNum != int32(maxNum*maxKey)-totalOwnNum {
		t.Error("totalNotOwnNum err")
	}
}

func BenchmarkLockChan(b *testing.B) {
	lock := &sourceLock{
		content: make(map[string]chan int),
	}
	key := "aaaa"
	wg := sync.WaitGroup{}
	wg.Add(b.N)
	for i := 0; i < b.N; i++ { // b.N，测试循环次数
		go func() {
			defer wg.Done()
			o, c := lock.lock(key)
			if o {
				lock.unLock(key)
				return
			}
			select {
			case <-c:
			}
		}()
	}
	wg.Wait()
}

func BenchmarkLock(b *testing.B) {
	lock := &sourceLock{
		content: make(map[string]chan int),
	}
	key := "aaaa"
	wg := sync.WaitGroup{}
	wg.Add(b.N)
	for i := 0; i < b.N; i++ { // b.N，测试循环次数
		go func() {
			defer wg.Done()
			for {
				o, _ := lock.lock(key)
				if !o {
					time.Sleep(time.Millisecond * 5)
				} else {
					lock.unLock(key)
					break
				}
			}
		}()
	}
	wg.Wait()
}
