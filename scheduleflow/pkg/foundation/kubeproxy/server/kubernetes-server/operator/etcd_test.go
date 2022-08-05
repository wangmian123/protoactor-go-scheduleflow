package operator

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestWaitUntil(t *testing.T) {
	lock := &sync.RWMutex{}
	cond := sync.NewCond(&sync.RWMutex{})

	go func() {
		time.Sleep(100 * time.Millisecond)
		fmt.Println("consumer start ")
		start := time.Now()
		lock.Lock()
		lock.Unlock()
		fmt.Printf("consumer end with duration %v \n", time.Since(start).Seconds())
	}()

	go func() {
		time.Sleep(500 * time.Millisecond)
		cond.Broadcast()
	}()
	lock.Lock()
	waitUntilCondition(cond, time.Second)
	lock.Unlock()
	fmt.Println("wait done")
}
