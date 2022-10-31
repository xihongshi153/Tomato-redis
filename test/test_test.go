package test

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"tomato-redis/kvraft"
)

var addressAndPort string = "120.46.208.103:8003 120.46.208.103:8001 120.46.208.103:8002"

func ClientInit() *kvraft.Clerk {
	addressAndPortArray := strings.Split(addressAndPort, " ")
	return kvraft.MakeClerk(addressAndPortArray)
}

// 测试计划 80%读 20%写
func TestMain(t *testing.T) {
	fmt.Println("TestMain")
	var wg sync.WaitGroup
	wg.Add(1)
	go One(0, ClientInit(), &wg)

	wg.Add(1)
	go One(1, ClientInit(), &wg)

	wg.Add(1)
	go One(2, ClientInit(), &wg)

	wg.Add(1)
	go One(3, ClientInit(), &wg)

	wg.Add(1)
	go One(4, ClientInit(), &wg)

	wg.Add(1)
	go One(5, ClientInit(), &wg)

	wg.Wait()
}

func One(id int, c *kvraft.Clerk, wg *sync.WaitGroup) {
	begin := time.Now()
	fmt.Println(id, begin)
	r := rand.New(rand.NewSource((int64(time.Now().Second()))))
	for i := 0; i < 3000; i++ {
		if i%1000 == 0 {
			fmt.Print(id, "current task ", i, time.Now(), "\n")
		}
		randNum := r.Int() % 1000
		cnt := 0
		keys := make(map[int]string)
		if randNum < 800 {
			var randKey int
			if cnt != 0 {
				randKey = r.Int() % cnt
			}
			c.Get(strconv.Itoa(randKey))
		} else {
			var randKey int
			if cnt != 0 {
				randKey = r.Int() % cnt
			}
			keys[randKey] = strconv.Itoa(randNum)
			c.Put(strconv.Itoa(randKey), strconv.Itoa(randNum))
			cnt++
		}
	}
	end := time.Now()
	fmt.Println(id, end)
	fmt.Println(id, "time cost:", end.Sub(begin).Seconds())
	wg.Done()
}
