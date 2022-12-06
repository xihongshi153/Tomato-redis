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

// go test -v -timeout 100s -run ^TestMain$ tomato-redis/test -count=1
// 测试计划 80%读 20%写
func TestMain(t *testing.T) {
	fmt.Println("TestMain")
	// fmt.Println("#####  10个 ")
	// makeOneMore(10)
	// fmt.Println("#####  20个")
	// makeOneMore(20)
	// fmt.Println("#####  30个")
	// makeOneMore(30)
	// fmt.Println("#####  40个")
	// makeOneMore(40)
	fmt.Println("#####  50个")
	makeOneMore(50)
}

func makeOneMore(cnt int) {
	var wg sync.WaitGroup
	for i := 0; i < cnt; i++ {
		wg.Add(1)
		go One(i, ClientInit(), &wg)
	}
	wg.Wait()
}

func One(id int, c *kvraft.Clerk, wg *sync.WaitGroup) {
	begin := time.Now()
	fmt.Println(id, begin.Minute(), " ", begin.Second())
	r := rand.New(rand.NewSource((int64(time.Now().Second()))))
	for i := 0; i < 2000; i++ {
		if i%1000 == 0 {
			fmt.Print(id, " current task ", i, " ", time.Now().Minute(), time.Now().Second(), "\n")
		}
		randNum := r.Int() % 1000
		cnt := 0
		keys := make(map[int]string)
		if randNum < 500 {
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
	fmt.Println(id, end.Minute(), " ", end.Second())
	fmt.Println(id, "time cost:", end.Sub(begin).Seconds())
	wg.Done()
}
