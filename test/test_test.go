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

var addressAndPort string = "localhost:8003 localhost:8001 localhost:8002"

func ClientInit() *kvraft.Clerk {
	addressAndPortArray := strings.Split(addressAndPort, " ")
	return kvraft.MakeClerk(addressAndPortArray)
}

// go test -v -timeout 100s -run ^TestMain$ tomato-redis/test -count=1
// 测试计划 80%读 20%写
func TestMain(t *testing.T) {
	fmt.Println("TestMain")
}

// oprationCnt  oneClient request times
// ReadWritePercent  readRequest = oprationCnt*ReadWritePercent
// dataNum the scale of append key
func makeOnes(cnt int, oprationCnt int, ReadWritePercent float64, dataNum int) {
	var wg sync.WaitGroup
	for i := 0; i < cnt; i++ {
		wg.Add(1)
		go One(i, ClientInit(), &wg, oprationCnt, ReadWritePercent, dataNum)
	}
	wg.Wait()
}
func One(id int, c *kvraft.Clerk, wg *sync.WaitGroup,
	oprationCnt int, ReadWritePercent float64, dataNum int) {
	begin := time.Now()
	fmt.Println(id, "beginTime ", begin.Minute(), ":", begin.Second())
	r := rand.New(rand.NewSource((int64(time.Now().Second()))))
	for i := 0; i < oprationCnt; i++ {
		if i%1000 == 0 {
			fmt.Print(id, " current task ", i, " ", time.Now().Minute(), time.Now().Second(), "\n")
		}
		randNum := r.Int() % dataNum
		cnt := 0
		keys := make(map[int]string)
		if float64(randNum) < float64(dataNum)*ReadWritePercent {
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
	fmt.Println(id, "end time ", end.Minute(), ":", end.Second())
	fmt.Println(id, "time cost:", end.Sub(begin).Seconds())
	wg.Done()
}
