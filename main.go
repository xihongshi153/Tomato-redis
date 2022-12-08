package main

import (
	"flag"
	"fmt"
	"log"

	//	_ "net/http/pprof"
	"strings"
	"time"
	"tomato-redis/kvraft"
	"tomato-redis/raft"
)

// 当前节点的名字 节点id  所有的的节点地址 端口
var addressAndPort string // example :  "120.46.208.103:8000 120.46.208.103:8001 120.46.208.103:8002"
var peerName string       // example: "peer2"
var peerId int            // example : 1
var kind string           // example : "server" "client"
func Init() {
	flag.StringVar(&kind, "kind", "", "server or client")
	flag.IntVar(&peerId, "id", -1, "peer Id,clientEnd index,from 0")
	flag.StringVar(&peerName, "name", "", "peer name")
	flag.StringVar(&addressAndPort, "port", "", "all address and port")
}
func main() {
	Init()
	flag.Parse()

	if peerId == -1 || peerName == "" || addressAndPort == "" || kind == "" {
		log.Fatal("args nil")
		return
	}

	addressAndPortArray := strings.Split(addressAndPort, " ")
	log.Printf("kind: %v name: %v id: %v addressAndProt: %v ", kind, peerName, peerId, addressAndPortArray[peerId])

	// 构建clientEnd
	//raft.Make(addressAndPortArray, peerId, raft.MakePersister(), make(chan raft.ApplyMsg))
	// 构建raftkv

	switch kind {
	case "server":
		// go func() {
		// 	// 启动一个 http server，注意 pprof 相关的 handler 已经自动注册过了
		// 	portstring := strings.Split(strings.Split(addressAndPort, " ")[peerId], ":")[1]
		// 	portNum, _ := strconv.Atoi(portstring)
		// 	fmt.Println("pprof port", strconv.Itoa(portNum+1000))
		// 	if err := http.ListenAndServe(":"+strconv.Itoa(portNum+1000), nil); err != nil {
		// 		log.Fatal(err)
		// 	}
		// 	os.Exit(0)
		// }()
		kvraft.StartKVServer(addressAndPortArray, peerId, raft.MakePersister(), 10000)
		for {
			time.Sleep(60 * time.Second)
		}
	case "client":
		c := kvraft.MakeClerk(addressAndPortArray)
		fmt.Println("welcome to tomato-redis client")
		var cmd string
		for {
			fmt.Print("client >")
			fmt.Scanln(&cmd)
			if cmd == "quit" {
				break
			}
			switch {
			case cmd == "get":
				var key string
				fmt.Println("get:input key")
				fmt.Scanln(&key)
				fmt.Println(c.Get(key))
			case cmd == "put":
				var key string
				var value string
				fmt.Println("put:input key")
				fmt.Scanln(&key)
				fmt.Println("put:input value")
				fmt.Scanln(&value)
				c.Put(key, value)
			case cmd == "":
			default:
				fmt.Println("input something wrong")
			}
		}

	default:
		fmt.Println("error kind")
	}

}
