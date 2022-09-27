package main

import (
	"flag"
	"log"
	"strings"
	"time"
	"tomato-redis/raft"
)

// 当前节点的名字 节点id  所有的的节点地址 端口
var addressAndPort string
var peerName string
var peerId int

func Init() {
	flag.IntVar(&peerId, "id", -1, "peer Id,clientEnd index,from 0")
	flag.StringVar(&peerName, "name", "", "peer name")
	flag.StringVar(&addressAndPort, "port", "", "all address and port")
}

func main() {
	Init()
	flag.Parse()

	if peerId == -1 || peerName == "" || addressAndPort == "" {
		log.Fatal("args nil")
		return
	}
	addressAndPortArray := strings.Split(addressAndPort, " ")
	log.Printf("name: %v id: %v addressAndProt: %v ", peerName, peerId, addressAndPortArray[peerId])
	// 构建clientEnd
	raft.Make(addressAndPortArray, peerId, raft.MakePersister(), make(chan raft.ApplyMsg))
	for {
		time.Sleep(60 * time.Second)
	}
}
