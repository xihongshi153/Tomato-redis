package kvraft

import (
	"log"
	"os"
)

const Debug = false

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	Ovetime        = "Overtime"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		file := "./" + "log_kv" + ".txt"
		logFile, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
		if err != nil {
			panic(err)
		}
		log.SetOutput(logFile) // 将文件设置为log输出的文件
		log.Printf(format, a...)
	}
	return
}

type Err string

// Put or Append
type PutAppendArgs struct {
	ClerkIndex   int
	RequestIndex int
	Key          string
	Value        string
	Op           string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	ClerkIndex   int
	RequestIndex int
	Key          string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
