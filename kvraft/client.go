package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"tomato-redis/myrpc"
	"tomato-redis/raft"
)

type Clerk struct {
	servers    []*myrpc.ClientEnd
	ClerkIndex int
	mu         sync.Mutex
	// You will have to modify this struct.
	RequestIndex int
	leaderIndex  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// 创建 clerk clerk 就是client使用的
func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = raft.Makepeers(servers)
	ck.ClerkIndex = int(nrand()) % 10000000
	ck.RequestIndex = 0
	ck.leaderIndex = -1
	DPrintf("clerk.Index=%d %v", ck.ClerkIndex, ck)
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
// clerk实现一个Get方法  调用KVServer的Get
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.mu.Lock()
	ck.RequestIndex++
	DPrintf("clerk.Index=%d Get key %v",
		ck.ClerkIndex, key)
	args := GetArgs{
		ClerkIndex:   ck.ClerkIndex,
		Key:          key,
		RequestIndex: ck.RequestIndex,
	}
	ck.mu.Unlock()

	for {
		for k, ce := range ck.servers {
			reply := GetReply{}
			if ck.leaderIndex != -1 && ck.leaderIndex != k {
				continue
			}
			ok := ce.Call("KVServer.Get", &args, &reply)

			if !ok {
				continue
			}
			if reply.Err == ErrWrongLeader {
				ck.leaderIndex = -1
				continue
			}
			if reply.Err == Ovetime {
				continue
			}
			ck.leaderIndex = k
			if reply.Err == OK {
				DPrintf("clerk.Index=%d success Get key %v value %v",
					ck.ClerkIndex, key, reply.Value)
				return reply.Value
			}

		}
		DPrintf("clerk.Index=%d Repeat Get key %v",
			ck.ClerkIndex, key)
	}
	//return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
// 由自己调用 分别实现put 和 Append
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	ck.RequestIndex++
	args := PutAppendArgs{
		ClerkIndex:   ck.ClerkIndex,
		RequestIndex: ck.RequestIndex,
		Key:          key,
		Value:        value,
		Op:           op,
	}
	ck.mu.Unlock()
	DPrintf("clerk.Index=%d PutAppend args %v",
		ck.ClerkIndex, args)

END:
	for {
		// 找到leader
		for k, ce := range ck.servers {
			reply := PutAppendReply{}
			if !(ck.leaderIndex == -1 || ck.leaderIndex == k) {
				continue
			}
			ok := ce.Call("KVServer.PutAppend", &args, &reply)

			if !ok {
				continue
			}
			if reply.Err == Ovetime {
				continue
			}
			if reply.Err == ErrWrongLeader {
				ck.leaderIndex = -1
				continue
			}
			ck.leaderIndex = k
			if reply.Err == OK {
				break END
			}

		}
		DPrintf("clerk.Index=%d Repeat PutAppend args %v",
			ck.ClerkIndex, args)

	}
	DPrintf("clerk.Index=%d success PutAppend args %v",
		ck.ClerkIndex, args)
}

// 不存在则加入 存在则替换
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// 追加
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
