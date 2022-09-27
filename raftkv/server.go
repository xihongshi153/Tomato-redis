package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"tomato-redis/labgob"
	"tomato-redis/raft"
)

const (
	GetType       string = "Get"
	AppendPutType string = "AppendPut"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType string // "Get" "AppendPut"
	Args   interface{}
}

type KVServer struct {
	mu                sync.Mutex
	me                int
	rf                *raft.Raft
	applyCh           chan raft.ApplyMsg
	dead              int32 // set by Kill()
	GetWaitChan       map[int]chan raft.ApplyMsg
	AppendPutWaitChan map[int]chan raft.ApplyMsg
	LastRequestIndex  map[int]int // clerkIndex  requestIndex
	maxraftstate      int         // snapshot if log grows this big
	// Your definitions here.
	stateMap map[string]string // 状态机
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	DPrintf("KVServer.me=%d kill self", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []string, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	DPrintf("KVServer.me=%d servers ", me)
	labgob.Register(Op{})
	labgob.Register(GetArgs{})
	labgob.Register(GetReply{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(PutAppendReply{})
	labgob.Register(raft.AppendEntriesArgs{})
	labgob.Register(raft.AppendEntriesReply{})
	labgob.Register(raft.RequestVoteArgs{})
	labgob.Register(raft.RequestVoteReply{})
	kv := new(KVServer)
	kv.mu.Lock()
	kv.me = me
	kv.maxraftstate = maxraftstate
	// You may need initialization code here.
	kv.stateMap = make(map[string]string)
	kv.applyCh = make(chan raft.ApplyMsg, 1)
	kv.AppendPutWaitChan = make(map[int]chan raft.ApplyMsg)
	kv.GetWaitChan = make(map[int]chan raft.ApplyMsg)
	kv.LastRequestIndex = make(map[int]int)
	if maxraftstate != -1 {
		kv.ingestSnap(persister.ReadSnapshot())
	}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// DPrintf("kv.me=%d kv.stateMap: %v\n", kv.me, kv.stateMap)
	// DPrintf("kv.me=%d kv.LastRequestIndex: %v\n", kv.me, kv.LastRequestIndex)
	go kv.applier()
	kv.mu.Unlock()
	// You may need initialization code here.

	return kv
}

// KvServer 实现Get
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		OpType: GetType,
		Args:   *args,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	DPrintf("KVServer.me=%d leader Get args %v  index %v", kv.me, args, index)
	kv.GetWaitChan[index] = make(chan raft.ApplyMsg, 2)
	waitChan := kv.GetWaitChan[index]
	kv.mu.Unlock()
	select {
	case <-time.After(time.Millisecond * time.Duration(300)):
		DPrintf("KVServer.me=%d leader Get overtime args %v", kv.me, args)
		reply.Err = Ovetime
		kv.mu.Lock()
		delete(kv.GetWaitChan, index)
		kv.mu.Unlock()
	case <-waitChan:
		kv.mu.Lock()
		val, is := kv.stateMap[args.Key]
		if is {
			reply.Value = val
		} else {
			reply.Value = ""
		}
		reply.Err = OK
		delete(kv.GetWaitChan, index)
		kv.mu.Unlock()
	}
}

// kvServer 实现PutAppend
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		OpType: AppendPutType,
		Args:   *args,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	DPrintf("KVServer.me=%d leader PutAppend args %v index %v", kv.me, args, index)
	kv.AppendPutWaitChan[index] = make(chan raft.ApplyMsg, 2)
	waitChan := kv.AppendPutWaitChan[index]
	kv.mu.Unlock()

	select {
	case <-time.After(time.Millisecond * time.Duration(300)):
		DPrintf("KVServer.me=%d leader PutAppend overtime args %v", kv.me, args)
		reply.Err = Ovetime
		kv.mu.Lock()
		delete(kv.AppendPutWaitChan, index)
		kv.mu.Unlock()
	case <-waitChan:
		kv.mu.Lock()
		reply.Err = OK
		delete(kv.AppendPutWaitChan, index)
		kv.mu.Unlock()
		DPrintf("KVServer.me=%d third index %v time %v args %v", kv.me, index, time.Now(), args)
	}
}

// 分发 applyCh中的信息
func (kv *KVServer) applier() {
	for applyMsg := range kv.applyCh {
		// isLeader := kv.rf.Raftstate == 1
		// if !isLeader {
		// 	continue
		// }
		if kv.killed() {
			return
		}
		DPrintf("KVServer.me=%d recieve applyMsg %v", kv.me, applyMsg)
		if applyMsg.SnapshotValid {
			snapshot := applyMsg.Snapshot
			// 执行snapshot
			kv.mu.Lock()
			kv.ingestSnap(snapshot)
			kv.mu.Unlock()
			continue
		}
		if applyMsg.CommandValid {
			op := applyMsg.Command.(Op)
			kv.mu.Lock()
			if op.OpType == GetType {
				isLeader := kv.rf.IsLeader()
				if isLeader {
					c, is := kv.GetWaitChan[applyMsg.CommandIndex]
					if !is {
						DPrintf("KVServer.me=%d command Overtime args %v", kv.me, op.Args.(GetArgs))
					}
					if is {
						c <- applyMsg
					}
				}
			} else {
				isLeader := kv.rf.IsLeader()
				//是leader
				if isLeader {

					args := op.Args.(PutAppendArgs)
					c, is := kv.AppendPutWaitChan[applyMsg.CommandIndex]
					// 等待channel已经被删除了
					if !is {
						DPrintf("KVServer.me=%d command Overtime  kv.LastRequestIndex[args.ClerkIndex] %v   applyMsg.RequestIndex %v args %v  ",
							kv.me, kv.LastRequestIndex[args.ClerkIndex], applyMsg.CommandIndex, op.Args.(PutAppendArgs))
						// 当 peer restart并且当选leader 但是statemap 还没有完全更新
						if kv.LastRequestIndex[args.ClerkIndex] < args.RequestIndex {
							kv.LastRequestIndex[args.ClerkIndex] = args.RequestIndex
							if args.Op == "Put" {
								kv.stateMap[args.Key] = args.Value
							}
							if args.Op == "Append" {
								kv.stateMap[args.Key] += args.Value
							}
						}

					}
					// 等待channel还没有被删除
					if is {
						if kv.LastRequestIndex[args.ClerkIndex] >= args.RequestIndex {
							kv.AppendPutWaitChan[applyMsg.CommandIndex] <- applyMsg
							DPrintf("KVServer.me=%d Command Repeat Apply args.RequestIndex %v  kv.LastRequestIndex[args.ClerkIndex] %v",
								kv.me, args.RequestIndex, kv.LastRequestIndex[args.ClerkIndex])
							kv.judgePersiterSize(applyMsg.CommandIndex, kv.maxraftstate)
							kv.mu.Unlock()

							continue
						}
						kv.LastRequestIndex[args.ClerkIndex] = args.RequestIndex
						if args.Op == "Put" {
							kv.stateMap[args.Key] = args.Value
						}
						if args.Op == "Append" {
							// if len(kv.stateMap[args.Key])-len(args.Value) >= 0 && kv.stateMap[args.Key][len(kv.stateMap[args.Key])-len(args.Value):len(kv.stateMap[args.Key])] == args.Value {
							// 	panic("repeat")
							// }
							kv.stateMap[args.Key] += args.Value
						}
						c <- applyMsg
					}
					// 不是leader
				} else {
					args := op.Args.(PutAppendArgs)
					if kv.LastRequestIndex[args.ClerkIndex] >= args.RequestIndex {
						DPrintf("KVServer.me=%d Command Repeat Apply args.RequestIndex %v  kv.LastRequestIndex[args.ClerkIndex] %v",
							kv.me, args.RequestIndex, kv.LastRequestIndex[args.ClerkIndex])
						kv.judgePersiterSize(applyMsg.CommandIndex, kv.maxraftstate)
						kv.mu.Unlock()

						continue
					}
					kv.LastRequestIndex[args.ClerkIndex] = args.RequestIndex
					if args.Op == "Put" {
						kv.stateMap[args.Key] = args.Value
					}
					if args.Op == "Append" {
						kv.stateMap[args.Key] += args.Value
					}

				}
			}
			kv.judgePersiterSize(applyMsg.CommandIndex, kv.maxraftstate)

			kv.mu.Unlock()
		}

	}
}
func (kv *KVServer) judgePersiterSize(index int, maxraftstate int) {
	if maxraftstate == -1 {
		return
	}

	if kv.rf.Persister.RaftStateSize() > kv.maxraftstate*9/10 {
		DPrintf("KVServer.me=%d judgePersiterSize   RaftStateSize %v",
			kv.me, kv.rf.Persister.RaftStateSize())
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		stateMap := kv.stateMap
		lastRequestIndex := kv.LastRequestIndex
		e.Encode(stateMap)
		e.Encode(lastRequestIndex)
		kv.rf.Snapshot(index, w.Bytes())
		DPrintf("KVServer.me=%d after judgePersiterSize  RaftStateSize %v",
			kv.me, kv.rf.Persister.RaftStateSize())
	}

}

func (kv *KVServer) ingestSnap(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	stateMap := make(map[string]string)
	lastRequestIndex := make(map[int]int)
	d.Decode(&stateMap)
	d.Decode(&lastRequestIndex)
	kv.stateMap = stateMap
	kv.LastRequestIndex = lastRequestIndex
	DPrintf("KVServer.me=%d ingestSnap %v", kv.me, stateMap)
}
