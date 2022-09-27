package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"tomato-redis/labgob"
	// "6.824/labrpc"
	"tomato-redis/myrpc"
)

var termTime time.Duration = time.Second / 2
var termTimeK = 30
var randBeforeElectionMax int = 200
var waitVoteMaxTime time.Duration = time.Second / 10
var tickerTime time.Duration = time.Second * 2 / time.Duration(termTimeK)
var reElectionMax int = 200

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	rfme int

	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex         // Lock to protect shared access to this peer's state
	peers     []*myrpc.ClientEnd // RPC end points of all peers
	Persister *Persister         // Object to hold this peer's persisted state
	me        int                // this peer's index into peers[]
	dead      int32              // set by Kill()
	now       time.Time          //上一次受到心跳或者append操作的时间

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// Persistent state on all servers
	Raftstate   int32 // 1 代表 leader 2 candidates 3 follower
	CurrentTerm int   // 服务器最后一次知道的任期号(初始化为0,持续递增)
	VotedFor    int   // 在当前获得选票的候选人的Id 你给谁投的票 他的id是VotedFor
	VotedGet    int   // 获得的选票
	Log         []Log // 所有的日志
	// Volatile state on all servers
	CommitIndex int // 已知的最大的已经被提交的日志条目的索引值 初始化为0 确认但是没有应用
	LastApplied int // 最后被应用到状态机的日志条目索引值 初始化为0  应用
	// Volatile state on leaders
	NextIndex      []int // 对于每一个服务器,需要发送给他的下一条日志条目的索引值
	MatchIndex     []int // 对于每一个服务器,已经复制给他的日志的最高索引值
	AppendRPCCount []int
	//
	ApplyCh chan ApplyMsg
	//
	LastSsTerm   int
	LastSsIndex  int
	SnapshotData []byte
}

// 输入的是Index
func (rf *Raft) get(Index int) Log {
	if Index == 0 {
		return rf.Log[0]
	}
	if Index <= rf.LastSsIndex {
		panic(fmt.Sprintf("rf.get error  Index %d rf.LastSsIndex %d", Index, rf.LastSsIndex))
	}
	return rf.Log[Index-rf.LastSsIndex]
}
func (rf *Raft) getIndex(Index int) int {
	if Index == 0 {
		return 0
	}
	if Index == rf.LastSsIndex {
		return rf.LastSsIndex
	}
	if Index <= rf.LastSsIndex {
		panic(fmt.Sprintf("rf.get error  Index %d rf.LastSsIndex %d", Index, rf.LastSsIndex))
	}
	return rf.Log[Index-rf.LastSsIndex].Index
}
func (rf *Raft) getTerm(Index int) int {
	if Index == 0 {
		return 0
	}
	if Index == rf.LastSsIndex {
		return rf.LastSsTerm
	}
	if Index < rf.LastSsIndex {
		panic(fmt.Sprintf("rf.get error  Index %d rf.LastSsIndex %d", Index, rf.LastSsIndex))
	}
	if Index-rf.LastSsIndex >= len(rf.Log) {
		fmt.Printf("  log %v\n", rf.Log)

		panic(fmt.Sprintf("rf.get error  Index %d rf.LastSsIndex %d log %v", Index, rf.LastSsIndex, rf.Log))
	}
	return rf.Log[Index-rf.LastSsIndex].Term
}

func (rf *Raft) set(Index int, l Log) {
	if Index <= rf.LastSsIndex {
		panic(fmt.Sprintf("rf.set error  Index %d rf.LastSsIndex %d", Index, rf.LastSsIndex))
	}
	rf.Log[Index-rf.LastSsIndex] = l
}
func (rf *Raft) getLastLogIndexAndTerm() (int, int) {
	if len(rf.Log) == 1 {
		return rf.LastSsIndex, rf.LastSsTerm
	}
	return rf.Log[len(rf.Log)-1].Index, rf.Log[len(rf.Log)-1].Term
}
func (rf *Raft) getLastLogIndex() int {
	if len(rf.Log) == 1 {
		return rf.LastSsIndex
	}
	return rf.Log[len(rf.Log)-1].Index
}
func (rf *Raft) getLastLogTerm() int {
	if len(rf.Log) == 1 {
		return rf.LastSsTerm
	}
	return rf.Log[len(rf.Log)-1].Term
}

type Log struct {
	Index   int         // 序列号
	Command interface{} // 指令
	Term    int         // 收到时的任期号
}

func Min(x int, y int) int {
	if x > y {
		return y
	}
	return x
}
func Max(x int, y int) int {
	if x > y {
		return x
	}
	return y
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	rf.mu.Lock()
	var isleader bool = rf.Raftstate == 1
	// Your code here (2A).
	term = rf.CurrentTerm
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	e.Encode(rf.LastSsIndex)
	e.Encode(rf.LastSsTerm)
	e.Encode(rf.CommitIndex)
	data := w.Bytes()
	rf.Persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	DPrintf("raft.me=%d readPersist", rf.me)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var Log []Log
	var CurrentTerm int
	var VotedFor int
	// var LastApplied int
	var LastSsIndex int
	var LastSsTerm int
	var commitIndex int
	if d.Decode(&CurrentTerm) != nil ||
		d.Decode(&VotedFor) != nil ||
		d.Decode(&Log) != nil ||
		// d.Decode(&LastApplied) != nil ||
		d.Decode(&LastSsIndex) != nil ||
		d.Decode(&LastSsTerm) != nil ||
		d.Decode(&commitIndex) != nil {
		DPrintf("raft.me= %d readPersist Decode nil", rf.me)
	} else {
		rf.CurrentTerm = CurrentTerm
		rf.VotedFor = VotedFor
		rf.Log = Log
		// rf.LastApplied = LastSsIndex
		rf.LastSsIndex = LastSsIndex
		rf.LastSsTerm = LastSsTerm
		rf.CommitIndex = commitIndex
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {

	atomic.StoreInt32(&rf.Raftstate, 3)
	atomic.StoreInt32(&rf.dead, 1)
	DPrintf("raft.me=%d kill self", rf.me)

	// Your code here, if desired.
}

func (rf *Raft) killed() bool {

	z := atomic.LoadInt32(&rf.dead)

	return z == 1
}
func (rf *Raft) IsLeader() bool {
	z := atomic.LoadInt32(&rf.Raftstate)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(addressAndPortArray []string, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.me = me
	registerSelf(rf, addressAndPortArray[me])
	time.Sleep(5 * time.Second)
	peers := Makepeers(addressAndPortArray)
	rf.peers = peers
	// Your initialization code here (2A, 2B, 2C).
	DPrintf("raft.me=%d Make raft peer ", me)
	rf.mu.Lock()
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.Log = []Log{}
	rf.Log = append(rf.Log, Log{})
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.Raftstate = 3
	rf.LastSsIndex = 0
	rf.LastSsTerm = 0
	peerslen := len(peers)
	rf.NextIndex = make([]int, peerslen)
	rf.AppendRPCCount = make([]int, peerslen)
	rf.ApplyCh = applyCh
	// 初始化
	logLen := len(rf.Log)
	for i := 0; i < peerslen; i++ {
		rf.NextIndex[i] = logLen
	}
	//初始化为0 即可
	rf.MatchIndex = make([]int, peerslen)
	// initialize from state persisted before a crash
	rf.now = time.Now()
	rf.Persister = persister
	rf.readPersist(persister.ReadRaftState())
	rf.SnapshotData = rf.Persister.ReadSnapshot()
	DPrintf("raft.me=%d init %v log %v rf.Commitindex %v rf.LastApply %d", rf.me, rf.NextIndex, rf.Log, rf.CommitIndex, rf.LastApplied)
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.apply()
	rf.mu.Unlock()
	return rf
}

// 在rpc 中注册自己
func registerSelf(r *Raft, ipAndPort string) {
	rpc.Register(r)
	rpc.HandleHTTP()
	DPrintf("raft.me=%d  registerself %v", r.me, ipAndPort)
	port := strings.Split(ipAndPort, ":")
	l, e := net.Listen("tcp", ":"+port[1])
	if e != nil {
		log.Fatal("listen err:", e)
	}
	go http.Serve(l, nil)
}

//
func Makepeers(addressAndPortArray []string) []*myrpc.ClientEnd {
	// 填充配置 建立连接
	DPrintf("make peers %v", addressAndPortArray)
	clientEnd := make([]*myrpc.ClientEnd, len(addressAndPortArray))
	for i := 0; i < len(addressAndPortArray); i++ {
		c, err := rpc.DialHTTP("tcp", addressAndPortArray[i])
		if err != nil {
			log.Fatal("main rpc.DialHTTP err", err)
		}
		if c == nil {
			log.Fatalf("connect fail %v ", addressAndPortArray[i])
		}
		clientEnd[i] = &myrpc.ClientEnd{
			Client: c,
		}
	}
	return clientEnd
}
