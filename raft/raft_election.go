package raft

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //候选人的任期号
	CandidateId  int //候选人id
	LastLogIndex int //候选人最后一条日志的index
	LastLogTerm  int //候选人最后一套日志的任期号
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //当前任期号,候选人用于更新自己
	VoteGranted bool //是否给候选人投票
}

func (rf *Raft) startElection() {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	DPrintf("raft.me=%d startElection Raftstate %d", rf.me, rf.Raftstate)

	args := RequestVoteArgs{}
	args.Term = rf.CurrentTerm

	args.CandidateId = rf.me

	args.LastLogIndex, args.LastLogTerm = rf.getLastLogIndexAndTerm()
	rf.mu.Unlock()
	rf.mu.Lock()
	if rf.Raftstate != 2 {
		rf.mu.Unlock()
		go rf.ticker()
		return
	}
	rf.mu.Unlock()

	var wg sync.WaitGroup
	var k int32 = 0
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		// wg.Add(1)
		rf.mu.Lock()
		atomic.AddInt32(&k, 1)
		go rf.sendRequestVote(&k, &wg, i, args)
		rf.mu.Unlock()
	}
	rf.mu.Lock()
	DPrintf("raft.me=%d wait begin", rf.me)
	rf.mu.Unlock()
	// wg.Wait()

	now := time.Now()
	for true {
		if atomic.LoadInt32(&k) == 0 {
			break
		}
		time.Sleep(waitVoteMaxTime / 5)
		if time.Since(now) > waitVoteMaxTime {
			break
		}
	}
	rf.mu.Lock()
	DPrintf("raft.me=%d wait end Raftstate %d", rf.me, rf.Raftstate)

	if rf.Raftstate != 2 {
		rf.mu.Unlock()

		go rf.ticker()
		return
	}
	rf.mu.Unlock()
	rf.mu.Lock()
	VotedGet := rf.VotedGet
	DPrintf("raft.me=%d election over getVote %d", rf.me, VotedGet)
	rf.mu.Unlock()

	//选举成功
	if VotedGet >= len(rf.peers)/2+1 {
		rf.mu.Lock()
		DPrintf("raft.me=%d election success raft.CurrentTerm=%d", rf.me, rf.CurrentTerm)
		rf.Raftstate = 1
		//rf.VotedFor = -1
		rf.persist()

		rf.VotedGet = 0
		rf.CommitIndex = rf.LastApplied
		rf.now = time.Now()
		peerslen := len(rf.peers)
		rf.NextIndex = make([]int, peerslen)
		// 初始化
		for i := 0; i < peerslen; i++ {
			rf.NextIndex[i] = Max(args.LastLogIndex, 1)
		}
		//初始化为0 即可
		rf.MatchIndex = make([]int, peerslen)
		rf.MatchIndex[rf.me] = args.LastLogIndex
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		go rf.ticker()
		go rf.Append()
		go rf.Commit(false)
		return
	}
	//选举失败 继续选举
	rf.mu.Lock()
	DPrintf("raft.me=%d election fail", rf.me)
	rf.Raftstate = 2
	rf.VotedFor = -1
	rf.VotedGet = 0
	rf.persist()

	rf.mu.Unlock()

	rand.Seed(time.Now().Unix() * int64(rf.me))
	time.Sleep(time.Millisecond * time.Duration(rand.Intn(reElectionMax)))

	rf.mu.Lock()
	if rf.Raftstate != 2 {
		rf.mu.Unlock()
		go rf.ticker()
		return
	}
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.VotedGet = 1
	rf.persist()

	DPrintf("raft.me=%d election again", rf.me)

	rf.mu.Unlock()

	go rf.startElection()
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.CurrentTerm
	// Reply false if term < currentTerm (§5.1)
	DPrintf("raft.me=%d RequestVoteArgs %v", rf.me, args)

	if args.Term < rf.CurrentTerm {
		reply.VoteGranted = false
		DPrintf("raft.me=%d refuseVote because term is less rf.CurrentTerm %d args.Term %d",
			rf.me, rf.CurrentTerm, args.Term)
		return nil
	}

	// 候选人最后一条Log条目的任期号大于本地最后一条Log条目的任期号；
	// 或者，候选人最后一条Log条目的任期号等于本地最后一条Log条目的任期号，
	// 且最后一条log的Index更大
	// If votedFor is null
	// if rf.VotedFor != -1 && rf.VotedFor != args.CandidateId && args.Term == rf.CurrentTerm {
	// 	DPrintf("raft.me=%d refuseVote because has voted for %d", rf.me, rf.VotedFor)
	// 	reply.VoteGranted = false
	// 	return
	// }
	// If RPC request or response contains term T > currentTerm:set currentTerm = T, convert to follower
	// 如果 ags.term 更大 直接投票

	// candidate’s log is at least as up-to-date as receiver’s log, grant vote
	if args.Term > rf.CurrentTerm {
		DPrintf("raft.me=%d change Term from %d to %d", rf.me, rf.CurrentTerm, args.Term)
		rf.CurrentTerm = args.Term
		rf.Raftstate = 3
		rf.VotedFor = -1
		rf.VotedGet = 0
		rf.persist()
	}
	lastIndex, lastTerm := rf.getLastLogIndexAndTerm()
	if args.LastLogTerm < lastTerm {
		DPrintf("raft.me=%d refuseVote because last log Term less", rf.me)
		reply.VoteGranted = false
		return nil
	}
	if args.LastLogIndex < lastIndex && args.LastLogTerm == lastTerm {
		DPrintf("raft.me=%d refuseVote because last log Index less", rf.me)
		reply.VoteGranted = false
		return nil
	}
	if rf.VotedFor != -1 && rf.VotedFor != args.CandidateId && args.Term == rf.CurrentTerm {
		DPrintf("raft.me=%d refuseVote because has voted for %d", rf.me, rf.VotedFor)
		reply.VoteGranted = false
		return nil
	}
	rf.VotedFor = args.CandidateId
	rf.CurrentTerm = args.Term
	rf.VotedGet = 0
	rf.Raftstate = 3
	rf.persist()

	DPrintf("raft.me=%d give Vote to %d", rf.me, args.CandidateId)
	rf.now = time.Now()
	reply.VoteGranted = true
	return nil
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(k *int32, wg *sync.WaitGroup, server int, args RequestVoteArgs) bool {
	reply := RequestVoteReply{}
	rf.mu.Lock()
	s := rf.peers[server]
	rf.mu.Unlock()
	ok := s.Call("Raft.RequestVote", &args, &reply)
	// wg.Done()
	atomic.AddInt32(k, -1)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("raft.me=%d sendRequestVote %d -> %d", rf.me, rf.me, server)
	if args.Term != rf.CurrentTerm {
		return ok
	}
	if reply.VoteGranted {
		DPrintf("raft.me=%d getRequestVote %d -> %d", rf.me, server, rf.me)
		rf.VotedGet++
	} else {
		DPrintf("raft.me=%d do not get %d RequestVote ", rf.me, server)
		if rf.CurrentTerm < reply.Term {
			DPrintf("raft.me=%d There is bigger Term so change to follower ", rf.me)
			rf.Raftstate = 3
			rf.VotedFor = -1
			rf.VotedGet = 0
			rf.CurrentTerm = reply.Term
			rf.persist()

		}
	}

	return ok
}
