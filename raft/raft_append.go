package raft

import (
	"time"
)

type AppendEntriesArgs struct {
	Me           int   // 发送者的id
	CurrentTerm  int   // leader的任期号
	Log          []Log // 传递的日志 如果为空说明是心跳
	PrevLogIndex int   // 前一条日志的Index
	PrevLogTerm  int   // 前一条日志的Term
	LeaderCommit int   // leader的CommitIndex
}
type AppendEntriesReply struct {
	Term   int  // currentTerm for leader to update itself
	Append bool // 添加成功
	Repeat bool

	//实现快速回复
	XTerm  int // Term of  conflict entry
	XIndex int // index of first entry of XTerm
	XLen   int // length of log
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 添加Log
	DPrintf("raft.me=%d  client use start , command %v,Log[] %v", rf.me, command, rf.Log)
	if !(rf.Raftstate == 1) {
		return 0, rf.CurrentTerm, false
	}
	l := Log{}
	l.Command = command
	lastIndex, _ := rf.getLastLogIndexAndTerm()
	l.Index = lastIndex + 1
	l.Term = rf.CurrentTerm
	rf.Log = append(rf.Log, l)
	rf.persist()

	rf.MatchIndex[rf.me] = l.Index
	DPrintf("raft.me=%d get client command,leader Log[] %v", rf.me, rf.Log)
	isLeader := rf.Raftstate == 1
	// Your code here (2B).-
	return l.Index, l.Term, isLeader
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("raft.me=%d args %v reply %v", rf.me, args, reply)
	DPrintf("raft.me=%d rf.LastSsIndex %d rf.CurrentTerm %d get %d  rpc AppendEntries but no judge args %v log %v",
		rf.me, rf.LastSsIndex, rf.CurrentTerm, args.Me, args, rf.Log)
	rf.now = time.Now()
	reply.Term = rf.CurrentTerm
	if args.CurrentTerm == rf.CurrentTerm && rf.Raftstate == 1 {
		DPrintf("raft.me=%d refuse follower's append RPC", rf.me)
		return nil
	}
	//if args.CurrentTerm 更小 拒绝
	if args.CurrentTerm < rf.CurrentTerm {
		DPrintf("raft.me=%d refuse %d append because of term args.CurrentTerm %d < rf.CurrentTerm %d",
			rf.me, args.Me, args.CurrentTerm, rf.CurrentTerm)
		reply.Append = false
		return nil
	}
	// if args.CurrentTerm 更大 更新Term,将状态设置成follower
	if args.CurrentTerm > rf.CurrentTerm {
		DPrintf("raft.me=%d changeTerm to %d", rf.me, rf.CurrentTerm)
		rf.CurrentTerm = args.CurrentTerm
		reply.Term = rf.CurrentTerm
		rf.Raftstate = 3
		rf.VotedFor = -1
		rf.VotedGet = 0
		rf.persist()
	}
	// <1>if args中的commitIndex 更大 && args的preLogIndex,preLogTerm与peer的最后一条log的Index,Term一致
	if args.LeaderCommit > rf.CommitIndex {
		if args.PrevLogIndex == rf.getLastLogIndex() && rf.getTerm(args.PrevLogIndex) == args.PrevLogTerm {
			precommitIndex := rf.CommitIndex
			rf.CommitIndex = Max(Min(args.LeaderCommit, rf.getLastLogIndex()), rf.CommitIndex)
			DPrintf("raft.me=%d rf.CommitIndex change from %d to %d", rf.me, precommitIndex, rf.CommitIndex)
		}
	}
	switch {
	case len(args.Log) == 0:
		// 空数组 心跳
		DPrintf("raft.me=%d get %d  heartbeats", rf.me, args.Me)
		rf.Raftstate = 3
		if rf.CurrentTerm < args.CurrentTerm {
			rf.CurrentTerm = args.CurrentTerm
			rf.VotedFor = -1
			rf.VotedGet = 0
		}
		rf.persist()
		rf.now = time.Now()
		return nil
	case len(args.Log) > 0:
		DPrintf("raft.me=%d prevLogArrayIndex %d and follower Log %v", rf.me, args.PrevLogIndex, rf.Log)
		// 如果传递过来的参数的PreLogIndex 比 快照的最后一条Index小,无法比较
		if args.PrevLogIndex < rf.LastSsIndex {
			reply.Append = false
			reply.XIndex = rf.LastSsIndex
			reply.XTerm = rf.LastSsTerm
			return nil
		}
		// 如果log中没有args.PrevLogTerm
		if args.PrevLogIndex > rf.getLastLogIndex() {
			DPrintf("raft.me=%d refuse %d append because  PrevLogIndex bigger than follower's log", rf.me, args.Me)
			reply.Append = false
			reply.XTerm = rf.getLastLogTerm()
			reply.XLen = rf.getLastLogIndex()
			reply.XIndex = rf.getLastLogIndex()
			return nil
		}

		//  找到PreLogIndex在当前节点的Term
		reply.XTerm = rf.getTerm(args.PrevLogIndex)
		// 找到Term的第一条logIndex
		for i := rf.LastSsIndex; i < rf.getLastLogIndex()+1; i++ {
			if rf.getTerm(i) == reply.XTerm {
				reply.XIndex = i
				break
			}
		}
		// Term不匹配
		if rf.getTerm(args.PrevLogIndex) != args.PrevLogTerm {
			DPrintf("raft.me=%d refuse %d append because  term not match", rf.me, args.Me)
			reply.Append = false
			return nil
		}
		// 如果 prelog匹配了 但是 log[0]不匹配 修建Log
		if args.Log[0].Index <= rf.getLastLogIndex() && rf.getTerm(args.Log[0].Index) != args.Log[0].Term {
			DPrintf("raft.me=%d log crash delete last", rf.me)
			rf.Log = rf.Log[0 : args.PrevLogIndex-rf.LastSsIndex+1]
		}
		// 如果 preLog匹配 log[0]也匹配
		if args.Log[0].Index <= rf.getLastLogIndex() && rf.getTerm(args.Log[0].Index) == args.Log[0].Term {
			DPrintf("raft.me=%d log repeate", rf.me)
			reply.Append = true
			// 如果最后一条log也匹配
			if args.Log[len(args.Log)-1].Index < rf.getLastLogIndex()+1 &&
				rf.getTerm(args.Log[len(args.Log)-1].Index) == args.Log[len(args.Log)-1].Term {
				return nil
			}
			// 修剪log 用于append
			rf.Log = rf.Log[0 : args.PrevLogIndex-rf.LastSsIndex+1]
		}
		DPrintf("raft.me=%d follower get the log %v", rf.me, args.Log)
		// append 所有log
		rf.Log = append(rf.Log, args.Log...)
		rf.persist()

		DPrintf("raft.me=%d after append follower Log %v", rf.me, rf.Log)
		reply.Append = true
	}
	return nil
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs) {

	reply := AppendEntriesReply{}
	rf.mu.Lock()
	if len(args.Log) == 0 {
		DPrintf("raft.me=%d sendHeatBeats  %d -> %d", rf.me, rf.me, server)
	} else {
		DPrintf("raft.me=%d sendAppendEntries  %d -> %d", rf.me, rf.me, server)
	}
	if rf.MatchIndex[server] < rf.NextIndex[server] {
		args.LeaderCommit = Min(rf.MatchIndex[server], rf.CommitIndex)
	}
	rf.mu.Unlock()
	rf.mu.Lock()
	s := rf.peers[server]
	if rf.Raftstate != 1 {
		// 在append的过程中发现 不是leader了 立刻停止
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	ok := s.Call("Raft.AppendEntries", &args, &reply)

	if !ok {
		return
	}
	if reply.Term > rf.CurrentTerm {
		rf.mu.Lock()
		DPrintf("raft.me=%d  %d Term bigger change to follower", rf.me, server)
		rf.Raftstate = 3
		rf.VotedFor = -1
		rf.CurrentTerm = reply.Term
		rf.persist()
		rf.mu.Unlock()
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(args.Log) != 0 {
		if !reply.Append {
			DPrintf("raft.me=%d Append Log %v fail  %d -> %d decrease NextIndex", rf.me, args.Log, rf.me, server)
			// if rf.NextIndex[server] > rf.MatchIndex[server]+1 {
			// 	rf.NextIndex[server]--
			// }
			//
			if reply.XIndex <= rf.LastSsIndex {
				rf.NextIndex[server] = reply.XIndex
				return
			}
			// 快重传机制
			if reply.XTerm == -1 {
				rf.NextIndex[server] = reply.XLen
			} else {
				if rf.getTerm(reply.XIndex) == reply.XTerm {
					rf.NextIndex[server] = reply.XIndex + 1
				} else {
					rf.NextIndex[server] = reply.XIndex
				}
			}
		} else {
			rf.MatchIndex[server] = args.Log[len(args.Log)-1].Index
			rf.NextIndex[server] = rf.MatchIndex[server] + 1
			// go rf.Commit(true)
		}
	}

}
func (rf *Raft) sendLogToAllFollower(command interface{}) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("raft.me=%d sendLogToAllFollower", rf.me)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		l := Log{}
		l.Term = rf.CurrentTerm
		lastIndex, _ := rf.getLastLogIndexAndTerm()
		l.Index = lastIndex + 1
		args := AppendEntriesArgs{}
		args.Log = []Log{}
		args.LeaderCommit = rf.CommitIndex
		if command.(string) != "" {
			args.Log = append(args.Log, l)
		}
		args.Me = rf.me
		args.CurrentTerm = rf.CurrentTerm
		// DPrintf("++++++++++ ssIndex %d rf.CommitIndex %d rf.getIndex(rf.CommitIndex) %d rf.getTerm(rf.CommitIndex) %d",
		// 	rf.LastSsIndex, rf.CommitIndex, rf.getIndex(rf.CommitIndex), rf.getTerm(rf.CommitIndex))
		args.PrevLogIndex = rf.getIndex(rf.CommitIndex)
		args.PrevLogTerm = rf.getTerm(rf.CommitIndex)
		// if rf.AppendRPCCount[i] < 100 {
		// 	rf.AppendRPCCount[i]++
		// 	go rf.sendAppendEntries(i, args)
		// }
		go rf.sendAppendEntries(i, args)
	}
}
func (rf *Raft) sendLogToFollower(followerId int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.NextIndex[followerId] <= rf.LastSsIndex {
		return
	}
	l := make([]Log, rf.getLastLogIndex()-rf.NextIndex[followerId]+1)
	// DPrintf("rf.NextIndex[followerId]-rf.LastSsIndex %d log  %v  ", rf.NextIndex[followerId]-rf.LastSsIndex, rf.Log)
	copy(l, rf.Log[rf.NextIndex[followerId]-rf.LastSsIndex:len(rf.Log)])
	// l.Term = rf.CurrentTerm
	// l.Index = rf.NextIndex[followerId]
	args := AppendEntriesArgs{}
	args.Log = l
	DPrintf("raft.me=%d sendLogToFollower %d log %v", rf.me, followerId, l)
	args.Me = rf.me
	args.CurrentTerm = rf.CurrentTerm
	if rf.NextIndex[followerId] != 0 {
		if rf.NextIndex[followerId] <= rf.LastSsIndex {
			return
		}
		args.PrevLogIndex = rf.getIndex(rf.NextIndex[followerId] - 1)
		args.PrevLogTerm = rf.getTerm(rf.NextIndex[followerId] - 1)
	}
	args.LeaderCommit = rf.CommitIndex
	go rf.sendAppendEntries(followerId, args)
}
