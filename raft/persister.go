package raft

//
// support for Raft and kvraft to save persistent
// Raft state (log &c) and k/v server snapshots.
//
// we will use the original persister.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import "sync"

type Persister struct {
	mu        sync.Mutex
	raftstate []byte
	snapshot  []byte
}

func MakePersister() *Persister {
	return &Persister{}
}

func clone(orig []byte) []byte {
	x := make([]byte, len(orig))
	copy(x, orig)
	return x
}

func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	return np
}

func (ps *Persister) SaveRaftState(state []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = clone(state)
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.raftstate)
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) SaveStateAndSnapshot(state []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = clone(state)
	ps.snapshot = clone(snapshot)
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.snapshot)
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}

type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderId          int    // leader id
	LastIncludedIndex int    // 快照中最后一条日志的Index
	LastIncludedTerm  int    // 快照中最后一条日志的Term
	Offset            int    //快照应该放在文件的位置
	Data              []byte //快照内容
}
type InstallSnapshotReply struct {
	Term    int  //follower's Term
	Install bool //是否成功
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	DPrintf("raft.me=%d rf.LastSsIndex %d follower  get InstallSnapshot RPC InstallSnapshotArgs.LastSsIndex %d LastSsTerm%d ",
		rf.me, rf.LastSsIndex, args.LastIncludedIndex, args.LastIncludedTerm)
	rf.mu.Lock()
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm || args.LastIncludedIndex < rf.LastSsIndex {
		DPrintf("raft.me=%d args.Term %v  rf.CurrentTerm %v args.LastIncludedIndex %v rf.SnapshotLastIncludedIndex %v",
			rf.me, args.Term, rf.CurrentTerm, args.LastIncludedIndex, rf.LastSsIndex)

		// DPrintf("raft.me=%d refuse InstallSnapshot because of Term less or snapshot older Log %v", rf.me, rf.Log)
		reply.Install = false
		rf.mu.Unlock()
		return nil
	}
	reply.Install = true
	index := args.LastIncludedIndex
	l := make([]Log, 0)
	l = append(l, Log{})
	if index-rf.LastSsIndex+1 < len(rf.Log) {
		l = append(l, rf.Log[index-rf.LastSsIndex+1:len(rf.Log)]...)
	}
	rf.Log = l
	rf.LastSsIndex = index
	rf.LastSsTerm = args.LastIncludedTerm
	rf.CommitIndex = index
	rf.LastApplied = index
	rf.Persister.SaveStateAndSnapshot(rf.Persister.ReadRaftState(), args.Data)
	rf.mu.Unlock()
	rf.ApplyCh <- ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	return nil
}

//leader 同步 follower的 快照
func (rf *Raft) sendSnapshot(server int, args InstallSnapshotArgs) {
	rf.mu.Lock()
	args.Term = rf.CurrentTerm
	args.LeaderId = rf.me
	args.Data = rf.Persister.ReadSnapshot()
	args.LastIncludedIndex = rf.LastSsIndex
	args.LastIncludedTerm = rf.LastSsTerm
	DPrintf("raft.me=%d  send InstallSnapshot RPC to server %d InstallSnapshotArgs %v", rf.me, server, args)
	reply := InstallSnapshotReply{}
	c := rf.peers[server]
	rf.mu.Unlock()
	ok := c.Call("Raft.InstallSnapshot", &args, &reply)
	if !ok {
		return
	}
	if !reply.Install {
		DPrintf("raft.me=%d  server %d refuse InstallSnapshot RPC  InstallSnapshotArgs %v", rf.me, server, args)
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
		return
	}
	rf.mu.Lock()
	rf.NextIndex[server] = args.LastIncludedIndex + 1
	rf.MatchIndex[server] = args.LastIncludedIndex
	rf.mu.Unlock()
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("raft.me=%d state machine Snapshot himself  index %d", rf.me, index)
	if rf.LastSsIndex >= index || index > rf.CommitIndex {
		return
	}
	l := make([]Log, 0)
	l = append(l, Log{})
	l = append(l, rf.Log[index-rf.LastSsIndex+1:len(rf.Log)]...)

	rf.LastSsTerm = rf.getTerm(index)
	rf.LastSsIndex = index
	rf.Log = l

	if index > rf.CommitIndex {
		rf.CommitIndex = index
	}
	if index > rf.LastApplied {
		rf.LastApplied = index
	}
	rf.persist()
	rf.Persister.SaveStateAndSnapshot(rf.Persister.ReadRaftState(), snapshot)

}
