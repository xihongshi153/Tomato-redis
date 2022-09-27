package raft

import (
	"math/rand"
	"time"
)

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) changeToCandidate() {
	rf.Raftstate = 2
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.VotedGet = 1
	rf.persist()
}

func (rf *Raft) ticker() {
	var k int32 = 0
	rf.mu.Lock()
	DPrintf("raft.me=%d ticker start ", rf.me)
	rf.now = time.Now()
	rf.mu.Unlock()
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		now := rf.now
		raftstate := rf.Raftstate
		// if rf.CommitIndex > rf.LastApplied && rf.Log[rf.CommitIndex].Term == rf.CurrentTerm {
		rf.mu.Unlock()
		// 一秒钟没有更新now
		if time.Since(now) > termTime && raftstate == 3 {
			// 发起选举
			rf.mu.Lock()
			DPrintf("rf.me=%d ticker follower timeout ", rf.me)
			rf.Raftstate = 2
			rf.CurrentTerm++
			rf.mu.Unlock()
			rand.Seed(time.Now().Unix() * int64(rf.me))
			time.Sleep(time.Millisecond * (time.Duration(rand.Intn(randBeforeElectionMax))))
			rf.mu.Lock()
			if rf.Raftstate != 2 {
				rf.mu.Unlock()
				go rf.ticker()
				return
			}
			rf.mu.Unlock()
			rf.mu.Lock()
			rf.changeToCandidate()
			rf.CurrentTerm--
			rf.mu.Unlock()
			go rf.startElection()
			return
		}

		if k > int32(termTimeK) {
			rf.mu.Lock()
			DPrintf("rf.me=%d Term end ", rf.me)
			rf.Raftstate = 2
			rf.CurrentTerm++
			rf.mu.Unlock()
			rand.Seed(time.Now().Unix() * int64(rf.me))
			time.Sleep(time.Millisecond * (time.Duration(rand.Intn(randBeforeElectionMax))))

			rf.mu.Lock()
			if rf.Raftstate != 2 {
				rf.mu.Unlock()
				go rf.ticker()
				return
			}
			rf.mu.Unlock()

			rf.mu.Lock()
			DPrintf("rf.me=%d term over ", rf.me)
			rf.changeToCandidate()
			rf.mu.Unlock()
			go rf.startElection()
			return
		}
		if raftstate == 1 {
			rf.mu.Lock()
			k++
			rf.mu.Unlock()
			go rf.sendLogToAllFollower("")
		}
		time.Sleep(tickerTime)
	}
}
func (rf *Raft) Append() {
	DPrintf("raft.me=%d Append start ", rf.me)
	for !rf.killed() {
		rf.mu.Lock()
		if rf.Raftstate != 1 {
			// 先解锁 再break
			rf.mu.Unlock()
			break
		}
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			lastIndex, _ := rf.getLastLogIndexAndTerm()
			if rf.NextIndex[i] < lastIndex+1 {
				DPrintf("raft.me=%d  rf.MatchIndex %v  rf.NextIndex %v", rf.me, rf.MatchIndex, rf.NextIndex)
				if rf.NextIndex[i] <= rf.LastSsIndex {
					go rf.sendSnapshot(i, InstallSnapshotArgs{})
				} else {
					go rf.sendLogToFollower(i)
				}
			}
		}
		//DPrintf("rf.me=%d every append rf.NextIndex: %v\n", rf.me, rf.NextIndex)
		rf.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	DPrintf("raft.me=%d Append over ", rf.me)
}
func (rf *Raft) Commit(once bool) {
	DPrintf("raft.me=%d Commit start ", rf.me)
	for !rf.killed() {

		rf.mu.Lock()
		if rf.Raftstate != 1 {
			rf.mu.Unlock()
			return
		}
		m := make(map[int]int)
		for _, v := range rf.MatchIndex {
			_, ok := m[v]
			if ok {
				m[v]++
			} else {
				m[v] = 1
			}
		}
		// DPrintf("rf.me=%d rf.MatchIndex: %v ,rf.NextIndex: %v leaderCommit %d\n leader Log %v",
		// 	rf.me, rf.MatchIndex, rf.NextIndex, rf.CommitIndex, rf.Log)
		for k, v := range m {
			if v > len(rf.peers)/2 {
				if k > rf.CommitIndex && k >= rf.LastSsIndex {
					if rf.getTerm(k) == rf.CurrentTerm {
						DPrintf("rf.me=%d leaderCommit has change  %d\n", rf.me, rf.CommitIndex)
						rf.CommitIndex = k
						go rf.sendLogToAllFollower("")
					}
				}
			}
		}
		if once {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	DPrintf("raft.me=%d Commit over ", rf.me)
}
func (rf *Raft) apply() {
	DPrintf("raft.me=%d apply start ", rf.me)
	for !rf.killed() {
		rf.mu.Lock()
		DPrintf("raft.me=%d *** %v ,rf.CommitIndex %v,rf.LastApplied %v isLeader %v %v %v", rf.me, rf.getLastLogIndex(),
			rf.CommitIndex, rf.LastApplied, rf.Raftstate == 1, rf.MatchIndex, rf.NextIndex)
		if rf.LastApplied < rf.LastSsIndex {
			rf.LastApplied = rf.LastSsIndex
			rf.CommitIndex = rf.LastSsIndex
			rf.persist()
		}
		rf.mu.Unlock()
		if rf.CommitIndex > rf.LastApplied {
			for rf.CommitIndex > rf.LastApplied {
				rf.mu.Lock()
				rf.LastApplied++
				DPrintf("raft.me=%d  rf.CommitIndex %d rf.LastApplied %d  apply to state machine rf.LastApplied  %v",
					rf.me, rf.CommitIndex, rf.LastApplied, rf.get(rf.LastApplied))
				applyMsg := ApplyMsg{}
				applyMsg.rfme = rf.me
				applyMsg.Command = rf.get(rf.LastApplied).Command
				applyMsg.CommandValid = true
				applyMsg.SnapshotValid = false
				applyMsg.CommandIndex = rf.LastApplied
				rf.mu.Unlock()
				rf.ApplyCh <- applyMsg
			}
			rf.mu.Lock()
			rf.persist()
			rf.mu.Unlock()
		}
		time.Sleep(10 * time.Millisecond)
	}
	DPrintf("raft.me=%d apply over ", rf.me)
}
