package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// persistent state on all servers:
	currentTerm int
	votedFor    int
	log         []LogEntry
	// volatile state on all servers:
	commitIndex int
	lastApplied int
	// volatile state on leaders:
	nextIndex  []int
	matchIndex []int

	// rf state follower, candidate, leader
	state               RaftState
	lastHeardFromLeader time.Time
	applyCh             chan raftapi.ApplyMsg
}

type RaftState int

const (
	Follower  RaftState = 0
	Candidate RaftState = 1
	Leader    RaftState = 2
)

type LogEntry struct {
	Term    int
	Command interface{}
}

const (
	ElectionTimeout   = 600 // milliseconds
	HeartbeatInterval = 200 // milliseconds
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // 候选人任期号
	CandidateId  int // 请求选票的候选人Id
	LastLogIndex int // 候选人最后日志条目的索引值
	LastLogTerm  int // 候选人最后日志条目的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // 当前任期号，以便候选人更新自己
	VoteGranted bool // 候选人赢得了此张选票时为真
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		return -1
	}
	return rf.log[len(rf.log)-1].Term
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果当前领导者还活着，拒绝投票
	elapsed := time.Since(rf.lastHeardFromLeader)
	if elapsed < time.Duration(HeartbeatInterval)*time.Millisecond && rf.state == Follower {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		// 如果收到的任期号比当前任期号大，更新当前任期号，并转换为跟随者
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
	}
	// 只能在跟随者状态下投票
	if rf.state != Follower {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// 检查是否已经投过票
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm > rf.getLastLogTerm() {
			// 候选人的日志更完整，投票
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		} else if args.LastLogTerm < rf.getLastLogTerm() {
			// 候选人的日志不够完整，拒绝投票
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		} else {
			// 日志任期号相同，比较索引值
			if args.LastLogIndex >= len(rf.log)-1 {
				// 候选人的日志更完整，投票
				reply.Term = rf.currentTerm
				reply.VoteGranted = true
				rf.votedFor = args.CandidateId
			} else {
				// 候选人的日志不够完整，拒绝投票
				reply.Term = rf.currentTerm
				reply.VoteGranted = false
			}
		}
	} else {
		// 已经投过票了
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果收到的任期号小于当前任期号，拒绝请求
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// 如果收到的任期号比当前任期号大，更新当前任期号，并转换为跟随者
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
	}
	// 下面处理args.Term == rf.currentTerm的情况
	// 如果是Candidate，转换为Follower
	if rf.state == Candidate {
		rf.state = Follower
		rf.votedFor = args.LeaderId
	}
	// 只有Follower才处理心跳包
	if rf.state != Follower {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// 处理日志
	if args.PrevLogIndex >= len(rf.log) || args.PrevLogIndex < 0 {
		// 日志不匹配，拒绝请求
		reply.Success = false
		reply.Term = rf.currentTerm
	} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// 日志不匹配，拒绝请求
		reply.Success = false
		reply.Term = rf.currentTerm
	} else {
		// 日志匹配，开始处理 Entries

		// 1. 寻找冲突点 (Conflict Resolution)
		// 我们需要找到第一条“既存在于本地，又与 Leader 发来的不一样”的日志
		matchLen := 0 // 记录有多少条日志是匹配的，不需要重写

		for i, entry := range args.Entries {
			idx := args.PrevLogIndex + 1 + i

			// 情况 A: 本地日志不够长，说明从这里开始都是新数据，没有冲突
			if idx >= len(rf.log) {
				break // 停止比较，剩下的都是要追加的新数据
			}

			// 情况 B: 索引位置存在，但任期不同 -> 发生冲突！
			if rf.log[idx].Term != entry.Term {
				// 截断日志：保留冲突点之前的所有日志
				rf.log = rf.log[:idx]
				break // 冲突处理完毕，停止比较，准备追加
			}

			// 情况 C: 索引和任期都相同 -> 匹配，继续检查下一条
			matchLen++
		}

		// 2. 追加新日志 (Append New Entries)
		// 只追加那些本地没有的，或者因为冲突被截断后缺失的部分
		// args.Entries[matchLen:] 就是那些“真正需要写入”的数据
		if matchLen < len(args.Entries) {
			rf.log = append(rf.log, args.Entries[matchLen:]...)
		}

		reply.Success = true
		reply.Term = rf.currentTerm
	}
	// 更新提交索引
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}

	// 更新收到心跳包的时间
	rf.lastHeardFromLeader = time.Now()
	rf.votedFor = args.LeaderId
}

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
type VoteResult struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, ch chan VoteResult) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		ch <- VoteResult{Term: reply.Term, VoteGranted: reply.VoteGranted}
	} else {
		ch <- VoteResult{Term: -1, VoteGranted: false}
	}
}

type AppendEntriesResult struct {
	PeerIndex int
	Term      int
	Success   bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, ch chan AppendEntriesResult) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		ch <- AppendEntriesResult{PeerIndex: server, Term: reply.Term, Success: reply.Success}
	} else {
		ch <- AppendEntriesResult{PeerIndex: server, Term: -1, Success: false}
	}

}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	index = len(rf.log)

	// 不是领导者，返回false
	if rf.state != Leader {
		isLeader = false
		return index, term, isLeader
	}
	// 将命令追加到日志
	newEntry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, newEntry)

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) heartbeatTicker() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.state == Follower || rf.state == Candidate {
			elapsed := time.Since(rf.lastHeardFromLeader)
			if elapsed >= time.Duration(ElectionTimeout)*time.Millisecond || rf.state == Candidate {
				// 超过选举超时时间，转换为候选人，发起新一轮选举
				rf.state = Candidate
				rf.currentTerm += 1
				rf.votedFor = rf.me

				// 发送RequestVote RPC
				ch := make(chan VoteResult, len(rf.peers)-1)
				for i := range rf.peers {
					if i != rf.me && rf.state == Candidate && !rf.killed() {
						args := &RequestVoteArgs{}
						args.Term = rf.currentTerm
						args.CandidateId = rf.me
						args.LastLogIndex = len(rf.log) - 1
						args.LastLogTerm = rf.getLastLogTerm()
						reply := &RequestVoteReply{}

						go rf.sendRequestVote(i, args, reply, ch)
					}
				}
				if rf.state != Candidate {
					continue
				}

				// 等待投票结果
				voteCount := 1
				totalPeers := len(rf.peers)
				for i := 0; i < totalPeers-1; i++ {
					voteResult := <-ch
					if voteResult.Term > rf.currentTerm {
						// 收到更高任期号，转换为Follower，退出选举
						rf.currentTerm = voteResult.Term
						rf.votedFor = -1
						rf.state = Follower
						break
					}

					if voteResult.VoteGranted {
						voteCount += 1
					}
					// 获得多数选票，转换为Leader
					if voteCount > totalPeers/2 && rf.state == Candidate {
						rf.state = Leader
						// 初始化Leader的volatile状态
						for j := range rf.peers {
							rf.nextIndex[j] = len(rf.log)
							rf.matchIndex[j] = 0
						}
						break
					}
				}
			}
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) sendHeartbeat() {
	heartbeatInterval := HeartbeatInterval * time.Millisecond
	checkInterval := 10 * time.Millisecond

	for !rf.killed() {
		rf.mu.Lock()
		if rf.state == Leader {
			term := rf.currentTerm
			peers := make([]int, 0, len(rf.peers)-1)
			for i := range rf.peers {
				if i != rf.me {
					peers = append(peers, i)
				}
			}

			ch := make(chan AppendEntriesResult, len(peers)-1)
			// 批量发送心跳
			for i, peer := range peers {
				args := &AppendEntriesArgs{
					Term:     term,
					LeaderId: rf.me,
				}
				reply := &AppendEntriesReply{}
				if i != rf.me && !rf.killed() && rf.state == Leader {
					go rf.sendAppendEntries(peer, args, reply, ch)
				}
			}

			rf.mu.Unlock()
			time.Sleep(heartbeatInterval)
		} else {
			rf.mu.Unlock()
			time.Sleep(checkInterval)
		}
	}
}

// 日志复制与提交协程
func (rf *Raft) logReplicationAndCommit() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state == Leader {
			// 计算有多少跟随者没有跟上
			needPeerIndexes := make([]int, 0)
			for i := range rf.peers {
				// 排除自己，且matchIndex小于日志最后索引的跟随者
				if i != rf.me && rf.matchIndex[i] < len(rf.log)-1 {
					needPeerIndexes = append(needPeerIndexes, i)
				}
			}
			if len(needPeerIndexes) > 0 {
				// 向需要的跟随者发送 AppendEntries RPC
				ch := make(chan AppendEntriesResult, len(needPeerIndexes))
				for i := range needPeerIndexes {
					peerIndex := needPeerIndexes[i]
					// 发送日志复制请求
					args := &AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.nextIndex[peerIndex] - 1,
						PrevLogTerm:  rf.log[rf.nextIndex[peerIndex]-1].Term,
						Entries:      rf.log[rf.nextIndex[peerIndex]:],
						LeaderCommit: rf.commitIndex,
					}
					reply := &AppendEntriesReply{}
					if peerIndex != rf.me && !rf.killed() && rf.state == Leader {
						go rf.sendAppendEntries(peerIndex, args, reply, ch)
					}
				}
				// 等待复制结果
				for i := 0; i < len(needPeerIndexes); i++ {
					result := <-ch
					// 收到更高任期号，转换为Follower
					if result.Term > rf.currentTerm {
						rf.currentTerm = result.Term
						rf.votedFor = -1
						rf.state = Follower
						break
					}
					if result.Success {
						// 更新matchIndex和nextIndex
						rf.matchIndex[result.PeerIndex] = len(rf.log) - 1
						rf.nextIndex[result.PeerIndex] = len(rf.log)
					} else {
						// 减少nextIndex，重试
						if rf.nextIndex[result.PeerIndex] > 1 {
							rf.nextIndex[result.PeerIndex] -= 1
						}
					}
				}
			}
			// 检查是否有新的日志可以提交
			for N := rf.commitIndex + 1; N < len(rf.log); N++ {
				count := 1 // 包括Leader自己
				for i := range rf.peers {
					if i != rf.me && rf.matchIndex[i] >= N {
						count += 1
					}
				}
				if count > len(rf.peers)/2 && rf.log[N].Term == rf.currentTerm {
					rf.commitIndex = N
				}
			}
		}
		// 对于所有的Raft节点，应用已提交的日志到状态机
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied += 1
			applyMsg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			// applyCh通道用于发送ApplyMsg
			rf.applyCh <- applyMsg
		}

		rf.mu.Unlock()
		// 每隔10毫秒检查一次
		time.Sleep(10 * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.dead = 0
	rf.currentTerm = 0
	rf.votedFor = -1
	// 初始化日志，添加哨兵节点
	rf.log = append(rf.log, LogEntry{
		Term:    0,
		Command: nil,
	})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.mu = sync.Mutex{}
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.state = Follower
	rf.lastHeardFromLeader = time.Now()
	rf.applyCh = applyCh

	// 日志复制与提交协程
	go rf.logReplicationAndCommit()

	// 心跳协程
	go rf.sendHeartbeat()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.heartbeatTicker()

	return rf
}
