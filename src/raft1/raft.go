package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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
	// snapshot related
	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte
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
	isleader = (rf.state == Leader && !rf.killed())
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		panic("readPersist decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}
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
func (rf *Raft) Snapshot(lIndex int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lIndex <= rf.lastIncludedIndex || lIndex > rf.commitIndex {
		return
	}
	pIndex := rf.l2pIndex(lIndex)

	if pIndex >= len(rf.log) {
		return
	}

	// 更新快照相关状态
	rf.lastIncludedIndex = lIndex
	rf.lastIncludedTerm = rf.log[pIndex].Term
	rf.snapshot = snapshot

	// 截断日志
	newLog := make([]LogEntry, 1)
	newLog[0] = LogEntry{
		Term:    rf.log[pIndex].Term,
		Command: nil,
	}
	// 将 pIndex 之后的所有日志追加到新切片中
	newLog = append(newLog, rf.log[pIndex+1:]...)
	rf.log = newLog

	rf.persist()
}

// 因为快照的引入，节点之间的index都需要为逻辑index（lIndex)，其物理index(pIndex)需要转换：
// pIndex = lIndex - lastIncludedIndex
// lIndex = pIndex + lastIncludedIndex
// 只有涉及rf.log的地方需要转换，其他的都为logic index
func (rf *Raft) l2pIndex(lIndex int) int {
	return lIndex - rf.lastIncludedIndex
}
func (rf *Raft) p2lIndex(pIndex int) int {
	return pIndex + rf.lastIncludedIndex
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
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

// InstallSnapshot RPC arguments structure.
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

// InstallSnapshot RPC reply structure.
type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		return -1
	}
	return rf.log[len(rf.log)-1].Term
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVoteHandler(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

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
	// 如果当前领导者还活着，拒绝投票
	elapsed := time.Since(rf.lastHeardFromLeader)
	if elapsed < time.Duration(HeartbeatInterval)*time.Millisecond && rf.state == Follower {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
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
			if args.LastLogIndex >= rf.p2lIndex(len(rf.log)-1) {
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

func (rf *Raft) AppendEntriesHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// 如果收到的任期号小于当前任期号，拒绝请求
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictIndex = rf.p2lIndex(len(rf.log))
		reply.ConflictTerm = -1
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
		reply.ConflictIndex = rf.p2lIndex(len(rf.log))
		reply.ConflictTerm = -1
		return
	}
	// 处理日志
	pPrevlogIndex := rf.l2pIndex(args.PrevLogIndex)
	if pPrevlogIndex < 0 {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictIndex = rf.lastIncludedIndex + 1
		reply.ConflictTerm = -1
	} else if pPrevlogIndex > len(rf.log)-1 {
		// 日志不匹配，拒绝请求
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictIndex = rf.p2lIndex(len(rf.log))
		reply.ConflictTerm = -1
	} else if rf.log[pPrevlogIndex].Term != args.PrevLogTerm {
		// 日志不匹配，拒绝请求
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictTerm = rf.log[pPrevlogIndex].Term

		pIndex := pPrevlogIndex - 1
		// 找到第一个不匹配的任期
		for pIndex > 0 {
			if rf.log[pIndex].Term != reply.ConflictTerm {
				break
			}
			pIndex--
		}
		reply.ConflictIndex = rf.p2lIndex(pIndex + 1)
	} else {
		// 心跳包或者日志匹配，开始处理 Entries

		// 1. 寻找冲突点 (Conflict Resolution)
		// 我们需要找到第一条“既存在于本地，又与 Leader 发来的不一样”的日志
		matchLen := 0 // 记录有多少条日志是匹配的，不需要重写

		for i, entry := range args.Entries {
			pIndex := pPrevlogIndex + 1 + i

			// 情况 A: 本地日志不够长，说明从这里开始都是新数据，没有冲突
			if pIndex >= len(rf.log) {
				break // 停止比较，剩下的都是要追加的新数据
			}

			// 情况 B: 索引位置存在，但任期不同 -> 发生冲突！
			if rf.log[pIndex].Term != entry.Term {
				// 截断日志：保留冲突点之前的所有日志
				rf.log = rf.log[:pIndex]
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
		// 更新提交索引
		if args.LeaderCommit > rf.commitIndex {
			// 为了安全性，取 Min(LeaderCommit, 我们刚刚验证过的最后一条日志下标)
			lastNewIndex := args.PrevLogIndex + len(args.Entries)
			rf.commitIndex = min(args.LeaderCommit, lastNewIndex)
		}
	}

	// 更新收到心跳包的时间
	rf.lastHeardFromLeader = time.Now()
	rf.votedFor = args.LeaderId
}

func (rf *Raft) InstallSnapshotHandler(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	// fmt.Printf("Recevied InstallSnapshotRPC\n")

	// 如果收到的任期号小于当前任期号，拒绝请求
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
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

	// 更新收到心跳包的时间
	rf.lastHeardFromLeader = time.Now()
	rf.votedFor = args.LeaderId

	// 快照处理
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		// 过时的快照，忽略
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
	} else {
		// 安装快照
		applyMsg := raftapi.ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
		if rf.killed() {
			close(rf.applyCh)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		rf.applyCh <- applyMsg
		rf.mu.Lock()

		reply.Term = rf.currentTerm
		// 截断过期的日志
		pIndex := rf.l2pIndex(args.LastIncludedIndex)
		if pIndex < len(rf.log) && rf.log[pIndex].Term == args.LastIncludedTerm {
			// 保留快照之后的日志
			newLog := make([]LogEntry, 1)
			newLog[0] = LogEntry{
				Term:    rf.log[pIndex].Term,
				Command: nil,
			}
			newLog = append(newLog, rf.log[pIndex+1:]...)
			rf.log = newLog
			if rf.commitIndex < rf.lastIncludedIndex {
				rf.commitIndex = rf.lastIncludedIndex
			}
		} else {
			// 快照覆盖了日志，丢弃快照之前的所有日志
			rf.log = make([]LogEntry, 1)
			rf.log[0] = LogEntry{
				Term:    args.LastIncludedTerm,
				Command: nil,
			}
			rf.commitIndex = rf.lastIncludedIndex
		}

		// 接受快照，更新状态
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.snapshot = args.Data
		rf.lastApplied = args.LastIncludedIndex

		rf.persist()
		rf.mu.Unlock()
	}
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

// channel的方式不适合这里，因为需要等待返回结果，闭包方式更合适

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
	index = rf.p2lIndex(len(rf.log))

	// 不是领导者，返回false
	if rf.killed() || rf.state != Leader {
		isLeader = false
		return index, term, isLeader
	}
	// 将命令追加到日志
	newEntry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, newEntry)
	rf.persist()

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

func (rf *Raft) heartBeatTicker() {
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
				rf.persist()

				voteCount := 1
				// 发送RequestVote RPC
				for i := range rf.peers {
					if i != rf.me && rf.state == Candidate && !rf.killed() {
						args := &RequestVoteArgs{
							Term:         rf.currentTerm,
							CandidateId:  rf.me,
							LastLogIndex: rf.p2lIndex(len(rf.log) - 1),
							LastLogTerm:  rf.getLastLogTerm(),
						}
						go func(server int, args *RequestVoteArgs) {
							reply := &RequestVoteReply{}
							ok := rf.peers[server].Call("Raft.RequestVoteHandler", args, reply)

							rf.mu.Lock()
							defer rf.mu.Unlock()

							// 检查状态有效性
							if !ok || rf.state != Candidate || rf.currentTerm != args.Term {
								return
							}

							if reply.Term > rf.currentTerm {
								rf.currentTerm = reply.Term
								rf.state = Follower
								rf.votedFor = -1
								rf.persist()
								return
							}

							if reply.VoteGranted {
								voteCount++
								if voteCount > len(rf.peers)/2 {
									// 赢得选举
									rf.state = Leader
									// 初始化 Leader 状态
									for j := range rf.peers {
										rf.nextIndex[j] = rf.p2lIndex(len(rf.log))
										rf.matchIndex[j] = 0
									}
									return
								}
							}
						}(i, args)
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

func (rf *Raft) logAppendTicker() {
	heartbeatInterval := HeartbeatInterval * time.Millisecond
	checkInterval := 10 * time.Millisecond

	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state == Leader {
			// 批量发送心跳（包含日志信息）
			for peer := range rf.peers {
				if peer != rf.me && !rf.killed() && rf.state == Leader {
					// 判定是否需要发送快照
					// 1、当 Leader 需要发送的下一条日志已经被压缩进快照时，必须发送快照
					if rf.nextIndex[peer] <= rf.lastIncludedIndex && rf.snapshot != nil {
						// 发送 InstallSnapshot RPC
						args := &InstallSnapshotArgs{
							Term:              rf.currentTerm,
							LeaderId:          rf.me,
							LastIncludedIndex: rf.lastIncludedIndex,
							LastIncludedTerm:  rf.lastIncludedTerm,
							Data:              rf.snapshot,
						}
						go func(server int, args *InstallSnapshotArgs) {
							reply := &InstallSnapshotReply{}
							ok := rf.peers[server].Call("Raft.InstallSnapshotHandler", args, reply)

							rf.mu.Lock()
							defer rf.mu.Unlock()

							if !ok || rf.state != Leader || rf.currentTerm != args.Term {
								return
							}

							if reply.Term > rf.currentTerm {
								rf.currentTerm = reply.Term
								rf.state = Follower
								rf.votedFor = -1
								rf.persist()
								return
							}

							// 快照发送成功，更新 nextIndex 和 matchIndex
							// 注意：这里可能因为并发，rf.lastIncludedIndex 已经又变大了，
							// 但至少我们要保证 nextIndex 推进到当时发送快照的位置
							if args.LastIncludedIndex+1 > rf.nextIndex[server] {
								rf.nextIndex[server] = args.LastIncludedIndex + 1
								rf.matchIndex[server] = args.LastIncludedIndex
							}
						}(peer, args)
						continue
					}
					// 2、发送 AppendEntries RPC
					prevLogIndex := rf.nextIndex[peer] - 1
					prevLogTerm := rf.log[rf.l2pIndex(prevLogIndex)].Term

					// 1. 准备参数 (这是深拷贝或切片引用，在锁内进行)
					args := &AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: prevLogIndex,
						PrevLogTerm:  prevLogTerm,
						Entries:      rf.log[rf.l2pIndex(rf.nextIndex[peer]):], // 如果是空的，就是心跳
						LeaderCommit: rf.commitIndex,
					}

					// 2. 启动协程处理单个 Peer，利用闭包捕获 args
					go func(p int, args *AppendEntriesArgs) {
						reply := &AppendEntriesReply{}
						ok := rf.peers[p].Call("Raft.AppendEntriesHandler", args, reply)

						rf.mu.Lock()
						defer rf.mu.Unlock()

						// 检查过期：RPC失败 / 身份变了 / Term变了
						if !ok || rf.state != Leader || rf.currentTerm != args.Term {
							return
						}

						// 发现更高的 Term，退位
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.state = Follower
							rf.votedFor = -1
							rf.persist()
							return
						}

						if reply.Success {
							// 【重点修复】使用 args 中的信息来更新，而不是 len(rf.log)
							match := args.PrevLogIndex + len(args.Entries)
							if match > rf.matchIndex[p] {
								rf.matchIndex[p] = match
								rf.nextIndex[p] = match + 1
							}
						} else {
							// 快速回退-方法一，主要应对少量Term大量log的冲突情况
							// index := args.PrevLogIndex
							// // 找到第一个不匹配的任期
							// for index > 0 {
							// 	if rf.log[index].Term != args.PrevLogTerm {
							// 		break
							// 	}
							// 	index--
							// }
							// rf.nextIndex[p] = max(1, index+1)

							// 快速回退-方法二，主要应对大量Term少量log的冲突情况
							if reply.ConflictTerm == -1 {
								rf.nextIndex[p] = reply.ConflictIndex
							} else {
								// 尝试在 Leader 日志中找到 ConflictTerm
								pLastIndexOfTerm := -1
								for i := len(rf.log) - 1; i >= 0; i-- {
									if rf.log[i].Term == reply.ConflictTerm {
										pLastIndexOfTerm = i
										break
									}
								}

								if pLastIndexOfTerm != -1 {
									// Leader 也有这个 Term，尝试从该 Term 之后继续
									rf.nextIndex[p] = rf.p2lIndex(pLastIndexOfTerm) + 1
								} else {
									// Leader 没有这个 Term，说明 Follower 该 Term 的日志全是错的，全部跳过
									rf.nextIndex[p] = reply.ConflictIndex
								}
							}
							// 兜底，防止越界
							if rf.nextIndex[p] < 1 {
								rf.nextIndex[p] = 1
							}
						}
					}(peer, args)
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

// 日志提交与应用协程
func (rf *Raft) logCommitAndApplyTicker() {
	// 注意：appleMsg不能在goroutine内发送，否则可能会死锁，而且没有保证顺序的应用状态机
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state == Leader {
			// 检查是否有新的日志可以提交
			for lIndex := rf.commitIndex + 1; lIndex < rf.p2lIndex(len(rf.log)); lIndex++ {
				count := 1
				for i := range rf.peers {
					if i != rf.me && rf.matchIndex[i] >= lIndex {
						count += 1
					}
				}
				if count > len(rf.peers)/2.0 && rf.log[rf.l2pIndex(lIndex)].Term == rf.currentTerm {
					rf.commitIndex = lIndex
				}
			}
		}
		// 对于所有的Raft节点，应用已提交的日志到状态机
		// 统计需要的applyMsg
		allApplyMsgs := []raftapi.ApplyMsg{}
		for rf.killed() == false && rf.lastApplied < rf.commitIndex {
			rf.lastApplied += 1
			applyMsg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.l2pIndex(rf.lastApplied)].Command,
				CommandIndex: rf.lastApplied,
			}
			allApplyMsgs = append(allApplyMsgs, applyMsg)
		}
		rf.mu.Unlock()

		// 应用到状态机（在锁外进行）
		for _, msg := range allApplyMsgs {
			rf.mu.Lock()
			if rf.killed() {
				close(rf.applyCh)
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
		}
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

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.snapshot = nil

	// initialize from state persisted before a crash
	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()
	if rf.lastIncludedIndex > 0 {
		rf.commitIndex = rf.lastIncludedIndex
		rf.lastApplied = rf.lastIncludedIndex
	}
	rf.mu.Unlock()

	// start ticker goroutine to start elections
	go rf.logCommitAndApplyTicker()
	go rf.logAppendTicker()
	go rf.heartBeatTicker()

	return rf
}
