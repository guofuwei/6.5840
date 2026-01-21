package rsm

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int
	Id  int
	Req any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	waitApplyChs   map[int]chan Notification
	lastAppliedIdx int
}

// 新增一个结构体用于在 Channel 中传递结果
type Notification struct {
	OpId   int
	Result any // 存放 DoOp 的返回值
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:             me,
		maxraftstate:   maxraftstate,
		applyCh:        make(chan raftapi.ApplyMsg),
		sm:             sm,
		waitApplyChs:   make(map[int]chan Notification),
		lastAppliedIdx: 0,
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	if (persister.ReadSnapshot() != nil) && (len(persister.ReadSnapshot()) > 0) {
		rsm.sm.Restore(persister.ReadSnapshot())
	}
	go rsm.ReceiveApplyChTicker()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	// 提交给 Raft 并等待提交完成
	rsm.mu.Lock()
	opId := int(time.Now().UnixNano()) + rand.Int()
	op := Op{Me: rsm.me, Id: opId, Req: req}
	ch := make(chan Notification, 1)
	rsm.waitApplyChs[op.Id] = ch
	rsm.mu.Unlock()
	_, term, isLeader := rsm.rf.Start(op)
	if !isLeader {
		close(ch)
		rsm.mu.Lock()
		delete(rsm.waitApplyChs, op.Id)
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}

	// 循环轮询等待，直到 Apply 成功 OR Term 发生变化 OR 不再是 Leader
	for {
		select {
		case notify, ok := <-ch:
			if !ok {
				return rpc.ErrWrongLeader, nil
			}
			if notify.OpId != op.Id {
				return rpc.ErrWrongLeader, nil
			}
			return rpc.OK, notify.Result

		case <-time.After(10 * time.Millisecond): // 每 10ms 检查一次状态
			currentTerm, isLeader := rsm.rf.GetState()

			// 如果不再是 Leader，或者 Term 变了（说明只要之前没 commit，现在肯定 commit 不了了）
			if !isLeader || currentTerm != term {
				rsm.mu.Lock()
				delete(rsm.waitApplyChs, op.Id)
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}

			// 还有一个情况：如果我们依然是 Leader，Term 也没变,
			// 但是 Log 已经被压缩了（Snapshot），或者 Index 处的 Log 变成了别人的？
			// 这种情况比较少见，通常 Term 变化就足够覆盖 Partion healing 的情况了。
		}
	}
}

func (rsm *RSM) ReceiveApplyChTicker() {
	threshold := int(0.9 * float64(rsm.maxraftstate))
	for {
		msg, ok := <-rsm.applyCh
		if !ok {
			rsm.mu.Lock()
			for _, ch := range rsm.waitApplyChs {
				close(ch)
			}
			rsm.waitApplyChs = make(map[int]chan Notification)
			rsm.mu.Unlock()
			return
		}
		if msg.CommandValid {
			op, ok := msg.Command.(Op)
			if !ok {
				// 处理类型转换失败的情况
				fmt.Println("some error in rsm ReceiveApplyCh")
			}
			// 所有节点都需要执行状态机
			result := rsm.sm.DoOp(op.Req)
			rsm.mu.Lock()
			// 只有Leader节点需要通知等待的客户端
			ch, exists := rsm.waitApplyChs[op.Id]
			if exists && op.Me == rsm.me {
				delete(rsm.waitApplyChs, op.Id)
				rsm.mu.Unlock()
				ch <- Notification{OpId: op.Id, Result: result}
			} else {
				rsm.mu.Unlock()
			}
			// 检查是否需要做快照
			rsm.mu.Lock()
			rsm.lastAppliedIdx = msg.CommandIndex
			if rsm.maxraftstate != -1 && rsm.rf.PersistBytes() > threshold {
				snapshot := rsm.sm.Snapshot()
				rsm.rf.Snapshot(rsm.lastAppliedIdx, snapshot)
			}
			rsm.mu.Unlock()
		} else if msg.SnapshotValid {
			rsm.mu.Lock()
			rsm.sm.Restore(msg.Snapshot)
			rsm.lastAppliedIdx = msg.SnapshotIndex
			rsm.mu.Unlock()
		}
	}
}
