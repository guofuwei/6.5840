package shardgrp

import (
	"bytes"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

type shardStatus uint8

const (
	shardAbsent shardStatus = iota
	shardServing
	shardFrozen
)

type shardData struct {
	Values   map[string]string
	Versions map[string]rpc.Tversion
}

type shardState struct {
	Data   shardData
	Num    shardcfg.Tnum
	Status shardStatus
}

type putRecord struct {
	RequestID int64
	Err       rpc.Err
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid

	mu       sync.Mutex
	shards   [shardcfg.NShards]shardState
	lastPuts map[int64]putRecord
}

func (kv *KVServer) DoOp(req any) any {
	switch args := req.(type) {
	case rpc.GetArgs:
		kv.mu.Lock()
		defer kv.mu.Unlock()
		shard := &kv.shards[shardcfg.Key2Shard(args.Key)]
		if shard.Status != shardServing {
			return &rpc.GetReply{Err: rpc.ErrWrongGroup}
		}
		value, ok := shard.Data.Values[args.Key]
		if !ok {
			return &rpc.GetReply{Err: rpc.ErrNoKey}
		}
		return &rpc.GetReply{
			Value:   value,
			Version: shard.Data.Versions[args.Key],
			Err:     rpc.OK,
		}

	case rpc.PutArgs:
		kv.mu.Lock()
		defer kv.mu.Unlock()
		shard := &kv.shards[shardcfg.Key2Shard(args.Key)]
		if shard.Status != shardServing {
			return &rpc.PutReply{Err: rpc.ErrWrongGroup}
		}
		if last, ok := kv.lastPuts[args.ClientId]; ok && last.RequestID == args.RequestId {
			return &rpc.PutReply{Err: last.Err}
		}

		reply := &rpc.PutReply{}
		current, exists := shard.Data.Versions[args.Key]
		if !exists {
			if args.Version == 0 {
				shard.Data.Values[args.Key] = args.Value
				shard.Data.Versions[args.Key] = 1
				reply.Err = rpc.OK
			} else {
				reply.Err = rpc.ErrNoKey
			}
		} else if args.Version != current {
			reply.Err = rpc.ErrVersion
		} else {
			shard.Data.Values[args.Key] = args.Value
			shard.Data.Versions[args.Key] = current + 1
			reply.Err = rpc.OK
		}
		kv.lastPuts[args.ClientId] = putRecord{RequestID: args.RequestId, Err: reply.Err}
		return reply

	case shardrpc.FreezeShardArgs:
		kv.mu.Lock()
		defer kv.mu.Unlock()
		shard := &kv.shards[args.Shard]
		reply := &shardrpc.FreezeShardReply{Num: shard.Num}

		if args.Num < shard.Num {
			reply.Err = rpc.ErrWrongGroup
			return reply
		}
		if args.Num == shard.Num {
			if shard.Status != shardFrozen {
				reply.Err = rpc.ErrWrongGroup
				return reply
			}
			reply.State = encodeShard(shard.Data)
			reply.Err = rpc.OK
			return reply
		}
		if shard.Status != shardServing {
			reply.Err = rpc.ErrWrongGroup
			return reply
		}

		shard.Num = args.Num
		shard.Status = shardFrozen
		reply.Num = args.Num
		reply.State = encodeShard(shard.Data)
		reply.Err = rpc.OK
		return reply

	case shardrpc.InstallShardArgs:
		data := decodeShard(args.State)
		kv.mu.Lock()
		defer kv.mu.Unlock()
		shard := &kv.shards[args.Shard]
		if args.Num < shard.Num {
			return &shardrpc.InstallShardReply{Err: rpc.ErrWrongGroup}
		}
		if args.Num == shard.Num {
			if shard.Status == shardServing {
				return &shardrpc.InstallShardReply{Err: rpc.OK}
			}
			return &shardrpc.InstallShardReply{Err: rpc.ErrWrongGroup}
		}

		shard.Data = data
		shard.Num = args.Num
		shard.Status = shardServing
		return &shardrpc.InstallShardReply{Err: rpc.OK}

	case shardrpc.DeleteShardArgs:
		kv.mu.Lock()
		defer kv.mu.Unlock()
		shard := &kv.shards[args.Shard]
		if args.Num < shard.Num {
			return &shardrpc.DeleteShardReply{Err: rpc.ErrWrongGroup}
		}
		if args.Num > shard.Num || shard.Status == shardServing {
			return &shardrpc.DeleteShardReply{Err: rpc.ErrWrongGroup}
		}
		if shard.Status == shardFrozen {
			shard.Data = newShardData()
			shard.Status = shardAbsent
		}
		return &shardrpc.DeleteShardReply{Err: rpc.OK}

	default:
		panic("shardgrp: unknown operation type")
	}
}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.shards) != nil || e.Encode(kv.lastPuts) != nil {
		panic("shardgrp: failed to encode snapshot")
	}
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var shards [shardcfg.NShards]shardState
	var lastPuts map[int64]putRecord
	if d.Decode(&shards) != nil || d.Decode(&lastPuts) != nil {
		panic("shardgrp: failed to decode snapshot")
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.shards = shards
	kv.lastPuts = lastPuts
	if kv.lastPuts == nil {
		kv.lastPuts = make(map[int64]putRecord)
	}
	for i := range kv.shards {
		normalizeShardData(&kv.shards[i].Data)
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, result := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = *result.(*rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	err, result := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = *result.(*rpc.PutReply)
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	err, result := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = *result.(*shardrpc.FreezeShardReply)
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	err, result := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = *result.(*shardrpc.InstallShardReply)
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	err, result := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = *result.(*shardrpc.DeleteShardReply)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{gid: gid, me: me}
	kv.lastPuts = make(map[int64]putRecord)
	for i := range kv.shards {
		kv.shards[i].Data = newShardData()
		if gid == shardcfg.Gid1 {
			kv.shards[i].Status = shardServing
		}
	}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	return []tester.IService{kv, kv.rsm.Raft()}
}

func newShardData() shardData {
	return shardData{
		Values:   make(map[string]string),
		Versions: make(map[string]rpc.Tversion),
	}
}

func normalizeShardData(data *shardData) {
	if data.Values == nil {
		data.Values = make(map[string]string)
	}
	if data.Versions == nil {
		data.Versions = make(map[string]rpc.Tversion)
	}
}

func encodeShard(data shardData) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(data) != nil {
		panic("shardgrp: failed to encode shard")
	}
	return w.Bytes()
}

func decodeShard(data []byte) shardData {
	if len(data) == 0 {
		return newShardData()
	}
	var shard shardData
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&shard) != nil {
		panic("shardgrp: failed to decode shard")
	}
	normalizeShardData(&shard)
	return shard
}
