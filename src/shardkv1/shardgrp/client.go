package shardgrp

import (
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

var nextClientID int64

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader  int

	clientID  int64
	requestID int64
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{
		clnt:     clnt,
		servers:  servers,
		clientID: atomic.AddInt64(&nextClientID, 1),
	}
	return ck
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	if len(ck.servers) == 0 {
		return "", 0, rpc.ErrWrongGroup
	}
	args := rpc.GetArgs{Key: key}
	attempts := 0
	for {
		var reply rpc.GetReply
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Get", &args, &reply)
		if ok {
			switch reply.Err {
			case rpc.OK:
				return reply.Value, reply.Version, rpc.OK
			case rpc.ErrNoKey:
				return "", 0, rpc.ErrNoKey
			case rpc.ErrWrongGroup:
				return "", 0, rpc.ErrWrongGroup
			}
		}
		attempts++
		if attempts >= len(ck.servers) {
			return "", 0, rpc.ErrWrongGroup
		}
		ck.nextServer()
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	if len(ck.servers) == 0 {
		return rpc.ErrWrongGroup
	}
	args := rpc.PutArgs{
		Key:       key,
		Value:     value,
		Version:   version,
		ClientId:  ck.clientID,
		RequestId: ck.requestID,
	}
	ck.requestID++
	uncertain := false
	attempts := 0

	for {
		var reply rpc.PutReply
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Put", &args, &reply)
		if !ok {
			uncertain = true
		} else {
			switch reply.Err {
			case rpc.OK:
				return rpc.OK
			case rpc.ErrNoKey:
				return rpc.ErrNoKey
			case rpc.ErrVersion:
				if uncertain {
					return rpc.ErrMaybe
				}
				return rpc.ErrVersion
			case rpc.ErrWrongGroup:
				if uncertain {
					return rpc.ErrMaybe
				}
				return rpc.ErrWrongGroup
			default:
				// Submit may have reached Raft before a server discovered
				// that it was no longer leader, so conservatively remember
				// that the outcome may be ambiguous.
				uncertain = true
			}
		}

		attempts++
		if attempts >= len(ck.servers) {
			if uncertain {
				return rpc.ErrMaybe
			}
			return rpc.ErrWrongGroup
		}
		ck.nextServer()
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	if len(ck.servers) == 0 {
		return nil, rpc.ErrWrongGroup
	}
	args := shardrpc.FreezeShardArgs{Shard: s, Num: num}
	attempts := 0
	for {
		var reply shardrpc.FreezeShardReply
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.FreezeShard", &args, &reply)
		if ok {
			if reply.Err == rpc.OK {
				return reply.State, rpc.OK
			}
			if reply.Err == rpc.ErrWrongGroup || reply.Err == rpc.ErrStaleConfig {
				return nil, reply.Err
			}
		}
		attempts++
		if attempts >= len(ck.servers) {
			return nil, rpc.ErrWrongLeader
		}
		ck.nextServer()
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	if len(ck.servers) == 0 {
		return rpc.ErrWrongGroup
	}
	args := shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}
	attempts := 0
	for {
		var reply shardrpc.InstallShardReply
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.InstallShard", &args, &reply)
		if ok {
			if reply.Err == rpc.OK {
				return rpc.OK
			}
			if reply.Err == rpc.ErrWrongGroup || reply.Err == rpc.ErrStaleConfig {
				return reply.Err
			}
		}
		attempts++
		if attempts >= len(ck.servers) {
			return rpc.ErrWrongLeader
		}
		ck.nextServer()
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	if len(ck.servers) == 0 {
		return rpc.ErrWrongGroup
	}
	args := shardrpc.DeleteShardArgs{Shard: s, Num: num}
	attempts := 0
	for {
		var reply shardrpc.DeleteShardReply
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.DeleteShard", &args, &reply)
		if ok {
			if reply.Err == rpc.OK {
				return rpc.OK
			}
			if reply.Err == rpc.ErrWrongGroup || reply.Err == rpc.ErrStaleConfig {
				return reply.Err
			}
		}
		attempts++
		if attempts >= len(ck.servers) {
			return rpc.ErrWrongLeader
		}
		ck.nextServer()
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) nextServer() {
	ck.leader = (ck.leader + 1) % len(ck.servers)
}
