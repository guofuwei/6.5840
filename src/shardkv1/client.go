package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"
)

type Clerk struct {
	clnt   *tester.Clnt
	sck    *shardctrler.ShardCtrler
	config *shardcfg.ShardConfig
	groups map[tester.Tgid]*shardgrp.Clerk
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt:   clnt,
		sck:    sck,
		groups: make(map[tester.Tgid]*shardgrp.Clerk),
	}
	ck.config = sck.Query()
	return ck
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	shard := shardcfg.Key2Shard(key)
	// A cached configuration may point at a group that has left and is
	// already shut down, in which case it cannot return ErrWrongGroup.
	// Refresh before each top-level operation so we never wait forever on
	// such a stale route.
	ck.refreshConfig()
	for {
		gid, servers, ok := ck.config.GidServers(shard)
		if !ok {
			ck.refreshConfig()
			time.Sleep(10 * time.Millisecond)
			continue
		}

		group := ck.groupClerk(gid, servers)
		value, version, err := group.Get(key)
		if err == rpc.ErrWrongGroup {
			ck.refreshConfig()
			time.Sleep(10 * time.Millisecond)
			continue
		}
		return value, version, err
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	shard := shardcfg.Key2Shard(key)
	ck.refreshConfig()
	uncertain := false
	for {
		gid, servers, ok := ck.config.GidServers(shard)
		if !ok {
			ck.refreshConfig()
			time.Sleep(10 * time.Millisecond)
			continue
		}

		group := ck.groupClerk(gid, servers)
		err := group.Put(key, value, version)
		switch err {
		case rpc.OK, rpc.ErrNoKey:
			return err
		case rpc.ErrVersion:
			if uncertain {
				return rpc.ErrMaybe
			}
			return rpc.ErrVersion
		case rpc.ErrMaybe:
			// Keep retrying the same logical, versioned write.  If an
			// earlier attempt committed, the next serving group will
			// contain that version and turn the outcome into ErrMaybe;
			// otherwise a retry can still complete normally.
			uncertain = true
			ck.refreshConfig()
			time.Sleep(10 * time.Millisecond)
			continue
		case rpc.ErrWrongGroup:
			ck.refreshConfig()
			time.Sleep(10 * time.Millisecond)
			continue
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (ck *Clerk) refreshConfig() {
	ck.config = ck.sck.Query()
}

func (ck *Clerk) groupClerk(gid tester.Tgid, servers []string) *shardgrp.Clerk {
	group, ok := ck.groups[gid]
	if !ok {
		group = shardgrp.MakeClerk(ck.clnt, servers)
		ck.groups[gid] = group
	}
	return group
}
