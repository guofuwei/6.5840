package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"time"

	"6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"
)

const currentConfigKey = "shardctrler-current-config"

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	want := cfg.String()
	for {
		err := sck.IKVClerk.Put(currentConfigKey, want, 0)
		if err == rpc.OK {
			return
		}

		// A lost Put reply is ambiguous.  Read the key back before
		// retrying so that an already successful initialization is not
		// mistaken for an ErrVersion failure.
		if err == rpc.ErrMaybe || err == rpc.ErrVersion {
			got, _, getErr := sck.readConfig()
			if getErr == rpc.OK && got.String() == want {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	old, version, err := sck.readConfig()
	if err != rpc.OK {
		return
	}
	if old.Num == new.Num && old.String() == new.String() {
		return
	}
	if new.Num != old.Num+1 {
		return
	}

	for shard := shardcfg.Tshid(0); shard < shardcfg.NShards; shard++ {
		oldGID := old.Shards[shard]
		newGID := new.Shards[shard]
		if oldGID == newGID {
			continue
		}

		var state []byte
		if oldServers, ok := old.Groups[oldGID]; ok {
			ck := shardgrp.MakeClerk(sck.clnt, oldServers)
			for {
				var freezeErr rpc.Err
				state, freezeErr = ck.FreezeShard(shard, new.Num)
				if freezeErr == rpc.OK {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
		}

		if newServers, ok := new.Groups[newGID]; ok {
			ck := shardgrp.MakeClerk(sck.clnt, newServers)
			for ck.InstallShard(shard, state, new.Num) != rpc.OK {
				time.Sleep(10 * time.Millisecond)
			}
		}

		if oldServers, ok := old.Groups[oldGID]; ok {
			ck := shardgrp.MakeClerk(sck.clnt, oldServers)
			for ck.DeleteShard(shard, new.Num) != rpc.OK {
				time.Sleep(10 * time.Millisecond)
			}
		}
	}

	// The old configuration remains visible while shards move.  Publish
	// the new one only after every move has completed.
	for {
		putErr := sck.IKVClerk.Put(currentConfigKey, new.String(), version)
		if putErr == rpc.OK {
			return
		}
		if putErr == rpc.ErrMaybe || putErr == rpc.ErrVersion {
			current, currentVersion, getErr := sck.readConfig()
			if getErr != rpc.OK {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			if current.Num == new.Num && current.String() == new.String() {
				return
			}
			if current.Num != old.Num {
				return
			}
			version = currentVersion
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	for {
		cfg, _, err := sck.readConfig()
		if err == rpc.OK {
			return cfg
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (sck *ShardCtrler) readConfig() (*shardcfg.ShardConfig, rpc.Tversion, rpc.Err) {
	value, version, err := sck.IKVClerk.Get(currentConfigKey)
	if err != rpc.OK {
		return nil, version, err
	}
	return shardcfg.FromString(value), version, rpc.OK
}
