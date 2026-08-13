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

const (
	currentConfigKey = "shardctrler-current-config"
	nextConfigKey    = "shardctrler-next-config"
)

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
	current, currentVersion := sck.mustReadConfig(currentConfigKey)
	next, _, err := sck.readConfig(nextConfigKey)
	if err == rpc.ErrNoKey {
		return
	}
	if err != rpc.OK || next.Num != current.Num+1 {
		return
	}

	sck.finishConfigChange(current, currentVersion, next)
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Keeping next initialized to current gives every later change a
	// durable, versioned slot in which to announce its intent before it
	// starts moving shards.
	sck.putConfig(currentConfigKey, cfg, 0)
	sck.putConfig(nextConfigKey, cfg, 0)
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	old, version := sck.mustReadConfig(currentConfigKey)
	if old.Num == new.Num && old.String() == new.String() {
		return
	}
	if new.Num != old.Num+1 {
		return
	}

	next, nextVersion, nextErr := sck.readConfig(nextConfigKey)
	if nextErr == rpc.OK && next.Num > old.Num {
		// Another controller (or an earlier invocation) already announced
		// the next configuration.  Only help if it is the same change.
		if next.String() == new.String() {
			sck.finishConfigChange(old, version, next)
		}
		return
	}
	if nextErr != rpc.OK && nextErr != rpc.ErrNoKey {
		return
	}
	if nextErr == rpc.ErrNoKey {
		nextVersion = 0
	}
	if !sck.putConfig(nextConfigKey, new, nextVersion) {
		return
	}

	sck.finishConfigChange(old, version, new)
}

// finishConfigChange returns false when the durable current/next records show
// that this target has completed or lost the configuration race.
func (sck *ShardCtrler) finishConfigChange(old *shardcfg.ShardConfig, oldVersion rpc.Tversion, new *shardcfg.ShardConfig) bool {
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
				var err rpc.Err
				state, err = ck.FreezeShard(shard, new.Num)
				if err == rpc.OK {
					break
				}
				if !sck.shouldRetryMigration(new) {
					return false
				}
				time.Sleep(10 * time.Millisecond)
			}
		}

		if newServers, ok := new.Groups[newGID]; ok {
			ck := shardgrp.MakeClerk(sck.clnt, newServers)
			for {
				err := ck.InstallShard(shard, state, new.Num)
				if err == rpc.OK {
					break
				}
				if !sck.shouldRetryMigration(new) {
					return false
				}
				time.Sleep(10 * time.Millisecond)
			}
		}

		if oldServers, ok := old.Groups[oldGID]; ok {
			ck := shardgrp.MakeClerk(sck.clnt, oldServers)
			for {
				err := ck.DeleteShard(shard, new.Num)
				if err == rpc.OK {
					break
				}
				if !sck.shouldRetryMigration(new) {
					return false
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}

	// The old configuration remains visible while shards move.  Publish
	// the new one only after every move has completed.
	return sck.putConfig(currentConfigKey, new, oldVersion)
}

// shouldRetryMigration decides controller ownership from the durable
// configuration records, never from the shard's local state alone. A
// same-target failure may be a transient RPC failure or an incomplete replay;
// stopping there would leave next ahead of current forever, so keep recovery
// active.
func (sck *ShardCtrler) shouldRetryMigration(target *shardcfg.ShardConfig) bool {
	current, _ := sck.mustReadConfig(currentConfigKey)
	if current.Num >= target.Num {
		return false
	}

	next, _, err := sck.readConfig(nextConfigKey)
	if err == rpc.OK {
		if next.Num > target.Num {
			return false
		}
		if next.Num == target.Num {
			if next.String() != target.String() {
				return false
			}
			return true
		}
	}

	return true
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	cfg, _ := sck.mustReadConfig(currentConfigKey)
	return cfg
}

func (sck *ShardCtrler) readConfig(key string) (*shardcfg.ShardConfig, rpc.Tversion, rpc.Err) {
	value, version, err := sck.IKVClerk.Get(key)
	if err != rpc.OK {
		return nil, version, err
	}
	return shardcfg.FromString(value), version, rpc.OK
}

func (sck *ShardCtrler) mustReadConfig(key string) (*shardcfg.ShardConfig, rpc.Tversion) {
	for {
		cfg, version, err := sck.readConfig(key)
		if err == rpc.OK {
			return cfg, version
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// putConfig performs a versioned write and resolves an ambiguous reply by
// reading the key back.  It returns false when some other value won the
// version race.
func (sck *ShardCtrler) putConfig(key string, cfg *shardcfg.ShardConfig, version rpc.Tversion) bool {
	want := cfg.String()
	err := sck.IKVClerk.Put(key, want, version)
	if err == rpc.OK {
		return true
	}
	if err != rpc.ErrMaybe && err != rpc.ErrVersion {
		return false
	}
	got, _, getErr := sck.readConfig(key)
	return getErr == rpc.OK && got.String() == want
}
