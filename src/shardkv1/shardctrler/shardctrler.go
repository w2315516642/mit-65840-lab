package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

const CfgKey = "config"

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
	cfgKey string
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	sck.cfgKey = CfgKey
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
	// Your code here
	cfgStr := cfg.String()
	for {
		err := sck.Put(sck.cfgKey, cfgStr, 0)
		if err == rpc.ErrNoKey {
			continue
		} else if err == rpc.ErrVersion {
			_, _, getErr := sck.Get(sck.cfgKey)
			if getErr == rpc.ErrNoKey {
				continue
			} else {
				// value 相同的话返回，不同的话表示已有配置，不管
				return
			}
		} else if err == rpc.ErrMaybe {
			_, _, getErr := sck.Get(sck.cfgKey)
			if getErr == rpc.ErrNoKey {
				continue
			} else {
				// value 相同的话返回，不同的话表示已有配置，不管
				return
			}
		} else if err == rpc.OK {
			break
		}
	}
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// Your code here.
	value, version, err := sck.Get(sck.cfgKey)
	if err == rpc.OK {
		old := shardcfg.FromString(value)
		if new.Num <= old.Num {
			return
		}

		// 迁移
		for i, oldGid := range old.Shards {
			// shard id 在新配置里对应哪个 gid
			newGid := new.Shards[i]
			if oldGid == newGid {
				continue
			}

			shardID := shardcfg.Tshid(i)

			src := shardgrp.MakeClerk(sck.clnt, old.Groups[oldGid])
			dst := shardgrp.MakeClerk(sck.clnt, new.Groups[newGid])

			state, err := src.FreezeShard(shardID, new.Num)
			if err != rpc.OK {
				return
			}
			err = dst.InstallShard(shardID, state, new.Num)
			if err != rpc.OK {
				return
			}
			err = src.DeleteShard(shardID, new.Num)
			if err != rpc.OK {
				return
			}

		}

		// 发布配置
		putValue := new.String()
		for {
			err = sck.Put(sck.cfgKey, putValue, version)
			if err == rpc.ErrMaybe || err == rpc.ErrVersion {
				newValue, newVersion, getErr := sck.Get(sck.cfgKey)
				if getErr == rpc.OK {
					if newValue == old.String() && newVersion == version {
						continue
					} else if newValue == putValue {
						return
					} else {
						return
					}
				}
			} else if err == rpc.OK {
				return
			} else {
				return
			}
		}
	}
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	value, _, err := sck.Get(sck.cfgKey)
	if err == rpc.OK {
		return shardcfg.FromString(value)
	}
	return nil
}
