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

	"6.5840/shardkv1/shardgrp"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	rcks map[tester.Tgid]*shardgrp.Clerk
	// You will have to modify this struct.
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
	}
	ck.rcks = make(map[tester.Tgid]*shardgrp.Clerk)
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) GetClerk(gid tester.Tgid) (*shardgrp.Clerk, bool) {
	rck, ok := ck.rcks[gid]
	return rck, ok
}

func (ck *Clerk) Shard2Clerk(shard shardcfg.Tshid) *shardgrp.Clerk {
	var cfg *shardcfg.ShardConfig
	for {
		cfg = ck.sck.Query()
		if cfg != nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	gid := cfg.Shards[shard]
	clnk, ok := ck.GetClerk(gid)
	if !ok {
		servers, exixt := cfg.Groups[gid]
		if exixt && len(servers) > 0 {
			clnk = shardgrp.MakeClerk(ck.clnt, servers)
			ck.rcks[gid] = clnk
		}
	}
	return clnk
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	shard := shardcfg.Key2Shard(key)
	for {
		clnk := ck.Shard2Clerk(shard)
		if clnk == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		value, version, err := clnk.Get(key)
		if err == rpc.OK {
			return value, version, err
		} else if err == rpc.ErrNoKey {
			return "", 0, err
		} else if err == rpc.ErrWrongGroup {
			continue
		}
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	shard := shardcfg.Key2Shard(key)
	for {
		clnk := ck.Shard2Clerk(shard)
		if clnk == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		err := clnk.Put(key, value, version)
		if err == rpc.ErrWrongGroup {
			continue
		}
		return err
	}
}
