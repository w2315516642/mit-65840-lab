package shardgrp

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type Clerk struct {
	*tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can  add to this struct.
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{Clnt: clnt, servers: servers}
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// Your code here
	args := rpc.GetArgs{
		Key: key,
	}

	maxRetry := 10
	for {
		var reply rpc.GetReply

		server := ck.servers[ck.leader]
		ok := ck.Call(server, "KVServer.Get", &args, &reply)
		if ok {
			if reply.Err == rpc.ErrNoKey || reply.Err == rpc.OK {
				return reply.Value, reply.Version, reply.Err
			} else if reply.Err == rpc.ErrWrongGroup {
				return "", 0, rpc.ErrWrongGroup
			}
		}
		if maxRetry <= 0 {
			return "", 0, rpc.ErrWrongGroup
		}
		maxRetry -= 1
		ck.leader = (ck.leader + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// Your code here
	args := rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}
	resend := false

	maxRetry := 10
	for {
		var reply rpc.PutReply

		server := ck.servers[ck.leader]
		ok := ck.Call(server, "KVServer.Put", &args, &reply)
		if ok {
			if reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey {
				return reply.Err
			} else if (reply.Err == rpc.ErrVersion) && !resend {
				return reply.Err
			} else if (reply.Err == rpc.ErrVersion) && resend {
				return rpc.ErrMaybe
			} else if reply.Err == rpc.ErrWrongGroup {
				return rpc.ErrWrongGroup
			}
			// ErrWrongLeader 的时候换 Leader 重试
		}
		if maxRetry <= 0 {
			return rpc.ErrWrongLeader
		}
		maxRetry -= 1
		resend = true
		ck.leader = (ck.leader + 1) % len(ck.servers)
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here
	args := shardrpc.FreezeShardArgs{
		Shard: s,
		Num:   num,
	}

	for {
		var reply shardrpc.FreezeShardReply

		server := ck.servers[ck.leader]
		ok := ck.Call(server, "KVServer.FreezeShard", &args, &reply)
		if ok {
			if reply.Err != rpc.ErrWrongLeader {
				return reply.State, reply.Err
			}
		}

		ck.leader = (ck.leader + 1) % len(ck.servers)
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	args := shardrpc.InstallShardArgs{
		Shard: s,
		State: state,
		Num:   num,
	}

	for {
		var reply shardrpc.InstallShardReply

		server := ck.servers[ck.leader]
		ok := ck.Call(server, "KVServer.InstallShard", &args, &reply)
		if ok {
			if reply.Err != rpc.ErrWrongLeader {
				return reply.Err
			}
		}

		ck.leader = (ck.leader + 1) % len(ck.servers)
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	args := shardrpc.DeleteShardArgs{
		Shard: s,
		Num:   num,
	}

	for {
		var reply shardrpc.DeleteShardReply

		server := ck.servers[ck.leader]
		ok := ck.Call(server, "KVServer.DeleteShard", &args, &reply)
		if ok {
			if reply.Err != rpc.ErrWrongLeader {
				return reply.Err
			}
		}

		ck.leader = (ck.leader + 1) % len(ck.servers)
	}
}
