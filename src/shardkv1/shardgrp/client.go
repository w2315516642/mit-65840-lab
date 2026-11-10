package shardgrp

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
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

	for {
		var reply rpc.PutReply

		server := ck.servers[ck.leader]
		ok := ck.Call(server, "KVServer.Put", &args, &reply)
		if ok {
			if reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey {
				return reply.Err
			} else if (reply.Err == rpc.ErrVersion || reply.Err == rpc.ErrWrongGroup) && !resend {
				return reply.Err
			} else if (reply.Err == rpc.ErrVersion || reply.Err == rpc.ErrWrongGroup) && resend {
				return rpc.ErrMaybe
			}
		}
		resend = true
		ck.leader = (ck.leader + 1) % len(ck.servers)
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here
	return nil, ""
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	return ""
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	return ""
}
