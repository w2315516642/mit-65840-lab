package shardgrp

import (
	"bytes"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const (
	ENVKEY = "65840ENV"
)

type Item struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me  int
	rsm *rsm.RSM
	gid tester.Tgid

	// Your code here
	entries map[string]Item
}

func (kv *KVServer) DoOp(req any) any {
	// Your code here
	switch args := req.(type) {
	case rpc.GetArgs:
		var reply rpc.GetReply

		key := args.Key
		item, exist := kv.entries[key]
		if !exist {
			reply.Err = rpc.ErrNoKey
		} else {
			reply.Value = item.Value
			reply.Version = item.Version
			reply.Err = rpc.OK
		}
		return reply
	case rpc.PutArgs:
		var reply rpc.PutReply

		item, exist := kv.entries[args.Key]
		if !exist {
			if args.Version == 0 {
				item = Item{
					Value:   args.Value,
					Version: 1,
				}
				kv.entries[args.Key] = item
				reply.Err = rpc.OK
			} else {
				reply.Err = rpc.ErrNoKey
			}
		} else {
			if args.Version == item.Version {
				item.Version = args.Version + 1
				item.Value = args.Value
				kv.entries[args.Key] = item
				reply.Err = rpc.OK
			} else {
				reply.Err = rpc.ErrVersion
			}
		}
		return reply
	default:
	}

	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if err := e.Encode(kv.entries); err != nil {
		panic(err)
	}

	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	if len(data) == 0 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	entries := make(map[string]Item)
	if err := d.Decode(&entries); err != nil {
		panic(err)
	}

	kv.entries = entries
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here
	err, result := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}

	rep := result.(rpc.GetReply)
	reply.Err = rep.Err
	reply.Value = rep.Value
	reply.Version = rep.Version
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here
	err, result := kv.rsm.Submit(*args)

	if err != rpc.OK {
		reply.Err = err
		return
	}

	rep := result.(rpc.PutReply)
	reply.Err = rep.Err
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// Your code here
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{gid: gid, me: me}
	kv.entries = make(map[string]Item)
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here

	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}
