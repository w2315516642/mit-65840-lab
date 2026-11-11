package shardgrp

import (
	"bytes"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const (
	ENVKEY = "65840ENV"
)

type ShardState int

const (
	Absent ShardState = iota
	Serving
	Frozen
)

type ShardMeta struct {
	State  ShardState
	CfgNum shardcfg.Tnum
}

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
	shards  [shardcfg.NShards]ShardMeta
}

func (kv *KVServer) IsServing(key string) bool {
	shardID := shardcfg.Key2Shard(key)
	return kv.shards[shardID].State == Serving
}

func (kv *KVServer) Encode(data any) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(data); err != nil {
		panic(err)
	}
	return w.Bytes()
}

func (kv *KVServer) Decode(data []byte) map[string]Item {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var out map[string]Item
	if err := d.Decode(&out); err != nil {
		panic(err)
	}
	return out
}

func (kv *KVServer) DoOp(req any) any {
	// Your code here
	switch args := req.(type) {
	case rpc.GetArgs:
		var reply rpc.GetReply

		// 该 shard 不服务这个 key 时直接返回 ErrWrongGroup
		if !kv.IsServing(args.Key) {
			reply.Err = rpc.ErrWrongGroup
			return reply
		}

		item, exist := kv.entries[args.Key]
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

		// 该 shard 不服务这个 key 时直接返回 ErrWrongGroup
		if !kv.IsServing(args.Key) {
			reply.Err = rpc.ErrWrongGroup
			return reply
		}

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
	case shardrpc.FreezeShardArgs:
		var reply shardrpc.FreezeShardReply

		shard := kv.shards[args.Shard]
		// 配置编号相等且 Frozen，当作重试请求重新返回数据
		// 正常请求 + shard 正在 Serving，正常返回
		if (args.Num == shard.CfgNum && shard.State == Frozen) ||
			(args.Num > shard.CfgNum && shard.State == Serving) {
			data := make(map[string]Item)
			for key, item := range kv.entries {
				if shardcfg.Key2Shard(key) == args.Shard {
					data[key] = item
				}
			}
			// 修改 shard 状态
			shard.CfgNum = args.Num
			shard.State = Frozen
			kv.shards[args.Shard] = shard
			// 设置返回值
			reply.State = kv.Encode(data)
			reply.Err = rpc.OK
			reply.Num = shard.CfgNum
		} else {
			// 过期请求
			reply.Num = shard.CfgNum
			reply.Err = rpc.ErrVersion
		}
		return reply
	case shardrpc.InstallShardArgs:
		var reply shardrpc.InstallShardReply

		shard := kv.shards[args.Shard]

		if args.Num < shard.CfgNum {
			reply.Err = rpc.ErrVersion
		} else if args.Num == shard.CfgNum {
			if shard.State == Serving {
				reply.Err = rpc.OK
			} else {
				reply.Err = rpc.ErrVersion
			}
		} else if args.Num > shard.CfgNum {
			// 没有这个 shard 的时候正常安装
			if shard.State == Absent {
				// 反序列化 entries
				data := kv.Decode(args.State)
				for key, item := range data {
					kv.entries[key] = item
				}

				shard.CfgNum = args.Num
				shard.State = Serving
				kv.shards[args.Shard] = shard

				reply.Err = rpc.OK
			} else {
				reply.Err = rpc.ErrVersion
			}
		}
		return reply
	case shardrpc.DeleteShardArgs:
		var reply shardrpc.DeleteShardReply

		shard := kv.shards[args.Shard]

		if args.Num == shard.CfgNum {
			if shard.State == Frozen {
				for key := range kv.entries {
					if shardcfg.Key2Shard(key) == args.Shard {
						delete(kv.entries, key)
					}
				}
				shard.State = Absent
				kv.shards[args.Shard] = shard

				reply.Err = rpc.OK
			} else if shard.State == Absent {
				reply.Err = rpc.OK
			} else {
				reply.Err = rpc.ErrVersion
			}
		} else {
			reply.Err = rpc.ErrVersion
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
	if err := e.Encode(kv.shards); err != nil {
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
	var shards [shardcfg.NShards]ShardMeta
	if err := d.Decode(&entries); err != nil {
		panic(err)
	}
	if err := d.Decode(&shards); err != nil {
		panic(err)
	}

	kv.entries = entries
	kv.shards = shards
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
	err, result := kv.rsm.Submit(*args)

	if err != rpc.OK {
		reply.Err = err
		return
	}

	*reply = result.(shardrpc.FreezeShardReply)
	// reply.Err = rep.Err
	// reply.Num = rep.Num
	// reply.State = rep.State
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
	err, result := kv.rsm.Submit(*args)

	if err != rpc.OK {
		reply.Err = err
		return
	}

	*reply = result.(shardrpc.InstallShardReply)
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
	err, result := kv.rsm.Submit(*args)

	if err != rpc.OK {
		reply.Err = err
		return
	}

	*reply = result.(shardrpc.DeleteShardReply)
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

	// Your code here
	kv.entries = make(map[string]Item)
	if gid == shardcfg.Gid1 {
		for i := range kv.shards {
			kv.shards[i].State = Serving
			kv.shards[i].CfgNum = shardcfg.NumFirst
		}
	}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}
