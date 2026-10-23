package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Entry struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	mu      sync.Mutex
	entries map[string]Entry
	// Your definitions here.
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		entries: make(map[string]Entry),
	}
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	key := args.Key

	kv.mu.Lock()
	entry, ok := kv.entries[key]
	kv.mu.Unlock()

	if ok {
		reply.Err = rpc.OK
		reply.Value = entry.Value
		reply.Version = entry.Version
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

// Put update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	version := args.Version

	kv.mu.Lock()
	defer kv.mu.Unlock()

	entry, ok := kv.entries[args.Key]
	if !ok {
		if args.Version != 0 {
			reply.Err = rpc.ErrNoKey
			return
		}

		newEntry := Entry{
			Version: 1,
			Value:   args.Value,
		}
		kv.entries[args.Key] = newEntry
		reply.Err = rpc.OK
	} else {
		curVersion := entry.Version
		if curVersion != version {
			reply.Err = rpc.ErrVersion
			return
		}

		entry.Value = args.Value
		entry.Version += 1

		kv.entries[args.Key] = entry
		reply.Err = rpc.OK
	}
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
