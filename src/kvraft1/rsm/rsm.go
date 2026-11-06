package rsm

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"

	crand "crypto/rand"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me         int
	OpID       int
	Req        any
	InstanceID [16]byte
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	opID       int
	waitTable  map[int]chan any
	instanceID [16]byte
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
	}
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	if _, err := crand.Read(rsm.instanceID[:]); err != nil {
		panic(err)
	}

	rsm.waitTable = make(map[int]chan any)
	go rsm.DoOpLoop()

	return rsm
}

func (rsm *RSM) DoOpLoop() {
	for {
		msg := <-rsm.applyCh

		if msg.CommandValid {
			// 带类型断言的取值（避免编译器认为是 any 导致取不出 Req 字段）
			op, ok := msg.Command.(Op)
			if ok {
				result := rsm.sm.DoOp(op.Req)

				rsm.mu.Lock()
				channel, exist := rsm.waitTable[op.OpID]
				rsm.mu.Unlock()

				if op.Me == rsm.me && op.InstanceID == rsm.instanceID && exist {
					channel <- result
				}
			}
		}
	}
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	rsm.mu.Lock()

	op := Op{
		Me:         rsm.me,
		OpID:       rsm.opID,
		Req:        req,
		InstanceID: rsm.instanceID,
	}
	channel := make(chan any, 1)
	rsm.waitTable[rsm.opID] = channel
	rsm.opID += 1

	rsm.mu.Unlock()

	defer func() {
		rsm.mu.Lock()
		delete(rsm.waitTable, op.OpID)
		rsm.mu.Unlock()
	}()

	_, term, isLeader := rsm.rf.Start(op)
	if isLeader {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		// 等待处理完毕并定期检查任期变化
		for {
			select {
			// 1. 正常接收到数据就返回
			case result := <-channel:
				return rpc.OK, result
			// 2. 定期检查 term
			case <-ticker.C:
				newTerm, isleader := rsm.rf.GetState()
				if newTerm != term || !isleader {
					return rpc.ErrWrongLeader, nil // i'm dead, try another server.
				}
			}
		}
	}

	return rpc.ErrWrongLeader, nil // i'm dead, try another server.
}
