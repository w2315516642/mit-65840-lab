package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type RaftRole string

const (
	Follower  RaftRole = "Follower"
	Candidate RaftRole = "Candidate"
	Leader    RaftRole = "Leader"
)

type LogEntry struct {
	Command any
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// Persistent states
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile states
	commitIndex int
	lastApplied int

	// Volatile states in Leader
	nextIndex  []int
	matchIndex []int

	role     RaftRole
	deadline time.Time
	notifyCh []chan struct{}

	applyCh chan raftapi.ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isLeader = rf.role == Leader
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int
	CandidateID int

	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. 判断 term 是否合法
	if args.Term > rf.currentTerm {
		rf.advanceTerm(args.Term)
	} else if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 2. 判断是否能给这个投票
	reply.Term = rf.currentTerm

	// 是否有票
	haveVote := rf.votedFor == -1 || rf.votedFor == args.CandidateID
	// 候选人是否有更新的日志
	newerLog := false
	lastLogTerm := rf.log[len(rf.log)-1].Term
	if args.LastLogTerm > lastLogTerm {
		newerLog = true
	} else if args.LastLogTerm == lastLogTerm {
		newerLog = args.LastLogIndex >= len(rf.log)-1
	}
	canVote := haveVote && newerLog

	if canVote {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.resetDeadline()
	} else {
		reply.VoteGranted = false
	}
}

type AppendEntriesArgs struct {
	Term     int
	LeaderID int

	PrevLogIndex int // 发送日志的前一条日志的 index
	PrevLogTerm  int // 发送日志的前一条日志的 term

	Entries []LogEntry

	LeaderCommit int // Leader 当前的提交日志 index
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. 检查 Term 是否合法
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		rf.advanceTerm(args.Term)
		rf.resetDeadline()
	} else {
		if rf.role == Candidate {
			rf.role = Follower
		}
		rf.resetDeadline()
	}
	reply.Term = rf.currentTerm

	// 2. 查看前缀日志是否匹配
	if args.PrevLogIndex < len(rf.log) {
		logTerm := rf.log[args.PrevLogIndex].Term
		if logTerm != args.PrevLogTerm {
			reply.Success = false
			return
		}
	} else {
		reply.Success = false
		return
	}

	// 3. 逐个添加日志
	i := 0
	for i < len(args.Entries) {
		logIndex := args.PrevLogIndex + 1 + i
		exist := logIndex < len(rf.log)
		// 3.3 不存在这个 index，直接追加
		if !exist {
			break
		}

		logTerm := rf.log[logIndex].Term
		// 3.1 当前日志 index 和 term 相同，跳过
		if logTerm == args.Entries[i].Term {
			i++
			continue
			// 3.2 index 相同，term 不同，删掉当前日志后追加
		} else {
			// 不包括这个有差异的 log
			rf.log = rf.log[:logIndex]
			break
		}
	}
	if i < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[i:]...)
	}

	// 4. 推进 commitIndex
	newCommitIndex := min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
	if newCommitIndex > rf.commitIndex {
		rf.commitIndex = newCommitIndex
		select {
		case rf.notifyCh[rf.me] <- struct{}{}:
		default:
		}
	}

	reply.Success = true

	// if len(args.Entries) == 0 {
	// 	reply.Success = true
	// 	return
	// }
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isLeader = rf.role == Leader
	if !isLeader {
		return -1, term, false
	}
	// 有占位条目在 0 位置，因此这里 len 直接就是新 cmd 的 index
	index = len(rf.log)

	logEntry := LogEntry{
		Command: command,
		Term:    term,
	}
	rf.log = append(rf.log, logEntry)
	rf.matchIndex[rf.me] = index
	rf.advanceCommit()

	// 通知开始复制日志
	for id := range rf.peers {
		if id == rf.me {
			continue
		}
		select {
		case rf.notifyCh[id] <- struct{}{}:
		default: // 通道满了就只更新日志，不复制给 Follower 了
		}
	}

	return index, term, isLeader
}

// 必须得调用方持锁，传进来的 term 必须大于当前 term
func (rf *Raft) advanceTerm(term int) {
	rf.currentTerm = term
	// change to Follower
	rf.votedFor = -1
	rf.role = Follower
}

// 外部持锁
func (rf *Raft) advanceCommit() {
	startIdx := rf.matchIndex[rf.me]
	threshold := len(rf.peers)/2 + 1
	lo := rf.commitIndex
	for startIdx > lo {
		cnt := 0

		termValid := rf.log[startIdx].Term == rf.currentTerm
		if termValid {
			for id := range rf.peers {
				indexValid := rf.matchIndex[id] >= startIdx
				if indexValid {
					cnt++
				}
			}
		}

		if cnt >= threshold {
			// 提交
			rf.commitIndex = startIdx
			select {
			case rf.notifyCh[rf.me] <- struct{}{}:
			default:
			}
			break
		}

		startIdx--
	}
}

func (rf *Raft) heartbeatLoop(server int, term int) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// 检查是否能够发送 AE 请求
	valid := func() bool {
		valid := rf.currentTerm == term && rf.role == Leader
		return valid
	}

	// 心跳循环
	for {

		rf.mu.Lock()
		if !valid() {
			rf.mu.Unlock()
			return
		}

		startIdx := rf.nextIndex[server]
		endIdx := len(rf.log) - 1
		logs := make([]LogEntry, len(rf.log)-startIdx)
		copy(logs, rf.log[startIdx:])

		args := AppendEntriesArgs{
			Term:         term,
			LeaderID:     rf.me,
			PrevLogIndex: startIdx - 1,
			PrevLogTerm:  rf.log[startIdx-1].Term,
			Entries:      logs,
			LeaderCommit: rf.commitIndex,
		}
		var reply AppendEntriesReply
		rf.mu.Unlock()

		ok := rf.sendAppendEntries(server, &args, &reply)

		if ok {
			rf.mu.Lock()
			// 1. 处理具有更大 term 的回复
			if reply.Term > rf.currentTerm {
				rf.advanceTerm(reply.Term)
				rf.mu.Unlock()
				return
			}
			// 2. 处理正常追加情况
			if valid() {
				if reply.Success {
					rf.matchIndex[server] = endIdx
					rf.nextIndex[server] = endIdx + 1

					rf.advanceCommit()
				} else {
					rf.nextIndex[server] -= 1

					rf.mu.Unlock()
					continue
				}
			}
			rf.mu.Unlock()
		}

		// 刚进来的时候立即发一次心跳，然后等待事件触发
		select {
		// 计时器事件
		case <-ticker.C:
		// 主动发送 AE 请求事件
		case <-rf.notifyCh[server]:
		}

		// 检查是否还需要发送心跳
		// rf.mu.Lock()
		// need := rf.currentTerm == args.Term && rf.role == Leader
		// if !need {
		// 	rf.mu.Unlock()
		// 	return
		// }
		// rf.mu.Unlock()

		// // 发送心跳请求
		// var reply AppendEntriesReply
		// ok := rf.sendAppendEntries(server, &args, &reply)
		//
		// // 处理回复
		// if ok {
		// 	rf.mu.Lock()
		// 	// 1. 处理具有更大 term 的回复
		// 	if reply.Term > rf.currentTerm {
		// 		rf.advanceTerm(reply.Term)
		// 		rf.mu.Unlock()
		// 		return
		// 	}
		// 	rf.mu.Unlock()
		// }
		//
		// // 2. 无其它情况下睡眠即可
		// time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

// 必须得调用方持锁
func (rf *Raft) becomeLeader() {
	rf.role = Leader

	for id := range rf.peers {
		if id == rf.me {
			rf.matchIndex[id] = len(rf.log) - 1
			continue
		}

		rf.matchIndex[id] = 0
		rf.nextIndex[id] = len(rf.log)

		go func(server int, term int) {
			rf.heartbeatLoop(server, term)
		}(id, rf.currentTerm)
	}
}

func (rf *Raft) tryStartElection() {
	rf.mu.Lock()

	timeout := !time.Now().Before(rf.deadline)
	isLeader := rf.role == Leader
	startElection := !isLeader && timeout
	if !startElection {
		rf.mu.Unlock()
		return
	}

	rf.role = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.resetDeadline()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	rf.mu.Unlock()

	numVotes := 1
	if numVotes > len(rf.peers)/2 {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		rf.becomeLeader()
		return
	}
	// Request vote from other ClientEnd
	for id := range rf.peers {
		if id == rf.me {
			continue
		}

		go func(server int) {
			var reply RequestVoteReply
			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 1. 处理来自更大 term 的回复
			if rf.currentTerm < reply.Term {
				rf.advanceTerm(reply.Term)
				return
			}

			// 2. 处理小于等于当前 term 的 reply
			termValid := rf.currentTerm == args.Term // currentTerm 等于发送请求时的 term
			roleValid := rf.role == Candidate
			valid := termValid && roleValid
			if valid && reply.VoteGranted {
				numVotes += 1
			}

			// 3. 如果票数足够，则转为 Leader
			voteEnough := numVotes > len(rf.peers)/2
			if valid && voteEnough {
				rf.becomeLeader()
			}
		}(id)
	} // End go func
}

// 必须得调用方持锁
func (rf *Raft) resetDeadline() {
	ms := 200 + (rand.Int63() % 300)
	rf.deadline = time.Now().Add(time.Duration(ms) * time.Millisecond)
}

func (rf *Raft) ticker() {
	for {

		// Your code here (3A)
		// Check if a leader election should be started.

		rf.tryStartElection()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applyLoop() {
	for {
		<-rf.notifyCh[rf.me]

		for {
			rf.mu.Lock()

			valid := rf.lastApplied < rf.commitIndex
			if !valid {
				rf.mu.Unlock()
				break
			}

			log := rf.log[rf.lastApplied+1]

			applyMsg := raftapi.ApplyMsg{
				CommandValid:  true,
				Command:       log.Command,
				CommandIndex:  rf.lastApplied + 1,
				SnapshotValid: false,
				Snapshot:      []byte{},
				SnapshotTerm:  0,
				SnapshotIndex: 0,
			}

			rf.mu.Unlock()

			// 等待信息发送完成
			rf.applyCh <- applyMsg

			rf.mu.Lock()
			rf.lastApplied++
			rf.mu.Unlock()
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg,
) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.role = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.resetDeadline()

	// 初始化 log 相关项
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Command: nil, Term: 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))

	rf.notifyCh = make([]chan struct{}, len(peers))
	for i := range rf.notifyCh {
		rf.notifyCh[i] = make(chan struct{}, 1)
	}

	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyLoop()

	return rf
}
