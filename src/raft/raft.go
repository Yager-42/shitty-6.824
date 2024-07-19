package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// 节点的角色
type status int

// 投票的状态
type voteState int

// 追加日志的状态
type appendEntriesState int

// 全局心跳超时时间
var heartBeatTime = 120 * time.Millisecond

// 节点角色
const (
	Follower status = iota
	Candidate
	Leader
)

// 投票状态
const (
	Normal voteState = iota
	Killed
	Expire
	Voted
)

// 日志的状态
const (
	AppNormal    appendEntriesState = iota // 追加正常
	AppOutOfDate                           // 追加过时
	AppKilled                              // Raft程序终止
	AppCommited                            // 追加的日志已经提交 (2B
	Mismatch                               // 追加不匹配 (2B
)

// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 所有节点都有的不变量
	// 当前的term
	currentTerm int
	// 当前term投给了谁
	votedFor int
	// 日志数组
	logs []LogEntry

	// 所有节点都有的变量
	// 状态机中已知的被提交的日志条目的索引值(初始化为0，持续递增)
	committedIdx int
	// 最后一个被追加到状态机日志的索引值
	lastApplied int

	// leader有的变量
	// 对于每一个server，需要发送给他下一个日志条目的索引值（初始化为leader日志index+1,那么范围就对标len）
	nextIndex []int
	// 对于每一个server，已经复制给该server的最后日志条目下标
	matchIndex []int

	// 由自己追加的:
	status   status        // 该节点是什么角色（状态）
	overtime time.Duration // 设置超时时间，200-400ms
	timer    *time.Ticker  // 每个节点中的计时器

	applyChan chan ApplyMsg // 日志都是存在这里client取（2B
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm

	//fmt.Println("the peer[", rf.me, "] in term [", term, "] state is:", rf.status)

	if rf.status == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //	需要竞选的人的任期
	CandidateId  int // 需要竞选的人的Id
	LastLogIndex int // 竞选人日志条目最后索引
	LastLogTerm  int // 候选人最后日志条目的任期号
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int       // 投票方的term，如果竞选者比自己还低就改为这个
	VoteGranted bool      // 是否投票给了该竞选人
	VoteState   voteState // 投票状态
}

// AppendEntriesArgs 由leader复制log条目，也可以当做是心跳连接，注释中的rf为leader节点
type AppendEntriesArgs struct {
	Term         int        // leader的任期
	LeaderId     int        // leader自身的ID
	PrevLogIndex int        // 预计要从哪里追加的index，因此每次要比当前的len(logs)多1 args初始化为：rf.nextIndex[i] - 1
	PrevLogTerm  int        // 追加新的日志的任期号(这边传的应该都是当前leader的任期号 args初始化为：rf.currentTerm
	Entries      []LogEntry // 预计存储的日志（为空时就是心跳连接）
	LeaderCommit int        // leader的commit index指的是最后一个被大多数机器都复制的日志Index
}

type AppendEntriesReply struct {
	Term        int                // leader的term可能是过时的，此时收到的Term用于更新他自己
	Success     bool               //	如果follower与Args中的PreLogIndex/PreLogTerm都匹配才会接过去新的日志（追加），不匹配直接返回false
	AppState    appendEntriesState // 追加状态
	UpNextIndex int                //  用于更新请求节点的nextIndex[i]
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 当前节点宕机
	if rf.killed() {
		reply.VoteState = Killed
		reply.Term = -1
		reply.VoteGranted = false
		//fmt.Printf("[	    func-RequestVote-rf(%+v)		] : return %v \n", rf.me, reply)
		return
	}

	// 当前节点更新
	if args.Term < rf.currentTerm {
		reply.VoteState = Expire
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		//fmt.Printf("[	    func-RequestVote-rf(%+v)		] : return %v for expired beacuse args.Term %v < rf.currentTerm %v\n", rf.me, reply, args.Term, rf.currentTerm)
	} else if args.Term > rf.currentTerm { // 当前节点更旧
		rf.status = Follower
		rf.currentTerm = args.Term

		// 测试是否要给票

		// 获取当前的term和log index
		curLogIdx := len(rf.logs) - 1
		curlogTerm := 0

		if curLogIdx >= 0 {
			curlogTerm = rf.logs[curLogIdx].Term
		}

		// 不符合给票条件
		if len(rf.logs) > 0 && args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIndex < curLogIdx || args.LastLogTerm < curlogTerm {
			reply.VoteState = Expire
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			//fmt.Printf("[	    func-RequestVote-rf(%+v)		] : return %v for expired beacuse args.LastLogIndex %v < curLogIdx %v or args.LastLogTerm %v < curlogTerm %v\n", rf.me, reply, args.LastLogIndex, curLogIdx, args.LastLogTerm, curlogTerm)
			return
		}

		// 给票
		rf.votedFor = args.CandidateId
		reply.VoteState = Normal
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.timer.Reset(rf.overtime)

		//fmt.Printf("[	    func-RequestVote-rf(%v)		] : voted rf[%v]\n", rf.me, rf.votedFor)

	} else { // 相同term，票给了别人了
		reply.VoteState = Voted
		reply.VoteGranted = false

		// 投票的和来问的不是一个人，不刷新timer
		if rf.votedFor != args.CandidateId {
			return
		} else {
			// 投票的和来问的是同一个人，继续维持自己的follwer身份
			rf.status = Follower
			rf.timer.Reset(rf.overtime)
		}
	}
	//fmt.Printf("[	    func-RequestVote-rf(%+v)		] : return %v \n", rf.me, reply)
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteNums *int) bool {
	if rf.killed() {
		return false
	}

	//fmt.Printf("[	sendRequestVote(%v) ] : send a RequestVote to %v at term %v\n", rf.me, server, rf.currentTerm)

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	for !ok {
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	switch reply.VoteState {
	case Expire:
		rf.status = Follower
		rf.timer.Reset(rf.overtime)
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
		}
	case Normal, Voted:
		if reply.VoteGranted && reply.Term == rf.currentTerm && *voteNums <= (len(rf.peers)/2) {
			*voteNums++
		}

		if *voteNums >= len(rf.peers)/2+1 {
			*voteNums = 0

			if rf.status == Leader {
				return ok
			}

			rf.status = Leader
			rf.nextIndex = make([]int, len(rf.peers))
			for i, _ := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.logs) + 1
			}
			rf.timer.Reset(heartBeatTime)

			//fmt.Printf("[	sendRequestVote-func-rf(%v)		] be a leader\n", rf.me)
		}
	case Killed:
		return false
	}

	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if rf.killed() {
		return index, term, false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status != Leader {
		return index, term, false
	}

	// 初始化日志条目。并进行追加
	appendLog := LogEntry{Term: rf.currentTerm, Command: command}
	rf.logs = append(rf.logs, appendLog)
	index = len(rf.logs)
	term = rf.currentTerm

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	atomic.StoreInt32(&rf.dead, 1)
	rf.mu.Lock()
	rf.timer.Stop()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.timer.C:
			if rf.killed() {
				return
			}
			rf.mu.Lock()

			switch rf.status {
			case Follower:
				rf.status = Candidate
				//fmt.Printf("[	ticker(%v) ] : become candidate from fllower \n", rf.me)
				fallthrough
			case Candidate:
				rf.currentTerm += 1
				rf.votedFor = rf.me
				votedNums := 1

				// 每轮选举开始时，重新设置选举超时
				rf.overtime = time.Duration(150+rand.Intn(200)) * time.Millisecond // 随机产生200-400ms
				rf.timer.Reset(rf.overtime)

				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}

					votedArgs := RequestVoteArgs{
						Term:         rf.currentTerm,
						CandidateId:  rf.me,
						LastLogIndex: len(rf.logs) - 1,
						LastLogTerm:  0}

					if (len(rf.logs)) > 0 {
						votedArgs.LastLogTerm = rf.logs[len(rf.logs)-1].Term
					}

					voteReply := RequestVoteReply{}

					go rf.sendRequestVote(i, &votedArgs, &voteReply, &votedNums)

				}
			case Leader:
				appendNums := 1
				rf.timer.Reset(heartBeatTime)

				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					appendEntriesArgs := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: 0,
						PrevLogTerm:  0,
						Entries:      nil,
						LeaderCommit: rf.committedIdx,
					}

					appendEntriesArgs.Entries = rf.logs[rf.nextIndex[i]-1:]

					if rf.nextIndex[i] > 0 {
						appendEntriesArgs.PrevLogIndex = rf.nextIndex[i] - 1
					}

					if appendEntriesArgs.PrevLogIndex > 0 {
						appendEntriesArgs.PrevLogTerm = rf.logs[appendEntriesArgs.PrevLogIndex-1].Term
					}

					appendEntriesReply := AppendEntriesReply{}

					//fmt.Printf("[	ticker(%v) ] : send a appendEntries order to %v\n", rf.me, i)

					go rf.sendAppendEntries(i, &appendEntriesArgs, &appendEntriesReply, &appendNums)
				}
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, appendNums *int) bool {
	if rf.killed() {
		return false
	}
	// paper中5.3节第一段末尾提到，如果append失败应该不断的retries ,直到这个log成功的被store
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for !ok {

		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)

	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//fmt.Printf("[	sendAppendEntries func-rf(%v)	] get reply :%+v from rf(%v)\n", rf.me, reply, server)

	if args.Term != rf.currentTerm {
		//fmt.Printf("[	sendAppendEntries func-rf(%v)	] arg term (%v) not match cur term (%v)\n", rf.me, args.Term, rf.currentTerm)
		return false
	}

	switch reply.AppState {
	case AppKilled:
		{
			//fmt.Printf("[	sendAppendEntries func-rf(%v)	] : has been killed ", rf.me)
			return false
		}
	case AppNormal:
		{
			if reply.Success && *appendNums <= len(rf.peers)/2 {
				*appendNums++
			}

			rf.nextIndex[server] += len(args.Entries)

			// 过半同意，处理leader的提交
			if *appendNums > len(rf.peers)/2 {
				*appendNums = 0

				// 不能在不同的term直接提交之前的term，需要在当前term处理当前的提交时顺带处理之前term的提交
				if len(rf.logs) > 0 && rf.logs[len(rf.logs)-1].Term != rf.currentTerm {
					return false
				}

				for rf.lastApplied < len(rf.logs) {
					rf.lastApplied++
					applyMsg := ApplyMsg{
						CommandValid: true,
						Command:      rf.logs[rf.lastApplied-1].Command,
						CommandIndex: rf.lastApplied,
					}
					rf.applyChan <- applyMsg
					rf.committedIdx = rf.lastApplied
					//fmt.Printf("[	sendAppendEntries func-rf(%v)	] commitLog  \n", rf.me)
				}
			}
			//fmt.Printf("[	sendAppendEntries func-rf(%v)	] rf.log :%+v  ; rf.lastApplied:%v\n", rf.me, rf.logs, rf.lastApplied)
			return true
		}
	case Mismatch:
		{ // 怎么处理term不同的mismatch
			if reply.Term != rf.currentTerm {
				return false
			}
			rf.nextIndex[server] = reply.UpNextIndex
		}
	case AppOutOfDate:
		{
			rf.status = Follower
			rf.votedFor = -1
			rf.timer.Reset(rf.overtime)
			rf.currentTerm = reply.Term

			//fmt.Printf("[	sendAppendEntries func-rf(%v)	] : become folloer\n ", rf.me)
		}

	case AppCommited:
		rf.nextIndex[server] = reply.UpNextIndex
	}
	return ok

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf("[	AppendEntries func-rf(%v) 	] arg:%+v,------ rf.logs:%v \n", rf.me, args, rf.logs)

	if rf.killed() {
		reply.AppState = AppKilled
		reply.Term = -1
		reply.Success = false
	}

	//  args.Term < rf.currentTerm:出现网络分区，args的任期，比当前raft的任期还小，说明args之前所在的分区已经OutOfDate 2A
	if args.Term < rf.currentTerm {
		reply.AppState = AppOutOfDate
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 处理confict
	// paper:Reply false if log doesn’t contain an entry at prevLogIndex,whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex > 0 && (len(rf.logs) < args.PrevLogIndex || rf.logs[args.PrevLogIndex-1].Term != args.PrevLogTerm) {
		reply.AppState = Mismatch
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.UpNextIndex = rf.lastApplied + 1
		return
	}

	// 如果当前节点提交的Index比传过来的还高，说明当前节点的日志已经超前,需返回过去
	if args.PrevLogIndex != -1 && rf.lastApplied > args.PrevLogIndex {
		reply.AppState = AppCommited
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.UpNextIndex = rf.lastApplied + 1
		return
	}

	// 对当前的rf进行ticker重置
	rf.currentTerm = args.Term
	rf.votedFor = args.LeaderId
	rf.status = Follower
	rf.timer.Reset(rf.overtime)

	// 对返回的reply进行赋值
	reply.AppState = AppNormal
	reply.Term = rf.currentTerm
	reply.Success = true

	// 处理日志
	if args.Entries != nil {
		rf.logs = rf.logs[:args.PrevLogIndex]
		rf.logs = append(rf.logs, args.Entries...)
	}

	// 同步当前节点
	for rf.lastApplied < args.LeaderCommit {
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[rf.lastApplied-1].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyChan <- applyMsg
		rf.committedIdx = rf.lastApplied

		//fmt.Printf("[	AppendEntries func-rf(%v)	] commitLog  \n", rf.me)
	}

	return
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.applyChan = applyCh

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 0)

	rf.committedIdx = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.status = Follower
	rf.overtime = time.Duration(150+rand.Intn(200)) * time.Millisecond // 随机产生150-350ms
	rf.timer = time.NewTicker(rf.overtime)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	//fmt.Printf("[ 	Make-func-rf(%v) 	]:  %v\n", rf.me, rf.overtime)

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
