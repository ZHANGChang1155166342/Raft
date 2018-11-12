//
// raft.go
// =======
// Write your code in this file
// We will use the original version of all other
// files for testing
//

package raft

//
// API
// ===
// This is an outline of the API that your raft implementation should
// expose.
//
// rf = Make(...)
//   Create a new Raft peer.
//
// rf.Start(command interface{}) (index, term, isleader)
//   Start agreement on a new log entry
//
// rf.GetState() (me, term, isLeader)
//   Ask a Raft peer for "me" (see line 58), its current term, and whether it thinks it
//   is a leader
//
// ApplyMsg
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (e.g. tester) on the
//   same peer, via the applyCh channel passed to Make()
//

import (
	"github.com/cmu440/rpc"
	"math/rand"
	"sync"
	"time"
)

// Constant
const MAX_NORMAL = 400
const MIN_NORMAL = 300
const MAX_LEADER = 299
const MIN_LEADER = 250

const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

//
// ApplyMsg
// ========
//
// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same peer, via the applyCh passed to Make()
//
type ApplyMsg struct {
	Index   int
	Command interface{}
}

type Log struct {
	Command interface{}
	Term    int
}

type Count struct {
	mux   sync.Mutex
	count int
}

//
// Raft struct
// ===========
//
// A Go object implementing a single Raft peer
//
type Raft struct {
	mux   sync.Mutex       // Lock to protect shared access to this peer's state
	peers []*rpc.ClientEnd // RPC end points of all peers
	me    int              // this peer's index into peers[]

	// Persistent state
	currentTerm int
	votedFor    int
	log         []Log
	// Volatile State
	commitIndex int
	lastApplied int
	//Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	//applyHelperCh chan int
	applyCh chan ApplyMsg

	timerTicker chan bool
	timeReset   chan int
	voteCount   int
	commitCount int

	close           []bool
	count           int
	role            int
	population      int
	lastLoggedIndex int
	lastLoggedTerm  int
}

//
// GetState()
// ==========
//
// Return "me", current term and whether this peer
// believes it is the leader
//
func (rf *Raft) GetState() (int, int, bool) {

	var me int
	var term int
	var isleader bool

	rf.mux.Lock()
	isleader = rf.role == Leader
	me = rf.me
	term = rf.currentTerm
	rf.mux.Unlock()

	return me, term, isleader
}

//
// RequestVoteArgs
// ===============
//
// Example RequestVote RPC arguments structure
//
// Please note
// ===========
// Field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B)
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// RequestVoteReply
// ================
//
// Example RequestVote RPC reply structure.
//
// Please note
// ===========
// Field names must start with capital letters!
//
//
type RequestVoteReply struct {
	// Your data here (2A)
	Term        int
	VoteGranted bool
}

// AppendEntriesArgs
type AppendEntriesArgs struct {
	mux          sync.Mutex
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

// AppendEntriesReply
type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// RequestVote
// ===========
//
// Example RequestVote RPC handler
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mux.Lock()
	if rf.role == Follower {
		rf.timeReset <- MAX_NORMAL
	}
	if args.Term < rf.currentTerm { // follower, candidate, leader
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mux.Unlock()
		return
	}
	if args.Term == rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		if rf.lastLoggedTerm < args.LastLogTerm ||
			(rf.lastLoggedTerm == args.LastLogTerm && rf.lastLoggedIndex <= args.LastLogIndex) {
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			rf.votedFor = args.CandidateId
		}
		rf.mux.Unlock()
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.voteCount = 0
		rf.timeReset <- MIN_NORMAL
		if rf.lastLoggedTerm < args.LastLogTerm ||
			(rf.lastLoggedTerm == args.LastLogTerm && rf.lastLoggedIndex <= args.LastLogIndex) {
			rf.votedFor = args.CandidateId
			reply.Term = args.Term
			reply.VoteGranted = true
			rf.mux.Unlock()
			return
		}
	}
	reply.VoteGranted = false
	reply.Term = args.Term
	rf.mux.Unlock()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mux.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		//fmt.Println("Peer " + strconv.Itoa(rf.me) + " False Term " + strconv.Itoa(rf.currentTerm) + " " +strconv.Itoa(args.Term))
		rf.mux.Unlock()
		return
	}
	rf.timeReset <- MAX_NORMAL
	if args.Entries == nil {
		rf.role = Follower // Problem CommitIndex
		//rf.timeReset <- MAX_NORMAL
		rf.votedFor = -1
		rf.voteCount = 0
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(rf.lastLoggedIndex, args.LeaderCommit)
			for rf.lastApplied < rf.commitIndex && rf.log[rf.commitIndex].Term == rf.currentTerm {
				rf.lastApplied += 1
				rf.applyCh <- ApplyMsg{
					Command: rf.log[rf.lastApplied].Command,
					Index:   rf.lastApplied,
				}
			}
		}
		reply.Term = args.Term
		reply.Success = true
		rf.mux.Unlock()
		return
	} else {
		rf.currentTerm = args.Term
		if rf.lastLoggedIndex < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
			reply.Term = rf.currentTerm
			rf.mux.Unlock()
			return
		}
		//if rf.lastLoggedIndex > args.PrevLogIndex && rf.log[args.PrevLogIndex+1].Term == args.Entries[0].Term {
		//	//reply.Success = true
		//	//reply.Term = rf.currentTerm
		rf.log = rf.log[:args.PrevLogIndex+1]
		rf.lastLoggedIndex = args.PrevLogIndex
		//	//rf.mux.Unlock()
		//	//return
		//}
		//if rf.lastLoggedIndex > args.PrevLogIndex && rf.log[args.PrevLogIndex+1].Term != args.Entries[0].Term {
		//	rf.log = rf.log[:args.PrevLogIndex+1]
		//	rf.lastLoggedIndex = args.PrevLogIndex
		//}
		for _, entry := range args.Entries {
			rf.log = append(rf.log, entry)
			rf.lastLoggedIndex += 1
		}
		rf.lastLoggedTerm = rf.currentTerm

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(rf.lastLoggedIndex, args.LeaderCommit)
			for rf.lastApplied < rf.commitIndex && rf.log[rf.commitIndex].Term == rf.currentTerm {
				rf.lastApplied += 1
				rf.applyCh <- ApplyMsg{
					Command: rf.log[rf.lastApplied].Command,
					Index:   rf.lastApplied,
				}
			}
		}
		//fmt.Println(rf.log)
		reply.Success = true
		reply.Term = rf.currentTerm
		rf.mux.Unlock()
		return
	}
}

//
// sendRequestVote
// ===============
//
// Example code to send a RequestVote RPC to a peer
//
// peer int -- index of the target peer in
// rf.peers[]
//
// args *RequestVoteArgs -- RPC arguments in args
//
// reply *RequestVoteReply -- RPC reply
//
// The types of args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers)
//
// The rpc package simulates a lossy network, in which peers
// may be unreachable, and in which requests and replies may be lost
//
// Call() sends a request and waits for a reply
//
// If a reply arrives within a timeout interval, Call() returns true;
// otherwise Call() returns false
//
// Thus Call() may not return for a while
//
// A false return can be caused by a dead peer, a live peer that
// can't be reached, a lost request, or a lost reply
//
// Call() is guaranteed to return (perhaps after a delay)
// *except* if the handler function on the peer side does not return
//
// Thus there
// is no need to implement your own timeouts around Call()
//
// Please look at the comments and documentation in ../rpc/rpc.go
// for more details
//
// If you are having trouble getting RPC to work, check that you have
// capitalized all field names in the struct passed over RPC, and
// that the caller passes the address of the reply struct with "&",
// not the struct itself
//
func (rf *Raft) sendRequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := false
	for !ok { // follower, candidate, leader
		ok = rf.peers[peer].Call("Raft.RequestVote", args, reply)
		if !ok {
			//fmt.Println("Call Failed")
			//continue
			return ok
		}
		rf.mux.Lock()
		if rf.currentTerm == reply.Term && reply.VoteGranted {
			rf.voteCount += 1
		}
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			rf.timeReset <- MIN_NORMAL
			rf.role = Follower
			rf.votedFor = -1
			rf.voteCount = 0

			rf.mux.Unlock()
			return ok
		}
		if (rf.voteCount > (len(rf.peers) / 2)) && (rf.role == Candidate) {
			rf.timeReset <- MIN_LEADER
			rf.voteCount = 0
			rf.role = Leader
			for i := 0; i < rf.population; i++ {
				rf.nextIndex[i] = rf.lastLoggedIndex + 1
				rf.matchIndex[i] = 0
			}
		}
		rf.mux.Unlock()
	}
	return ok
}

//func (rf *Raft) sendAppendEntriesHeartBeat(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
//	ok := false
//	for {
//		ok = rf.peers[peer].Call("Raft.AppendEntries", args, reply)
//		if !ok {
//			return ok
//		}
//		rf.mux.Lock() //Problem
//		if reply.Term > rf.currentTerm {
//			rf.role = Follower
//			rf.currentTerm = reply.Term
//			rf.votedFor = -1
//			rf.voteCount = 0
//
//			rf.timeReset <- MAX_NORMAL
//			rf.mux.Unlock()
//			return ok
//		}
//		rf.mux.Unlock()
//		return ok
//	}
//}

func (rf *Raft) sendAppendEntriesNormal(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply, count *Count) bool {
	ok := false
	//for {
		args.mux.Lock()
		ok = rf.peers[peer].Call("Raft.AppendEntries", args, reply)
		//fmt.Println("Running " + strconv.Itoa(count.count) + " pos1")
		args.mux.Unlock()
		if !ok {
			//continue
			return ok
		}
		rf.mux.Lock()
		//if rf.close[count.count] {
		//	rf.mux.Unlock()
		//	fmt.Println("Routine:" + strconv.Itoa(count.count) + " is Down")
		//	return ok
		//}
		if rf.role != Leader {
			//fmt.Println("I am not Leader anymore")
			rf.mux.Unlock()
			return ok
		}
		if reply.Term > rf.currentTerm {
			//fmt.Println("Catch Term Error")
			rf.timeReset <- MAX_NORMAL
			rf.role = Follower
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.voteCount = 0
			rf.mux.Unlock()
			return ok
		}
		if !reply.Success {
			args.PrevLogIndex -= 1
			rf.nextIndex[peer] = args.PrevLogIndex + 1
		} else {
			args.PrevLogIndex += len(args.Entries)
			rf.nextIndex[peer] = args.PrevLogIndex + 1
			rf.matchIndex[peer] = rf.nextIndex[peer] - 1
		}
		if rf.lastLoggedIndex >= rf.nextIndex[peer] {
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			args.Entries = rf.log[args.PrevLogIndex+1:]
			args.LeaderCommit = rf.commitIndex
			args.Term = rf.currentTerm
			rf.mux.Unlock()
			return ok
		}
		counter := 0
		max := 0
		for _, entry := range rf.matchIndex {
			if entry > max {
				max = entry
			}
		}
		for _, entry := range rf.matchIndex {
			if entry == max {
				counter += 1
			}
		}
		if counter > rf.population / 2 {
			rf.commitIndex = max
			//rf.commitIndex = rf.lastLoggedIndex
			for rf.lastApplied < rf.commitIndex && rf.log[rf.commitIndex].Term == rf.currentTerm {
				rf.lastApplied += 1
				//fmt.Println("*****************************************")
				//fmt.Println("Server:" + strconv.Itoa(rf.me) + " apply ")
				//fmt.Println(rf.log[rf.lastApplied].Command)
				//fmt.Println("*****************************************")
				rf.applyCh <- ApplyMsg{
					Command: rf.log[rf.lastApplied].Command,
					Index:   rf.lastApplied,
				}
			}
		}
		//fmt.Println("*****************")
		//fmt.Println("Peer:")
		//fmt.Println(rf.log)
		//fmt.Println("*****************")
		rf.mux.Unlock()
		//break
	//}
	return ok
}

//
// Start
// =====
//
// The service using Raft (e.g. a k/v peer) wants to start
// agreement on the next command to be appended to Raft's log
//
// If this peer is not the leader, return false
//
// Otherwise start the agreement and return immediately
//
// There is no guarantee that this command will ever be committed to
// the Raft log, since the leader may fail or lose an election
//
// The first return value is the index that the command will appear at
// if it is ever committed
//
// The second return value is the current term
//
// The third return value is true if this peer believes it is
// the leader
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mux.Lock()
	isLeader := rf.role == Leader
	if !isLeader {
		rf.mux.Unlock()
		return -1, -1, isLeader
	}
	//Leader Operations when new Command Come int  Problem
	newLog := Log{Command: command, Term: rf.currentTerm}
	rf.log = append(rf.log, newLog)
	rf.lastLoggedIndex += 1
	rf.lastLoggedTerm = rf.currentTerm
	rf.matchIndex[rf.me] = rf.lastLoggedIndex
	// Send to server parallel
	rf.count += 1
	rf.close = append(rf.close, false)
	//fmt.Println("*****************")
	//fmt.Println("Leader:")
	//fmt.Println(rf.log)
	//fmt.Println("*****************")
	if rf.count > 0 {
		rf.close[rf.count-1] = true
	}
	c := &Count{count: rf.count}
	rf.commitCount = 1
	for i := 0; i < rf.population; i++ {
		if i == rf.me {
			continue
		}
		//fmt.Println(rf.log[rf.nextIndex[i]:])
		go rf.sendAppendEntriesNormal(i, &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[i] - 1,
			PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
			Entries:      rf.log[rf.nextIndex[i]:],
			LeaderCommit: rf.commitIndex,
		}, &AppendEntriesReply{}, c)
	}
	//Return Value
	index := rf.lastLoggedIndex
	term := rf.currentTerm
	rf.mux.Unlock()
	return index, term, isLeader
}

// Kill
// ====
//
// The tester calls Kill() when a Raft instance will not
// be needed again
//
// You are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance
//
func (rf *Raft) Kill() {
}

//
// Make
// ====
//
// The service or tester wants to create a Raft peer
//
// The port numbers of all the Raft peers (including this one)
// are in peers[]
//
// This peer's port is peers[me]
//
// All the peers' peers[] arrays have the same order
//
// applyCh
// =======
//
// applyCh is a channel on which the tester or service expects
// Raft to send ApplyMsg messages
//
// Make() must return quickly, so it should start Goroutines
// for any long-running work
//
func Make(peers []*rpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:           peers,
		me:              me,
		applyCh:         applyCh,
		currentTerm:     0,
		votedFor:        -1,
		timerTicker:     make(chan bool),
		timeReset:       make(chan int),
		role:            Follower,
		voteCount:       0,
		population:      len(peers),
		lastLoggedIndex: 0,
		lastLoggedTerm:  0,
		commitIndex:     0,
		lastApplied:     0,
		nextIndex:       make([]int, len(peers)),
		matchIndex:      make([]int, len(peers)),
		log:             make([]Log, 1),
		close:           make([]bool, 0),
		count:           -1,
		commitCount:     0,
	}
	rf.log[0].Term = 0
	rf.log[0].Command = nil
	go rf.timerRoutine()
	go rf.mainRoutine()
	return rf
}

//  Routines
func (rf *Raft) mainRoutine() {
	for {
		select {
		case <-rf.timerTicker:
			rf.mux.Lock()
			switch rf.role {
			default:
				rf.currentTerm += 1
				rf.role = Candidate
				rf.votedFor = rf.me
				rf.voteCount = 1
				for i := 0; i < rf.population; i++ {
					if i == rf.me {
						continue
					}
					go rf.sendRequestVote(i,
						&RequestVoteArgs{
							Term:         rf.currentTerm,
							CandidateId:  rf.me,
							LastLogIndex: rf.lastLoggedIndex, // problem
							LastLogTerm:  rf.lastLoggedTerm,  // problem
						}, &RequestVoteReply{})
				}
				rf.mux.Unlock()
			case Leader:
				c := &Count{count: rf.count}
				for i := 0; i < rf.population; i++ {
					if i == rf.me {
						continue
					}
					go rf.sendAppendEntriesNormal(i,
						&AppendEntriesArgs{
							Term:         rf.currentTerm,
							LeaderId:     rf.me,
							PrevLogIndex: rf.nextIndex[i] - 1,
							PrevLogTerm:  rf.log[rf.nextIndex[i] - 1].Term,
							Entries:      rf.log[rf.nextIndex[i]:],
							LeaderCommit: rf.commitIndex,
						}, &AppendEntriesReply{}, c)
				}
				rf.mux.Unlock()
			}
		}
	}
}

func (rf *Raft) timerRoutine() {
	max := MAX_NORMAL
	min := MIN_NORMAL
	for {
		randTime := randInt(min, max)
		timer := time.NewTimer(time.Duration(randTime) * time.Millisecond)
		select {
		case reset := <-rf.timeReset:
			if reset < MIN_NORMAL {
				max = MAX_LEADER
				min = MIN_LEADER
			} else {
				max = MAX_NORMAL
				min = MIN_NORMAL
			}
		case <-timer.C:
			rf.timerTicker <- true
		}
	}
}

// Helper Functions
func randInt(min, max int) int {
	return min + rand.Intn(max-min)
}

func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

