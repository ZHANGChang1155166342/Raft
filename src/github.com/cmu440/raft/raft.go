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
const MIN_NORMAL = 250
const MAX_LEADER = 249
const MIN_LEADER = 201

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

	// Your data here (2A, 2B).
	// Look at the Raft paper's Figure 2 for a description of what
	// state a Raft peer should maintain
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

	applyHelperCh chan ApplyMsg
	applyCh chan ApplyMsg

	timerTicker chan bool
	timeReset   chan int
	voteCount   int

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
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      Log
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
			(rf.lastLoggedTerm == args.LastLogTerm && rf.lastLoggedIndex <= args.LastLogTerm) {
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
			(rf.lastLoggedTerm == args.LastLogTerm && rf.lastLoggedIndex <= args.LastLogTerm) {
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
		rf.mux.Unlock()
		return
	}
	if args.Entries.Command == nil {
		if args.Term >= rf.currentTerm {
			rf.role = Follower
			rf.timeReset <- MAX_NORMAL
			rf.votedFor = -1
			rf.voteCount = 0

			reply.Term = args.Term
			reply.Success = true
		} else {
			reply.Term = rf.currentTerm
			reply.Success = false
		}
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
	//ok := rf.peers[peer].Call("Raft.RequestVote", args, reply)
	ok := false
	for !ok { // follower, candidate, leader
		ok = rf.peers[peer].Call("Raft.RequestVote", args, reply)
		if !ok {
			continue
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
				if i == rf.me {
					continue
				}
				rf.nextIndex[i] = rf.lastLoggedIndex + 1
				rf.matchIndex[i] = 0
			}
		}
		rf.mux.Unlock()
	}

	return ok
}

func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := false
	for !ok {
		ok = rf.peers[peer].Call("Raft.AppendEntries", args, reply)
	}
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
	index := -1
	term := -1

	rf.mux.Lock()
	isLeader := rf.role == Leader
	if !isLeader {
		rf.mux.Unlock()
		return  index, term, isLeader
	}
	// is a Leader
	// Your code here (2B)
	return index, term, isLeader
}

//
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
	// Your code here, if desired
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
		applyHelperCh:   make(chan ApplyMsg),
		role:            Follower,
		voteCount:       0,
		population:      len(peers),
		lastLoggedIndex: 0,
		lastLoggedTerm:  0,

		commitIndex:     0,
		lastApplied:     0,
		nextIndex:       make([]int, len(peers)),
		matchIndex:      make([]int, len(peers)),
		log:make([]Log, 1),
	}

	go rf.timerRoutine()
	go rf.mainRoutine()
	go rf.applyRoutine()
	// Your initialization code here (2A, 2B)
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
				for i := 0; i < rf.population; i++ {
					if i == rf.me {
						continue
					}
					go rf.sendAppendEntries(i,
						&AppendEntriesArgs{
							Term:         rf.currentTerm,
							LeaderId:     rf.me,
							PrevLogIndex: 0,   // problem
							PrevLogTerm:  0,   //problem
							Entries:      Log{Command:nil}, // problem
							LeaderCommit: 0,   // problem
						}, &AppendEntriesReply{})
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

func (rf *Raft) applyRoutine() {
	for {
		select {
		case msg := <- rf.applyHelperCh:
			rf.applyCh <- msg
		}
	}
}

// Helper Functions
func randInt(min, max int) int {
	return min + rand.Intn(max-min)
}
