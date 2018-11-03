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
	Follower = 0
	Candidate = 1
	Leader = 2
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
	Term int
}

//
// Raft struct
// ===========
//
// A Go object implementing a single Raft peer
//
type Raft struct {
	mux   sync.Mutex        // Lock to protect shared access to this peer's state
	peers []*rpc.ClientEnd  // RPC end points of all peers
	me    int               // this peer's index into peers[]

	// Your data here (2A, 2B).
	// Look at the Raft paper's Figure 2 for a description of what
	// state a Raft peer should maintain
	// Persistent state
	currentTerm int
	votedFor int
	log[] Log // problem
	// Volatile State
	commitIndex int
	lastApplied int

	//Volatile state on leaders
	nextIndex[] int
	matchIndex[] int

	applyCh chan ApplyMsg

	timerTicker chan bool
	timeReset chan int
	//voteCh chan int
	voteCount int

	leaderCh chan int
	candidateCh chan int
	followerCh chan int

	role int
	population int

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
	// Your code here (2A)
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
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
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
	Term int
	VoteGranted bool
}


// AppendEntriesArgs
type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int

	PrevLogTerm int
	Entries[] string // problem
	LeaderCommit int
}

// AppendEntriesReply
type AppendEntriesReply struct {
	Term int
	Success bool
}

//
// RequestVote
// ===========
//
// Example RequestVote RPC handler
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//fmt.Println(strconv.Itoa(args.CandidateId))
	rf.mux.Lock()
	if args.Term < rf.currentTerm {  // follower, candidate, leader
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
	if args.Term == rf.currentTerm { // follower, candidate, leader
		if rf.role == Leader {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			rf.mux.Unlock()
			return
		}
		if rf.votedFor == args.CandidateId || rf.votedFor == -1 { // problem log index check
			//fmt.Println(strconv.Itoa(rf.me) + " Vote for" + strconv.Itoa(args.CandidateId))
			reply.Term = rf.currentTerm
			reply.VoteGranted = true

			rf.votedFor = args.CandidateId
			rf.currentTerm = args.Term
		} else {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		}
	}
	if args.Term > rf.currentTerm {
		rf.role = Follower
		rf.timeReset <- MIN_NORMAL
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.voteCount = 0

		reply.Term = args.Term
		reply.VoteGranted = true
	}
	rf.mux.Unlock()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mux.Lock()
	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.timeReset <- MAX_NORMAL
		rf.votedFor = -1
		rf.voteCount = 0
		reply.Term = args.Term
		reply.Success = true
	}
	rf.mux.Unlock()
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
	ok := rf.peers[peer].Call("Raft.RequestVote", args, reply)
	if ok {  // follower, candidate, leader
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
		if (rf.voteCount > (len(rf.peers) / 2)) && (rf.role != Leader) {
			rf.timeReset <- MIN_LEADER
			rf.voteCount = 0
			rf.role = Leader
		}
		rf.mux.Unlock()
	}
	return ok
}

func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)
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
	isLeader := true
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
		peers:peers,
		me:me,
		applyCh: applyCh,
		currentTerm:0,
		votedFor:-1,
		timerTicker:make(chan bool),
		timeReset:make(chan int),
		role:0,
		voteCount:0,
		population:len(peers),
		leaderCh:make(chan int),
		candidateCh:make(chan int),
		followerCh:make(chan int),
	}
	go rf.timerRoutine()
	go rf.mainRoutine()
	// Your initialization code here (2A, 2B)
	return rf
}

//  Routines
func (rf *Raft) mainRoutine() {
	for {
		select {
		case <- rf.timerTicker:
			rf.mux.Lock()
			switch rf.role {
			default:
				rf.currentTerm += 1
				rf.role = Candidate
				rf.votedFor = rf.me
				rf.voteCount += 1
				for i := 0; i < rf.population; i++ {
					if i == rf.me {
						continue
					}
					go rf.sendRequestVote(i,
						&RequestVoteArgs{
							Term:rf.currentTerm,
							CandidateId:rf.me,
							LastLogIndex:len(rf.nextIndex),
							LastLogTerm:-1, // problem
						}, &RequestVoteReply{})
				}
				rf.mux.Unlock()
			case Leader:
				rf.role = Leader
				for i := 0; i < rf.population; i++ {
					if i == rf.me {
						continue
					}
					go rf.sendAppendEntries(i,
						&AppendEntriesArgs{
							Term:rf.currentTerm,
							LeaderId:rf.me,
							PrevLogIndex:0, // problem
							PrevLogTerm:0, //problem
							Entries:nil,
							LeaderCommit:0, // problem
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
		randTime := randInt(min, max) // may need to be longer
		timer := time.NewTimer(time.Duration(randTime) * time.Millisecond)
		select {
		case reset := <- rf.timeReset:
			if reset < MIN_NORMAL {
				max = MAX_LEADER
				min = MIN_LEADER
			} else {
				max = MAX_NORMAL
				min = MIN_NORMAL
			}
		case <- timer.C:
			rf.timerTicker <- true
		}
	}
}

// Helper Functions
func randInt(min , max int) int {
	return min + rand.Intn(max-min)
}

