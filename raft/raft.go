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
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"cs350/labgob"
	"cs350/labrpc"
)

// import "bytes"
// import "cs350/labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	CurrentTerm int // latest term server has seen
	VoteFor     int
	Logs        []Log
	ApplyCh     chan ApplyMsg // see make() function

	// Volatile state on all servers
	State           State // see definition of this const below
	CommitIndex     int   // index of highest log entry known to be committed (initialized to 0)
	LastApplied     int   // index of highest log entry applied to state machine (initialized to 0)
	ElectionTimeout time.Time

	// Volatile state on leaders
	NextIndex  []int // index of the next log entry to send to that server (initialized to leader last log index + 1)
	MatchIndex []int // index of highest log entry known to be replicated on server (initialized to 0)
}

type State string

const (
	Leader    State = "L"
	Candidate State = "C"
	Follower  State = "F"
)

type Log struct {
	Index   int
	Term    int
	Entries interface{} // command for state machine
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	Candidate    int // candidate requesting vote
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntries struct {
	Term         int   // leader’s term
	LeaderId     int   // so follower can redirect clients
	PrevLogIndex int   // index of log entry immediately preceding new ones
	PrevLogTerm  int   // term of prevLogIndex entry
	Entries      []Log // log entries to store (empty for heartbeat; may send more than one for efficiency)
	Commit       int   // leader commit index
}

type AppendEntriesReply struct {
	Term          int  // currentTerm, for leader to update itself
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	BetterNextIdx int  // when !Success due to condition 1&2, tell leader to update NextIdx[me] to resend appendEntries (initialize to NextIdx[me]-1)
}

type ApplyMsgReply struct {
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

// a candidate gives up election and returns back to follower
func (rf *Raft) abstain(term int) {
	rf.State = Follower
	rf.CurrentTerm = term
	rf.VoteFor = -1
	rf.ElectionTimeout = nextElection()

	rf.persist()

	rf.NextIndex = nil
	rf.MatchIndex = nil
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.CurrentTerm
	if rf.State == Leader {
		isleader = true
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VoteFor)
	e.Encode(rf.Logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentTerm, VoteFor int
	var Logs []Log
	if d.Decode(&CurrentTerm) != nil || d.Decode(&VoteFor) != nil || d.Decode(&Logs) != nil {
		log.Panicln("fail to decode in readPersist()")
	} else {
		rf.CurrentTerm = CurrentTerm
		rf.VoteFor = VoteFor
		rf.Logs = Logs
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false

	if rf.CurrentTerm > args.Term { // reject to vote
		log.Println("Reject to vote - small term", "candidate", args.Candidate, "follower", rf.me, "terms", args.Term, rf.CurrentTerm)
		return
	} else if rf.CurrentTerm < args.Term { // change term
		rf.abstain(args.Term)
	}

	myLogTerm := rf.Logs[len(rf.Logs)-1].Term
	if args.LastLogTerm < myLogTerm || (args.LastLogTerm == myLogTerm && args.LastLogIndex < len(rf.Logs)-1) { // reject to vote
		log.Println("Reject to vote - stale log", "candidate", args.Candidate, "follower", rf.me, "logTerms", args.LastLogTerm, myLogTerm, "logIdxs", args.LastLogIndex, len(rf.Logs)-1)
		return
	}

	if rf.State == Candidate {
		rf.abstain(args.Term)
	}

	if rf.VoteFor == -1 || rf.VoteFor == args.Candidate { // vote
		rf.abstain(args.Term)
		reply.VoteGranted = true
		rf.VoteFor = args.Candidate

		rf.persist()
	}
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()

	index := len(rf.Logs)
	term := rf.CurrentTerm
	isLeader := rf.State == Leader

	// Your code here (2B).
	if !isLeader || rf.killed() {
		rf.mu.Unlock()
		return index, term, isLeader
	}

	rf.Logs = append(rf.Logs, Log{
		Index:   index,
		Term:    term,
		Entries: command,
	})
	rf.persist()

	rf.NextIndex[rf.me]++
	rf.MatchIndex[rf.me] = index
	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer != rf.me {
			rf.pacemaker(peer, term)
		}
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func nextElection() time.Time {
	rand.Seed(time.Now().UnixNano())
	return time.Now().Add(time.Duration(500+rand.Intn(500)) * time.Millisecond)
}

// append log entries
func (rf *Raft) AppendEntries(args *AppendEntries, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.CurrentTerm
	reply.Success = false

	// reject leader
	if rf.CurrentTerm > args.Term { // condition 1
		log.Println("Reject AppendEntries: small term, follower", rf.me, "leader", args.LeaderId, "leaderTerm", args.Term, "myTerm", rf.CurrentTerm)
		return
	}
	if len(rf.Logs)-1 < args.PrevLogIndex { // condition 2
		log.Println("Reject AppendEntries: not enough logs, follower", rf.me, "loglen", len(rf.Logs), "prevLogIndex", args.PrevLogIndex)
		reply.BetterNextIdx = len(rf.Logs)
		return
	}
	if args.PrevLogTerm != rf.Logs[args.PrevLogIndex].Term { // condition 3
		log.Println("Reject AppendEntries: log index not comply, follower", rf.me, "loglen", len(rf.Logs), "prevLogIndex", args.PrevLogIndex, "argsTerm", args.PrevLogTerm, "myLogTerm", rf.Logs[args.PrevLogIndex].Term)
		for i := args.PrevLogIndex - 1; i > -1; i-- {
			if rf.Logs[i].Term <= args.PrevLogTerm {
				reply.BetterNextIdx = i
				break
			}
		}
		return
	}

	// convert to follower first
	rf.abstain(args.Term)

	// start to append entries
	if len(args.Entries) > 0 {
		existingEntries := rf.Logs[args.PrevLogIndex+1:]
		var idx int
		// delete conflicting entries
		for idx = 0; idx < min(len(existingEntries), len(args.Entries)); idx++ {
			if existingEntries[idx].Term != args.Entries[idx].Term { // find the confliciting index
				rf.Logs = rf.Logs[:args.PrevLogIndex+1+idx]
				break
			}
		}
		// append
		if idx < len(args.Entries) {
			rf.Logs = append(rf.Logs, args.Entries[idx:]...)
		}
	}
	rf.persist()

	if args.Commit > rf.CommitIndex {
		// log.Println("Check log term of Server ", rf.me, ":", rf.Logs[len(rf.Logs)-1].Term, "LeaderComIdx", args.Commit, "myComIdx", rf.CommitIndex)
		rf.CommitIndex = min(args.Commit, len(rf.Logs)-1)

		for rf.CommitIndex > rf.LastApplied {
			rf.LastApplied++
			entry := ApplyMsg{
				CommandValid: true,
				Command:      rf.Logs[rf.LastApplied].Entries,
				CommandIndex: rf.Logs[rf.LastApplied].Index,
			}
			rf.ApplyCh <- entry
		}
	}

	reply.Term = args.Term
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// deal with appendEntries and its reply
func (rf *Raft) pacemaker(peer int, term int) {
	rf.mu.Lock()

	if rf.State != Leader {
		rf.mu.Unlock()
		return
	}
	prevLogIndex := rf.NextIndex[peer] - 1
	prevLogTerm := rf.Logs[prevLogIndex].Term

	var logEntries []Log
	if len(rf.Logs) > rf.NextIndex[peer] {
		logEntries = rf.Logs[rf.NextIndex[peer]:]
	}

	args := AppendEntries{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      logEntries,
		Commit:       rf.CommitIndex,
	}
	rf.mu.Unlock()

	reply := AppendEntriesReply{BetterNextIdx: prevLogIndex}
	sent := rf.sendAppendEntries(peer, &args, &reply)

	// check response
	if sent {
		rf.mu.Lock()
		if reply.Term > rf.CurrentTerm {
			rf.abstain(reply.Term)
			rf.mu.Unlock()
		} else {
			if rf.State != Leader {
				rf.mu.Unlock()
				log.Println("Replaced during heartbeat - Term", rf.CurrentTerm, " Server", rf.me, "Follower", peer)
				return
			}
			if !reply.Success { // recurse until we find the same log entry
				log.Println("AppendEntries unsucessful - Term", rf.CurrentTerm, "Server", rf.me, "Follower", peer, "nextIdx", rf.NextIndex)
				// rf.NextIndex[peer]--
				rf.NextIndex[peer] = reply.BetterNextIdx
				rf.mu.Unlock()

				go rf.pacemaker(peer, term)
			} else {
				rf.MatchIndex[peer] = args.PrevLogIndex + len(args.Entries)
				rf.NextIndex[peer] = rf.MatchIndex[peer] + 1
				log.Println("AppendEntries sucessful - Term", rf.CurrentTerm, "Server", rf.me, "Follower", peer, "nextIdx", rf.NextIndex)
				rf.mu.Unlock()

				go rf.checkCommit(rf.CurrentTerm)
			}
		}
	} else {
		log.Println("AppendEntries not sent - Term", rf.CurrentTerm, "Leader", rf.me, "Server", rf.me, "follower", peer)
	}
}

func (rf *Raft) checkCommit(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() || rf.State != Leader || rf.CurrentTerm != term || rf.CommitIndex >= len(rf.Logs)-1 {
		return
	}

	for n := rf.CommitIndex + 1; n < len(rf.Logs); n++ {
		if rf.Logs[n].Term == term {
			count := 0
			for i := range rf.peers {
				if rf.MatchIndex[i] >= n {
					count++
				}
			}
			if count >= len(rf.peers)/2+1 {
				rf.CommitIndex = n
			}
		}
	}
	for i := rf.LastApplied + 1; i <= rf.CommitIndex; i++ {
		entry := ApplyMsg{
			CommandValid: true,
			Command:      rf.Logs[i].Entries,
			CommandIndex: i,
		}
		rf.ApplyCh <- entry
		rf.LastApplied++
	}
}

// send initial empty AppendEntries RPCs (heartbeat) to each server
func (rf *Raft) establishElection() {
	log.Println("Term", rf.CurrentTerm, "Server", rf.me, "starts to establish election.")
	rf.MatchIndex = make([]int, len(rf.peers))
	rf.NextIndex = make([]int, len(rf.peers))

	for i := range rf.peers {
		log.Println("establishing: ", i)
		if i == rf.me {
			continue
		}
		rf.MatchIndex[i] = 0
		rf.NextIndex[i] = len(rf.Logs)

		var logEntries []Log
		if len(rf.Logs) > rf.NextIndex[i] {
			logEntries = rf.Logs[rf.NextIndex[i]:]
		}

		args := AppendEntries{
			Term:         rf.CurrentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.NextIndex[i] - 1,
			PrevLogTerm:  rf.Logs[rf.NextIndex[i]-1].Term,
			Entries:      logEntries,
			Commit:       rf.CommitIndex,
		}

		reply := AppendEntriesReply{BetterNextIdx: args.PrevLogIndex}
		rf.sendAppendEntries(i, &args, &reply)
		if reply.Term > rf.CurrentTerm || rf.killed() {
			rf.mu.Lock()
			log.Println("Fail to establish:", "Server", rf.me, "Reply:", i, reply.Success)
			rf.abstain(reply.Term)
			rf.mu.Unlock()
			return
		}
	}
	log.Println("Established:", "Term", rf.CurrentTerm, "Server", rf.me, "Status", rf.State)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		var nap time.Duration

		if rf.State == Leader {
			rf.ElectionTimeout = nextElection()
			nap = time.Until(rf.ElectionTimeout)
			rf.mu.Unlock()
		} else if time.Now().Before(rf.ElectionTimeout) { // wait until election time
			// log.Println("Server", rf.me, "needs to nap")
			nap = time.Until(rf.ElectionTimeout)
			rf.mu.Unlock()
		} else { // becomes a candidate and starts to elect
			rf.CurrentTerm++
			rf.State = Candidate
			rf.VoteFor = rf.me
			rf.persist()

			rf.ElectionTimeout = nextElection()
			rf.mu.Unlock()
			nap = time.Until(rf.ElectionTimeout)
			log.Println("Term", rf.CurrentTerm, "Server", rf.me, "starts to elect.")

			/************************ request votes ************************/
			votes := 1
			abstain := false
			for i := range rf.peers {
				if i == rf.me {
					continue // skip for myself
				}

				args := RequestVoteArgs{
					Term:         rf.CurrentTerm,
					Candidate:    rf.me,
					LastLogIndex: len(rf.Logs) - 1,
					LastLogTerm:  rf.Logs[len(rf.Logs)-1].Term,
				}

				reply := RequestVoteReply{}

				ok := rf.sendRequestVote(i, &args, &reply) // request vote
				log.Println("Server", rf.me, "Vote requested from", i, "Sent", ok, "voteReceived", reply.VoteGranted)

				rf.mu.Lock()
				if rf.CurrentTerm < reply.Term {
					rf.abstain(reply.Term)
				}
				if rf.State != Candidate || rf.killed() { // give up the election
					log.Println("Server", rf.me, "gives up the election.")
					abstain = true
					rf.mu.Unlock()
					continue
				}
				rf.mu.Unlock()

				if reply.VoteGranted { // get vote
					votes++
				}
				// go func() {...code} ()
			}

			/************************ establish election ************************/
			rf.mu.Lock()
			if abstain || time.Now().After(rf.ElectionTimeout) {
				log.Println("Server", rf.me, "- election timeout, try again.")
				rf.ElectionTimeout = nextElection()
				rf.mu.Unlock()
				continue
			}

			if votes > len(rf.peers)/2 { // win election
				log.Println("Server", rf.me, "wins the election.")
				rf.State = Leader
				rf.establishElection()
			} else { // try to elect again
				log.Println("Server", rf.me, "Vote", votes, "- election fails, try again.")
				rf.mu.Unlock()
				continue
			}
			rf.mu.Unlock()

			/************************ send heartbeats ************************/
			for !rf.killed() {
				log.Println("Heartbeats - Server", rf.me)
				for i := range rf.peers {
					if i == rf.me {
						continue
					}

					rf.pacemaker(i, rf.CurrentTerm)

					rf.mu.Lock()
					if rf.State != Leader {
						log.Println("Replaced: Term", rf.CurrentTerm, "Server", rf.me, "- start new election.")
						rf.mu.Unlock()
						break
					}
					rf.mu.Unlock()
				}

				rf.mu.Lock()
				if rf.State != Leader {
					rf.mu.Unlock()
					break
				}
				rf.mu.Unlock()

				// 10 heartbeats per second
				time.Sleep(100 * time.Millisecond)
			}
		}

		time.Sleep(nap)
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		dead:      0,

		// Your initialization code here (2A, 2B, 2C).
		State:       Follower,
		CurrentTerm: 0,
		VoteFor:     -1,

		Logs:    []Log{{Term: 0, Index: 0, Entries: -1}},
		ApplyCh: applyCh,

		ElectionTimeout: nextElection(),

		CommitIndex: 0,
		LastApplied: 0,
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// starting from 2B
	rf.ApplyCh <- ApplyMsg{
		CommandValid: true,
		Command:      -1, // cannot use nil for dummy bc 2C churn test
		CommandIndex: rf.CommitIndex,
	}
	// rf.LastApplied++

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
