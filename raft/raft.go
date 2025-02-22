package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	position    int
	currentTerm int

	lastHeartBeat   time.Time
	electionTimeout int64

	votedFor int
	nVotes   int

	log         []Entry
	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	snapshot          []byte
	lastIncludedIndex int
	lastIncludedTerm  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isleader := rf.position == 2
	// Your code here (3A).
	return term, isleader
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	err := e.Encode(rf.currentTerm)
	if err != nil {
		return
	}
	err = e.Encode(rf.votedFor)
	if err != nil {
		return
	}
	err = e.Encode(rf.log)
	if err != nil {
		return
	}
	err = e.Encode(rf.lastIncludedIndex)
	if err != nil {
		return
	}
	err = e.Encode(rf.lastIncludedTerm)
	if err != nil {
		return
	}

	raftState := w.Bytes()
	rf.persister.Save(raftState, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currTerm int
	var votedFor int
	var log []Entry
	var includedIndex int
	var includedTerm int

	if d.Decode(&currTerm) != nil ||
		d.Decode(&votedFor) != nil || d.Decode(&log) != nil ||
		d.Decode(&includedIndex) != nil || d.Decode(&includedTerm) != nil {
		return
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		rf.currentTerm = currTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = includedIndex
		rf.lastIncludedTerm = includedTerm
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastIncludedIndex {
		return
	}
	logInd := index - rf.lastIncludedIndex
	if logInd >= len(rf.log) {
		return
	}
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.log[logInd].Term
	newLog := make([]Entry, 0)
	for i, entry := range rf.log {
		if 1 <= i && i <= logInd {
			continue
		}
		newLog = append(newLog, entry)
	}
	rf.log = newLog
	rf.snapshot = snapshot
	rf.persist()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
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

	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if rf.currentTerm < args.Term {
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.position = 0

		rf.persist()
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	var lastLogIndex int
	var lastLogTerm int
	if len(rf.log) == 0 {
		lastLogIndex = rf.lastIncludedIndex
		lastLogTerm = rf.lastIncludedTerm
	} else {
		lastLogIndex = len(rf.log) - 1 + rf.lastIncludedIndex
		lastLogTerm = rf.log[lastLogIndex-rf.lastIncludedIndex].Term
	}

	if args.LastLogTerm < lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	reply.Term = rf.currentTerm
	rf.lastHeartBeat = time.Now()
	rf.persist()

	return
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
func (rf *Raft) sendRequestVote(server int, electionStartingTerm int) {
	rf.mu.Lock()
	term := rf.currentTerm
	candidateId := rf.me
	var lastLogIndex int
	var lastLogTerm int
	if len(rf.log) == 0 {
		lastLogIndex = rf.lastIncludedIndex
		lastLogTerm = rf.lastIncludedTerm
	} else {
		lastLogIndex = len(rf.log) - 1 + rf.lastIncludedIndex
		lastLogTerm = rf.log[lastLogIndex-rf.lastIncludedIndex].Term
	}

	args := RequestVoteArgs{term, candidateId, lastLogIndex, lastLogTerm}
	reply := RequestVoteReply{}

	rf.mu.Unlock()

	ok := false
	for ok == false {
		rf.mu.Lock()
		if rf.position != 1 || electionStartingTerm != rf.currentTerm {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		ok = rf.peers[server].Call("Raft.RequestVote", &args, &reply)
	}

	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.position = 0
		rf.votedFor = -1
		rf.persist()
		rf.mu.Unlock()

		return
	}

	if rf.position != 1 {
		rf.mu.Unlock()

		return
	}

	if reply.VoteGranted {
		rf.nVotes++
		if rf.nVotes > len(rf.peers)/2 {
			rf.position = 2
			for i := range rf.peers {
				rf.nextIndex[i] = len(rf.log) + rf.lastIncludedIndex
				rf.matchIndex[i] = 0
			}
			rf.matchIndex[rf.me] = len(rf.log) - 1 + rf.lastIncludedIndex
		}
		rf.mu.Unlock()

		return
	}
	rf.mu.Unlock()
}

func (rf *Raft) startElection() {
	rf.position = 1
	rf.currentTerm += 1
	rf.lastHeartBeat = time.Now()
	rf.electionTimeout = 300 + (rand.Int63() % 300)
	rf.votedFor = rf.me
	rf.nVotes = 1
	rf.persist()

	electionStartingTerm := rf.currentTerm
	rf.mu.Unlock()

	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}

		go rf.sendRequestVote(server, electionStartingTerm)
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.position != 2 {
		return -1, -1, false
	}

	rf.log = append(rf.log, Entry{Command: command, Term: rf.currentTerm})
	rf.nextIndex[rf.me] = len(rf.log) + rf.lastIncludedIndex
	rf.matchIndex[rf.me] = len(rf.log) - 1 + rf.lastIncludedIndex
	rf.persist()
	return len(rf.log) - 1 + rf.lastIncludedIndex, rf.log[len(rf.log)-1].Term, true
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

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	rf.position = 0
	rf.lastHeartBeat = time.Now()

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		reply.Term = rf.currentTerm
		return
	}

	logInd := args.LastIncludedIndex - rf.lastIncludedIndex
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	newLog := make([]Entry, 0)
	for i, entry := range rf.log {
		if 1 <= i && i <= logInd {
			continue
		}
		newLog = append(newLog, entry)
	}
	rf.log = newLog
	rf.snapshot = args.Data
	rf.commitIndex = args.LastIncludedIndex

	rf.persist()
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendInstallSnapshot(server int) {
	args := InstallSnapshotArgs{rf.currentTerm, rf.me, rf.lastIncludedIndex,
		rf.lastIncludedTerm, rf.snapshot}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok && reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.position = 0
		rf.votedFor = -1
		rf.persist()

		return
	}

	if rf.position != 2 || rf.currentTerm != args.Term || !ok {
		return
	}

	rf.nextIndex[server] = rf.lastIncludedIndex + 1
	rf.matchIndex[server] = rf.lastIncludedIndex
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.XLen = -1
		return
	}
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.persist()
	}

	rf.position = 0
	rf.lastHeartBeat = time.Now()

	var lastTerm int
	if args.PrevLogIndex-rf.lastIncludedIndex < len(rf.log) {
		lastTerm = rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term
	} else {
		lastTerm = rf.lastIncludedTerm
	}
	if len(rf.log)+rf.lastIncludedIndex <= args.PrevLogIndex || lastTerm != args.PrevLogTerm {
		if len(rf.log)+rf.lastIncludedIndex > args.PrevLogIndex {
			reply.XIndex = args.PrevLogIndex
			reply.XTerm = rf.log[reply.XIndex-rf.lastIncludedIndex].Term
			for reply.XIndex > rf.lastIncludedIndex &&
				rf.log[reply.XIndex-1-rf.lastIncludedIndex].Term == reply.XTerm {
				reply.XIndex--
			}
			reply.XLen = len(rf.log) + rf.lastIncludedIndex
		} else {
			reply.XIndex = len(rf.log) + rf.lastIncludedIndex
			reply.XTerm = -1
			reply.XLen = len(rf.log) + rf.lastIncludedIndex
		}
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	ind := args.PrevLogIndex + 1 - rf.lastIncludedIndex
	for i := range args.Entries {
		if len(rf.log) <= ind+i {
			rf.log = append(rf.log, args.Entries[i])
			continue
		}
		if rf.log[ind+i].Term != args.Entries[i].Term {
			rf.log = rf.log[:ind+i]
			rf.log = append(rf.log, args.Entries[i])
		}
	}
	if len(args.Entries) > 0 {
		rf.persist()
	}

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < len(rf.log)-1+rf.lastIncludedIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.log) - 1 + rf.lastIncludedIndex
		}
	}

	reply.Term = rf.currentTerm
	reply.Success = true

	return
}

func (rf *Raft) sendAppendEntries(server int) {
	rf.mu.Lock()
	if rf.position != 2 {
		rf.mu.Unlock()
		return
	}

	term := rf.currentTerm
	leaderId := rf.me
	leaderCommit := rf.commitIndex

	prevLogIndex := rf.nextIndex[server] - 1
	var prevLogTerm int
	var entries []Entry

	if prevLogIndex < rf.lastIncludedIndex {
		//call!
		//fmt.Println(prevLogIndex, rf.lastIncludedIndex)
		rf.sendInstallSnapshot(server)
		return
	} else {
		prevLogTerm = rf.log[prevLogIndex-rf.lastIncludedIndex].Term
		entries = rf.log[prevLogIndex+1-rf.lastIncludedIndex:]
	}
	args := AppendEntriesArgs{term, leaderId, prevLogIndex,
		prevLogTerm, entries, leaderCommit}
	reply := AppendEntriesReply{}
	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok && reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.position = 0
		rf.votedFor = -1
		rf.persist()

		return
	}

	if rf.position != 2 || rf.currentTerm != args.Term || !ok || reply.XLen == -1 {
		return
	}

	if reply.Success {
		rf.nextIndex[server] = prevLogIndex + len(entries) + 1
		rf.matchIndex[server] = prevLogIndex + len(entries)
	} else {
		if prevLogIndex >= reply.XLen {
			rf.nextIndex[server] = reply.XLen
			/*if reply.XLen == 0 {
				fmt.Println("HOW")
			}*/
			if rf.nextIndex[server] < rf.lastIncludedIndex {
				///fmt.Println("HOOOW")
			}
		} else {
			index := prevLogIndex
			for index > rf.lastIncludedIndex && rf.log[index-rf.lastIncludedIndex].Term != reply.XTerm {
				index--
			}
			if index == rf.lastIncludedIndex {
				rf.nextIndex[server] = reply.XIndex
				if rf.nextIndex[server] < rf.lastIncludedIndex {
					//fmt.Println("?????")
					///fmt.Println(reply.XIndex, rf.lastIncludedIndex)
				}
			} else {
				rf.nextIndex[server] = index
			}
		}
	}
}

func (rf *Raft) sendHeartBeats() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.position == 2 {
			rf.mu.Unlock()
			for server, _ := range rf.peers {
				if server == rf.me {
					continue
				}

				go rf.sendAppendEntries(server)
			}

			time.Sleep(120 * time.Millisecond)
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		//fmt.Println("check:", rf.me, rf.lastIncludedIndex)
		if rf.position != 2 {
			if time.Since(rf.lastHeartBeat) > time.Duration(rf.electionTimeout)*time.Millisecond {
				go rf.startElection()
			} else {
				rf.mu.Unlock()
			}
		} else {
			rf.mu.Unlock()
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) commiter(applyCh chan ApplyMsg) {
	for rf.killed() == false {
		rf.mu.Lock()

		if rf.lastApplied < rf.lastIncludedIndex {
			applyMsg := ApplyMsg{}
			applyMsg.SnapshotValid = true
			applyMsg.Snapshot = rf.snapshot
			applyMsg.SnapshotIndex = rf.lastIncludedIndex
			rf.lastApplied = rf.lastIncludedIndex

			rf.mu.Unlock()
			applyCh <- applyMsg
			rf.mu.Lock()
		}

		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			if i <= rf.lastIncludedIndex {
				continue
			}
			applyMsg := ApplyMsg{}
			applyMsg.CommandIndex = i
			applyMsg.Command = rf.log[i-rf.lastIncludedIndex].Command
			applyMsg.CommandValid = true

			rf.lastApplied = i

			rf.mu.Unlock()
			applyCh <- applyMsg
			rf.mu.Lock()
		}

		rf.mu.Unlock()

		time.Sleep(20 * time.Millisecond)
	}
}

func (rf *Raft) checkCommitIndex() {
	for rf.killed() == false {
		rf.mu.Lock()

		if rf.position != 2 {
			rf.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}

		for i := rf.commitIndex + 1; i < len(rf.log)+rf.lastIncludedIndex; i++ {
			if i <= rf.lastIncludedIndex || rf.log[i-rf.lastIncludedIndex].Term != rf.currentTerm {
				continue
			}

			count := 1
			for j := 0; j < len(rf.peers); j++ {
				if j != rf.me && rf.matchIndex[j] >= i {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = i
			}
		}

		rf.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.position = 0
	rf.currentTerm = 0
	rf.lastHeartBeat = time.Now()
	rf.electionTimeout = 300 + (rand.Int63() % 300)
	rf.votedFor = -1

	rf.log = make([]Entry, 0)
	rf.log = append(rf.log, Entry{})
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.snapshot = make([]byte, 0)
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.snapshot

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.sendHeartBeats()
	go rf.commiter(applyCh)
	go rf.checkCommitIndex()

	return rf
}
