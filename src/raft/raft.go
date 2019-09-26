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
	"fmt"
	//"fmt"
	//"fmt"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state                 int //0 for follower, 1 for candidate, 2 for leader
	previousHeartBeatTime int64
	electionTimeThreshold int64
	leaderID              int
	applyChan             chan ApplyMsg
	Log                   []LogEntry
	currentTerm           int
	votesFor              int //index
	commitIndex           int
	lastApplied           int
	lastTermToVote        int
	electionStarted       int64
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	if rf.state == 2 {
		isleader = true
	} else {
		isleader = false
	}
	// Your code here (2A).
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term         int
	Success      bool
	NextTryIndex int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()

	//reply.Term = rf.currentTerm
	//rf.mu.Lock()
	//rf.mu.Lock()
	//rf.mu.Unlock()
	if rf.lastTermToVote < args.Term { // if server has not voted yet
		fmt.Println("psifizw ", rf.lastTermToVote, args.Term)
		rf.lastTermToVote = args.Term
		//fmt.Println("mphka", rf.lastTermToVote)
		if (rf.currentTerm <= args.Term) && len(rf.Log)-1 <= args.LastLogIndex {
			reply.VoteGranted = true
			rf.votesFor = args.CandidateID

		} else {
			reply.VoteGranted = false
		}
	}
	rf.mu.Unlock()
	//rf.mu.Unlock()

	/*else if rf.lastTermToVote == args.Term {
		if rf.votesFor == args.CandidateID { // if i have prev voted vote again
			reply.VoteGranted = true
		} else {
			//i have voted another server
		}

	}*/
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		//step down from being a leader
		reply.Success = false
	} else {
		reply.Success = true
		rf.mu.Lock()
		rf.previousHeartBeatTime = time.Now().UnixNano()
		rf.mu.Unlock()
	}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) startServer() {
	//wait for heartbeats

	var randomElectionSeed int64
	randomElectionSeed = 30
	for {
		//if follower
		if rf.state == 0 {

			timeSinceLastHeartbeat := time.Now().UnixNano() - rf.previousHeartBeatTime
			//fmt.Println(string(timeSinceLastHeartbeat) + " :time")
			if timeSinceLastHeartbeat > (rf.electionTimeThreshold + randomElectionSeed*int64(rf.me)) {
				rf.mu.Lock()
				rf.state = 1
				//fmt.Println("MPHKA")
				rf.currentTerm++
				rf.mu.Unlock()
				//rf.resetPeerVotes()
				//fmt.Println("TIME OUT")
				//rf.startElection() //thelei GO?
			}

		} else if rf.state == 1 {
			if (time.Now().UnixNano() - rf.electionStarted) > 50 {
				rf.mu.Lock()
				rf.electionStarted = time.Now().UnixNano()
				rf.mu.Unlock()
				rf.startElection()

			}

		} else { // if leader
			rf.sendHeartBeats()
		}

	}
}

func (rf *Raft) startElection() {

	//fmt.Println("Election starts1")
	//now send requests to all other peers to vote for rf by using the sendRequest

	//fmt.Println("Election starts2")

	decideLeader(rf)

}

func (rf *Raft) sendHeartBeats() {
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.leaderID,
		PrevLogIndex: 0, //todo se all the entries below again
		PrevLogTerm:  0,
		Entries:      rf.Log,
		LeaderCommit: 0}
	var reply AppendEntriesReply
	for i, _ := range rf.peers {
		heartbeatStatus := rf.sendAppendEntries(i, &args, &reply)
		if heartbeatStatus == false {
			//fmt.Println("Heartbeat failed")
		}
		if reply.Success == false {
			rf.mu.Lock()
			rf.state = 0
			rf.mu.Unlock()
		}
	}

}

func decideLeader(rf *Raft) {
	//fmt.Println("ELECTION STARTS")
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	votesNeeded := (len(rf.peers)) % 2 //votes needed except the one rf gives to itself
	var votesReceived int
	lastLogIndex := len(rf.Log) - 1
	var args = RequestVoteArgs{}

	args.Term = rf.currentTerm
	args.CandidateID = rf.me
	if lastLogIndex == -1 {
		args.LastLogIndex = 0
		args.LastLogTerm = 0
	} else {
		args.LastLogIndex = lastLogIndex
		args.LastLogTerm = rf.Log[lastLogIndex].Term
	}

	var reply RequestVoteReply
	//rf.votesFor = rf.me
	rf.mu.Lock()
	rf.votesFor = rf.me //vote myself
	rf.mu.Unlock()
	//TODO H ILOPOIHSH AUTH EINAI SIRIAKH, NOMIZW PREPEI NA STELNEIS SE THREASD TA REQUEST VOTE KAI NA PAREIS META TA SVSTA
	for i, _ := range rf.peers {
		//fmt.Println("PEER SENT")
		voteStatus := rf.sendRequestVote(i, &args, &reply) //TODO TSEKARE AN EINAI THREAD H AN THA EPISTREPSEI AMESWS
		if voteStatus == false {
			//fmt.Println("voting failed") //todo WRITE MORE INFO

		}
		if reply.VoteGranted {
			votesReceived++
			//fmt.Println("VOTE ++")
			fmt.Print(votesReceived)
			fmt.Print(votesNeeded)
		}

	}
	rf.mu.Lock()
	if votesReceived >= votesNeeded {
		//rf.mu.Lock()
		rf.state = 2
		fmt.Println("WE HAVE LEADER")
		rf.leaderID = rf.me
		//TODO CHECK IF LEADER IS ONLY ONE.
		//rf.mu.Unlock()
	} else {
		//become a follower again
		rf.state = 0
	}
	rf.mu.Unlock()
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
	rf.state = 0
	rf.previousHeartBeatTime = time.Now().UnixNano()
	rf.electionTimeThreshold = 200
	rf.applyChan = applyCh
	rf.currentTerm = 0
	rf.votesFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastTermToVote = -1
	rf.electionStarted = -1

	go rf.startServer()
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
