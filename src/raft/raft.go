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
	"math/rand"

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
	numberOfPeers         int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.state == 2 {
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()

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
//func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
//	// Your code here (2A, 2B).
//	rf.mu.Lock()
//	if rf.votesFor == -1 { // if server has not voted yet
//		fmt.Println("Egw o: ", rf.me, " Prin psifisa sto: ", rf.lastTermToVote, " Twra psifizw sto: ", args.Term)
//		if rf.currentTerm <= args.Term { //&& len(rf.Log)-1 <= args.LastLogIndex {
//			fmt.Println("san: ", rf.me, " psifizw sto term:", args.Term)
//			reply.VoteGranted = true
//			rf.lastTermToVote = args.Term
//			rf.currentTerm = args.Term
//			rf.votesFor = args.CandidateID
//			reply.Term = rf.currentTerm
//			fmt.Println("Psifizw ton ", args.CandidateID)
//
//		} else {
//			//fmt.Println("ma6")
//			reply.VoteGranted = false
//			//reply.Term = rf.currentTerm
//
//		}
//	} else {
//		fmt.Println("eimai o ", rf.me, " kai den mpainw sto term ", args.Term)
//		reply.VoteGranted = false
//		//reply.Term = rf.currentTerm
//	}
//	rf.mu.Unlock()
//}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	fmt.Println(rf.me, " Received request vote")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A, 2B).
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		fmt.Println(rf.me, "1")
	} else {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votesFor = -1
			if rf.leaderID == rf.me {
				rf.leaderID = -1
				rf.state = 0
			}
		}
		if rf.votesFor == -1 || rf.votesFor == args.CandidateID {
			if len(rf.Log) == 0 {
				reply.VoteGranted = true
				reply.Term = rf.currentTerm
				rf.votesFor = args.CandidateID
				fmt.Println(rf.me, "2")
			} else {
				if args.LastLogTerm >= rf.Log[len(rf.Log)-1].Term {
					if args.LastLogIndex >= len(rf.Log) { //Den eimai sigouros an to len einai to swsto mipws thelei comitindex?
						reply.VoteGranted = true
						reply.Term = rf.currentTerm
						rf.votesFor = args.CandidateID
						fmt.Println(rf.me, "3")
					} else {
						reply.Term = rf.currentTerm
						reply.VoteGranted = false
						fmt.Println(rf.me, "4")
					}
				} else {
					reply.Term = rf.currentTerm
					reply.VoteGranted = false
					fmt.Println(rf.me, "5")
				}
			}
		} else {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			fmt.Println(rf.me, "6")
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		//step down from being a leader
		//rf.currentTerm = args.Term
		//rf.votesFor = -1
		reply.Success = false
	} else {
		reply.Success = true
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.votesFor = -1
		rf.previousHeartBeatTime = time.Now().UnixNano() / int64(time.Millisecond)
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
	randomElectionSeed = rand.Int63n(100)
	for {
		//if follower
		if rf.state == 0 {

			timeSinceLastHeartbeat := time.Now().UnixNano()/int64(time.Millisecond) - rf.previousHeartBeatTime
			//fmt.Println(string(timeSinceLastHeartbeat) + " :time")
			if timeSinceLastHeartbeat > (rf.electionTimeThreshold + randomElectionSeed) {
				rf.mu.Lock()
				rf.state = 1
				//fmt.Println("KANW EKLOGES")
				rf.mu.Unlock()
			}

		} else if rf.state == 1 {
			timeSinceLastHeartbeatCandidate := time.Now().UnixNano()/int64(time.Millisecond) - rf.previousHeartBeatTime
			//if received heartbeat as a canidate, become a follower again
			if timeSinceLastHeartbeatCandidate < rf.electionTimeThreshold {
				rf.mu.Lock()
				rf.state = 0
				rf.votesFor = -1
				rf.mu.Unlock()
				fmt.Println("HMOUN CANDIDATE KAI MOU IRTHE LEADER")
			} else if (time.Now().UnixNano()/int64(time.Millisecond) - rf.electionStarted) > 600 {
				rf.mu.Lock()
				rf.electionStarted = time.Now().UnixNano() / int64(time.Millisecond)
				rf.currentTerm++
				rf.votesFor = rf.me //vote myself
				rf.lastTermToVote = rf.currentTerm
				rf.mu.Unlock()
				go startElection(rf)

			}

		} else { // if leader
			sendHeartBeats(rf)
		}

	}
}

func startElection(rf *Raft) {
	//rf.mu.Lock()
	//
	//rf.mu.Unlock()

	votesNeeded := rf.numberOfPeers / 2 //votes needed except the one rf gives to itself
	votesReceived := 1                  //myself
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

	for i, _ := range rf.peers {
		if i != rf.me {
			var reply RequestVoteReply
			//fmt.Println("PEER SENT ", i)
			voteStatus := rf.sendRequestVote(i, &args, &reply)
			if voteStatus == false {
				fmt.Println("VOTER IS DOWN")
			}
			if reply.VoteGranted { //&& reply.Term == rf.currentTerm {
				votesReceived++
				if votesReceived > votesNeeded {
					break
				}
			}
		}

	}
	rf.mu.Lock()
	if votesReceived > votesNeeded {
		//rf.mu.Lock()
		rf.state = 2
		rf.votesFor = -1
		fmt.Println(rf.currentTerm, votesReceived, votesNeeded, "NEW LEADER IS: ", rf.me)
		rf.leaderID = rf.me
		//rf.mu.Unlock()
	} else {
		//become a follower again
		//rf.state = 0
		//rf.currentTerm--
		//fmt.Println("I was not elected: ", rf.me)
	}
	rf.mu.Unlock()
}

func sendHeartBeats(rf *Raft) {
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: 0, //todo se all the entries below again
		PrevLogTerm:  0,
		Entries:      rf.Log,
		LeaderCommit: 0}

	failedVotes := 0
	for i, _ := range rf.peers {
		var reply AppendEntriesReply
		if i != rf.me {
			heartbeatStatus := rf.sendAppendEntries(i, &args, &reply)
			if heartbeatStatus == false {
				//fmt.Println("Heartbeat failed")
				failedVotes++
				if failedVotes > len(rf.peers)/2 {
					break
				}
			}
			if reply.Success == false {
				rf.mu.Lock()
				//rf.currentTerm = reply.Term
				fmt.Println("egw o ", rf.me, " kanw step down apo leader")
				rf.state = 0
				rf.leaderID = -1
				rf.mu.Unlock()
			}
		}

	}
	//if you do not have the quorum online, step down from being leader
	if failedVotes > len(rf.peers)/2 {
		fmt.Println("RE MEGALE EISAI KOLOFARDOS POU EMEINES MONOS")
		rf.mu.Lock()
		rf.state = 0
		rf.leaderID = -1
		rf.mu.Unlock()
	}

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
	rf.previousHeartBeatTime = time.Now().UnixNano() / int64(time.Millisecond)
	rf.electionTimeThreshold = 200
	rf.applyChan = applyCh
	rf.currentTerm = 0
	rf.votesFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.leaderID = -1
	rf.lastTermToVote = -1
	rf.electionStarted = -1
	rf.numberOfPeers = len(peers)

	/*for i, peer := range peers{
		fmt.Println(p)
	}*/

	go rf.startServer()
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
