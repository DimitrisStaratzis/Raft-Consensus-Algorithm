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
	//"//fmt"
	//"math/rand"

	"bytes"
	"encoding/gob"
	"fmt"

	//"fmt"

	//"fmt"

	//"fmt"
	//"//fmt"
	//"//fmt"
	//"//fmt"
	//"//fmt"
	"math/rand"

	//"log"

	//"math/rand"

	//"//fmt"
	//"//fmt"
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
	Leader         bool
	Index          int
	DeletedIndexes int
	Command        interface{}
	UseSnapshot    bool   // ignore for lab2; only used in lab3
	Snapshot       []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Term    int
	Command interface{}
	Index   int
}

type vote struct {
	vote bool
	term int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          string
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's State
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted State
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// State a Raft server must maintain.
	State                 int //0 for follower, 1 for candidate, 2 for leader
	previousHeartBeatTime time.Time
	LeaderID              int
	electionTimeOut       time.Duration
	applyChan             chan ApplyMsg
	Log                   []LogEntry
	emptyLog              []LogEntry
	NextIndex             []int
	MatchIndex            []int
	CurrentTerm           int
	VotesFor              int //index
	CommitIndex           int
	LastApplied           int
	lastTermToVote        int
	electionStarted       int64
	Killed                bool
	numberOfPeers         int
	heartBeatTimeOut      time.Duration
	startedElection       time.Time
	LastSnapshotIndex     int
	LastSnapshotTerm      int
	DeletedIndexes        int
	previousCompaction    int
}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.CurrentTerm, rf.State == 2
}

//
// save Raft's persistent State to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	_ = e.Encode(rf.Log)
	_ = e.Encode(rf.VotesFor)
	_ = e.Encode(rf.CurrentTerm)
	_ = e.Encode(rf.LastSnapshotIndex)
	_ = e.Encode(rf.LastSnapshotTerm)
	_ = e.Encode(&rf.DeletedIndexes)
	_ = e.Encode(&rf.previousCompaction)

	//_ = e.Encode(rf.State)
	//_ = e.Encode(rf.LeaderID)
	//_ = e.Encode(rf.MatchIndex)
	//_ = e.Encode(rf.NextIndex)
	//_ = e.Encode(rf.CommitIndex)
	//_ = e.Encode(rf.LastApplied)
	//_ = e.Encode(rf.Killed)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted State.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	if data == nil || len(data) < 1 { // bootstrap without any State?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	_ = d.Decode(&rf.Log)
	_ = d.Decode(&rf.VotesFor)
	_ = d.Decode(&rf.CurrentTerm)
	_ = d.Decode(&rf.LastSnapshotIndex)
	_ = d.Decode(&rf.LastSnapshotTerm)
	_ = d.Decode(&rf.DeletedIndexes)
	_ = d.Decode(&rf.previousCompaction)

	//_ = d.Decode(&rf.State)
	//_ = d.Decode(&rf.LeaderID)
	//_ = d.Decode(&rf.MatchIndex)
	//_ = d.Decode(&rf.NextIndex)
	//_ = d.Decode(&rf.CommitIndex)
	//_ = d.Decode(&rf.LastApplied)
	//_ = d.Decode(&rf.Killed)

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
	Term                        int
	Success                     bool
	difference                  int
	firstIndexOfConflictingTerm int
}

//example RequestVote RPC handler.

//func (rf *Raft) isUpToDate(cIndex int, cTerm int) bool {
//	term, index := rf.getLastTerm(), rf.getLastIndex()
//
//	if cTerm != term {
//		return cTerm >= term
//	}
//
//	return cIndex >= index
//}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	fmt.Println(": ", rf.me, " RECEIVED REQUEST VOTE FROM ", args.CandidateID, " FOR TERM ", args.Term, " VOTES FOR = ", rf.VotesFor)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Do not grant vote if term < CurrentTerm

	if args.Term < rf.CurrentTerm {
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		fmt.Println(": ", rf.me, "COMPLETED REQUEST VOTE FROM ", args.CandidateID, " FOR TERM ", args.Term, " FINISHED", rf.VotesFor)
		return
	}

	if rf.CurrentTerm < args.Term {
		rf.CurrentTerm = args.Term
		rf.State = 0
		//rf.previousHeartBeatTime = time.Now()
		fmt.Println(rf.me, " BIGGER TERM REQUESTED, SO STEPPING DOWN, MY TERM NOW IS: ", args.Term)
		rf.VotesFor = -1
		//rf.persist()
	}
	reply.Term = rf.CurrentTerm
	if ((rf.VotesFor == -1) || (rf.VotesFor == args.CandidateID)) && candidateLogIsUpToDate(args, rf) { // if server has not voted yet
		fmt.Println(": ", rf.me, " I VOTE IN TERM : ", args.Term, " FOR ", args.CandidateID)
		reply.VoteGranted = true
		rf.lastTermToVote = args.Term
		rf.CurrentTerm = args.Term
		rf.VotesFor = args.CandidateID
		rf.previousHeartBeatTime = time.Now()
		//rf.persist()

		//////fmt.Print(" ton ", args.CandidateID)

		//} else {
		//	////fmt.Println(": ", rf.me, " den psifizw sto term: ", args.Term, " ton ", args.CandidateID)
		//	reply.VoteGranted = false
		//
		//}
	} else {
		////fmt.Println(": ", rf.me, " den psifizw sto term: ", args.Term, " ton ", args.CandidateID)
		reply.VoteGranted = false

	}
	fmt.Println(": ", rf.me, "COMPLETED REQUEST VOTE FROM ", args.CandidateID, " FOR TERM ", args.Term, " FINISHED", rf.VotesFor)

	rf.persist()
}

func candidateLogIsUpToDate(args *RequestVoteArgs, rf *Raft) bool {
	if len(rf.Log) > 0 {
		if rf.Log[len(rf.Log)-1].Term != args.LastLogTerm {
			return args.LastLogTerm > rf.Log[len(rf.Log)-1].Term
		} else {
			return args.LastLogIndex >= len(rf.Log)-1
		}
	}
	return true
}

func min(x int, y int) int {
	if x < y {
		return x
	}
	return y
}

func thereIsConflict(raft *Raft, leaderLogs []LogEntry, myLogs []LogEntry) bool {
	for i := range myLogs {
		if i == len(leaderLogs) {
			break
		}
		if leaderLogs[i].Term != myLogs[i].Term {
			return true
		}
	}
	return false
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	//leaderCommit := args.LeaderCommit
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//defer rf.mu.Unlock()
	fmt.Println(": ", rf.me, " FINISHED APPENDING FROM THE LEADER OF THE TERM ", args.Term, " ->  ", args.LeaderId, " MY TERM IS ", rf.CurrentTerm, "AND MY LOGARG IS ", args.Entries)
	reply.Term = rf.CurrentTerm

	if args.Term < rf.CurrentTerm {
		reply.Success = false
		reply.firstIndexOfConflictingTerm = len(rf.Log)
		//rf.mu.Unlock()
	} else if args.Term >= rf.CurrentTerm {
		reply.Success = true
		rf.CurrentTerm = args.Term
		rf.VotesFor = args.LeaderId
		rf.State = 0
		rf.previousHeartBeatTime = time.Now()

		//if args.PrevLogIndex > len(rf.Log)-1 {
		//	reply.Success=false
		//	return
		//}

		//if args.PrevLogIndex > len(rf.Log)-1 {
		//	return
		//}
		//if not a heartbeat
		////fmt.Println(len(rf.Log) >= args.PrevLogIndex, " + ", args.PrevLogIndex >= 0, " + ", len(args.Entries)>0)

		if args.PrevLogIndex > len(rf.Log)-1 {
			reply.Success = false
			reply.firstIndexOfConflictingTerm = len(rf.Log)
			rf.persist()
			//rf.mu.Unlock()
			fmt.Println(": ", rf.me, " FINISHED APPENDING FROM THE LEADER OF THE TERM ", args.Term, " ->  ", args.LeaderId, " MY TERM IS ", rf.CurrentTerm, " AND MY LOGARG IS ", args.Entries)

			return
		}

		rf.previousHeartBeatTime = time.Now()
		if args.PrevLogIndex > 0 {
			////fmt.Println("to index einai: ", args.PrevLogIndex, "kai to megethos einai: ",len(rf.Log))
			if rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
				//
				////fmt.Println(": ", rf.me, " DEN EXOUN KOINH VASH TA LOGS MOY ME TOY LEADER  ", args.LeaderId)
				reply.Success = false
				//reply.firstIndexOfConflictingTerm = - 50

				//optimization suggested on paper
				for i := 0; i < len(rf.Log); i++ {
					if rf.Log[i].Term == rf.Log[args.PrevLogIndex].Term {
						reply.firstIndexOfConflictingTerm = i
						break
					}
				}

				rf.persist()
				//rf.mu.Unlock()
				//rf.mu.Unlock()
				fmt.Println(": ", rf.me, " FINISHED APPENDING FROM THE LEADER OF THE TERM ", args.Term, " ->  ", args.LeaderId, " MY TERM IS ", rf.CurrentTerm, "AND MY LOGARG IS ", args.Entries)

				return
			}
		}
		rf.previousHeartBeatTime = time.Now()

		logsToCompare := rf.Log[args.PrevLogIndex+1:]
		if thereIsConflict(rf, args.Entries, logsToCompare) || len(logsToCompare) < len(args.Entries) {
			////fmt.Println("EIXAME CONFLICT")
			rf.Log = rf.Log[:args.PrevLogIndex+1]
			rf.Log = append(rf.Log, args.Entries...)
		} else {
			//rf.Log = append(rf.Log, args.Entries...)
		}
		rf.previousHeartBeatTime = time.Now()

		if args.LeaderCommit > rf.CommitIndex {

			rf.CommitIndex = min(args.LeaderCommit, len(rf.Log)-1)
			//send commited changes to apply channel
			go func(rf *Raft) {
				rf.mu.Lock()
				if rf.Killed {
					rf.mu.Unlock()
					return
				}
				fmt.Println(": ", rf.me, " COMMITING FROM ", rf.LastApplied, " TO ", rf.CommitIndex)

				if rf.LastApplied+rf.DeletedIndexes < rf.LastSnapshotIndex { // We need to apply latest snapshot
					rf.mu.Unlock()

					rf.applyChan <- ApplyMsg{UseSnapshot: true, Snapshot: rf.persister.ReadSnapshot()}

					rf.mu.Lock()
					rf.LastApplied = rf.LastSnapshotIndex
					rf.mu.Unlock()
					return
				}

				for i := rf.LastApplied + 1; i <= rf.CommitIndex; i++ {
					var applymsg ApplyMsg
					applymsg.Index = i
					applymsg.DeletedIndexes = rf.DeletedIndexes
					applymsg.Leader = false
					applymsg.Command = rf.Log[i].Command
					fmt.Println(rf.me, " I am ready to apply log with index: ", i+rf.DeletedIndexes, " to the state machine")
					rf.applyChan <- applymsg
					fmt.Println(rf.me, " I applied log with index: ", i+rf.DeletedIndexes, " to the state machine")

				}
				fmt.Println(rf.me, " I applied my logs to the state machine")
				rf.LastApplied = rf.CommitIndex
				rf.mu.Unlock()
				rf.persist()

			}(rf)

		}
		rf.previousHeartBeatTime = time.Now()
		rf.persist()

	}
	fmt.Println(": ", rf.me, "FINISHED APPENDING FROM THE LEADER OF TERM ", args.Term, " ->  ", args.LeaderId, " MY TERM IS ", rf.CurrentTerm, "AND LOGARG IS: ", args.Entries)

}

//func thereIsConflict(raft *Raft) bool {
//
//}

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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok && rf.State == 2 && args.Term == rf.CurrentTerm {

		if reply.Term > rf.CurrentTerm {
			//rf.mu.Lock()
			rf.VotesFor = -1
			rf.State = 0
			fmt.Println(":", rf.me, "I AM NOT THE LEADER ANYMORE", "MY TERM IS ", rf.CurrentTerm, "SOMEONE HAD TERM: ", reply.Term)
			//rf.previousHeartBeatTime = time.Now() //todo ksanades to
			rf.CurrentTerm = reply.Term
			rf.LeaderID = -1
			rf.persist()
			return ok
			//rf.mu.Unlock()
		}

		if reply.Success {
			rf.MatchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.NextIndex[server] = rf.MatchIndex[server] + 1
			rf.persist()
			////fmt.Println("MPHKA")
		} else { //if args.Term >= rf.CurrentTerm && len(args.Entries)>0{
			//if rf.NextIndex[server] > 0 {
			//	if reply.firstIndexOfConflictingTerm > 0 {
			//		rf.NextIndex[server] = reply.firstIndexOfConflictingTerm
			//	}else{
			//		rf.NextIndex[server]--
			//	}
			rf.NextIndex[server] = reply.firstIndexOfConflictingTerm
			rf.persist()
		}

		//search all entries from last to commit index.
		fmt.Println(rf.me, "Will check whethe the majority of other servers have a log: commitIndex =", rf.CommitIndex, " Last log entry index = ", len(rf.Log)-1)

		for N := len(rf.Log) - 1; rf.CommitIndex < N; N-- {
			count := 1
			//if equal terms check if if the majority has it
			if rf.Log[N].Term == rf.CurrentTerm {
				for i := range rf.peers {
					if rf.MatchIndex[i] >= N {
						count++
					}
				}
			}
			fmt.Println("MY LOG IS RECEIVED BY : ", count, " THEY SHOULD BE: ", (len(rf.peers)/2)+1)
			//if the majority has it apply entries to state machine
			if count > len(rf.peers)/2 {
				rf.CommitIndex = N
				rf.persist()
				//commit logs
				fmt.Println(":", rf.me, "COMMITING ENTRIES UNTIL: ", rf.CommitIndex+rf.DeletedIndexes)
				go func(rf *Raft) {
					rf.mu.Lock()
					if rf.Killed {
						rf.mu.Unlock()
						return
					}

					if rf.LastApplied+rf.DeletedIndexes < rf.LastSnapshotIndex { // We need to apply latest snapshot
						rf.mu.Unlock()

						rf.applyChan <- ApplyMsg{UseSnapshot: true, Snapshot: rf.persister.ReadSnapshot()}

						rf.mu.Lock()
						rf.LastApplied = rf.LastSnapshotIndex
						rf.mu.Unlock()
						return
					}

					for i := rf.LastApplied + 1; i <= rf.CommitIndex; i++ {
						var applymsg ApplyMsg
						applymsg.Index = i
						applymsg.Leader = true
						applymsg.DeletedIndexes = rf.DeletedIndexes
						applymsg.Command = rf.Log[i].Command
						rf.applyChan <- applymsg
						fmt.Println(rf.me, " I applied my logs to the state machine")
					}
					rf.LastApplied = rf.CommitIndex
					rf.persist()
					rf.mu.Unlock()
				}(rf)
				break
			}
		}
	}
	return ok

	//if ok && rf.State == 2 && args.Term == rf.CurrentTerm{
	//	//it is heartbeat
	//	if len(args.Entries) == 0  {
	//		if reply.Success==false{
	//			rf.mu.Lock()
	//			rf.VotesFor = -1
	//
	//			rf.State = 0
	//			
	//			rf.previousHeartBeatTime = time.Now()
	//			//rf.CurrentTerm = reply.Term
	//			rf.LeaderID = -1
	//			rf.mu.Unlock()
	//		}
	//	//if not heartbeat
	//	}else if len(args.Entries) > 0{
	//		if reply.Success{
	//			rf.MatchIndex[server] = args.PrevLogIndex + len(args.Entries)
	//			rf.NextIndex[server] = rf.MatchIndex[server] + 1
	//			////fmt.Println("MPHKA")
	//		}else {//if args.Term >= rf.CurrentTerm && len(args.Entries)>0{
	//			rf.NextIndex[server]--
	//		}
	//		//count if majority commited my entries
	//		for N := len(rf.Log)-1; rf.CommitIndex < N; N-- {
	//			////fmt.Println("MPHKA")
	//			count := 1
	//
	//			if rf.Log[N].Term == rf.CurrentTerm {
	//				for i := range rf.peers {
	//					if rf.MatchIndex[i] >= N {
	//						count++
	//					}
	//				}
	//			}
	//			
	//			if count > len(rf.peers) / 2 {
	//				rf.CommitIndex = N
	//				//commit logs
	//				
	//				go func(rf *Raft) {
	//					for i := rf.LastApplied +1; i <= rf.CommitIndex; i++ {
	//						rf.applyChan <- ApplyMsg{Index: i, Command: rf.Log[i].Command}
	//					}
	//					rf.LastApplied = rf.CommitIndex
	//				}(rf)
	//				break
	//			}
	//		}
	//	}
	//}
	//return ok
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := 0
	term := 0
	//isLeader := true

	//rf.mu.Lock()
	if rf.State != 2 {
		return rf.DeletedIndexes + index, term, false
	}

	// Your code here (2B).
	var logentry LogEntry
	logentry.Command = command
	logentry.Term = rf.CurrentTerm
	index = len(rf.Log) + rf.DeletedIndexes // not -1 because it will increase
	fmt.Println("ENTRY ADDED: ===============================================================================================", logentry, " at index:", index)
	rf.Log = append(rf.Log, logentry)
	term = rf.CurrentTerm
	rf.persist()
	//rf.mu.Unlock()
	return index, term, true
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.Killed = true
}

func (rf *Raft) runServer() {

	rand.Seed(time.Now().UnixNano())
	random := rand.Intn(100)
	rand.Seed(int64(random))
	timeout := time.Duration(rand.Intn(200)+600) * time.Millisecond
	for {
		rf.mu.Lock()
		killed := rf.Killed
		rf.mu.Unlock()
		if killed {
			return
		}
		rf.mu.Lock()
		state := rf.State
		rf.mu.Unlock()
		if state == 0 {
			rf.mu.Lock()
			if time.Now().Sub(rf.previousHeartBeatTime) > timeout {
				//rf.mu.Lock()
				rf.State = 1
				rf.persist()
				fmt.Println(":", rf.me, "TIMEOUT AND ELECTIONS FOR TERM ", rf.CurrentTerm+1, "AFTER ", timeout)
				rf.mu.Unlock()
			} else {
				rf.mu.Unlock()
			}

		} else if state == 1 {
			rf.mu.Lock()
			rf.CurrentTerm++
			rf.VotesFor = rf.me //vote myself
			rf.lastTermToVote = rf.CurrentTerm
			rf.startedElection = time.Now()
			rf.persist()
			rf.mu.Unlock()
			startElection(rf)
		} else { // if leader
			sendAppendEntries(rf)
			time.Sleep(100 * time.Millisecond)

		}

	}
}

func startElection(rf *Raft) {
	fmt.Println("---------------WE HAVE ELECTIONS FOR TERM: ", rf.CurrentTerm, " FROM SERVER: ", rf.me, " WITH STATE STATE: ", rf.State)
	//rf.mu.Lock()
	VOTING_NOW := true
	votesNeeded := rf.numberOfPeers / 2
	votesReceived := 1
	newVote := make(chan vote)

	rand.Seed(time.Now().UnixNano())
	random := rand.Intn(100)
	rand.Seed(int64(random))
	timeout2 := time.Duration(rand.Intn(200)+600) * time.Millisecond

	//rf.mu.Unlock()

	for i, _ := range rf.peers {
		rf.mu.Lock()
		if i != rf.me && rf.State == 1 {

			//votes needed except the one rf gives to itself
			//myself
			lastLogIndex := len(rf.Log) - 1

			var args = RequestVoteArgs{}
			var reply RequestVoteReply

			args.Term = rf.CurrentTerm
			args.CandidateID = rf.me

			if lastLogIndex == -1 {
				args.LastLogIndex = 0
				args.LastLogTerm = 0
			} else {
				args.LastLogIndex = lastLogIndex
				args.LastLogTerm = rf.Log[lastLogIndex].Term
			}

			go func(i int, args RequestVoteArgs, reply RequestVoteReply, newVote chan vote) {
				fmt.Println(":", rf.me, " SENDIND REQUEST VOTE ", i, "FOR TERM ", rf.CurrentTerm)
				if rf.sendRequestVote(i, &args, &reply) {
					//time.Sleep(10*time.Microsecond)
					var voteargs vote
					voteargs.vote = reply.VoteGranted
					voteargs.term = reply.Term

					rf.mu.Lock()
					currentTerm := rf.CurrentTerm
					rf.mu.Unlock()

					if reply.Term == currentTerm {
						newVote <- voteargs
					}

					if reply.Term > currentTerm {
						rf.mu.Lock()
						rf.CurrentTerm = reply.Term
						rf.State = 0
						//rf.previousHeartBeatTime = time.Now()
						rf.VotesFor = -1
						rf.persist()
						rf.mu.Unlock()
					}
				} else {
					//did not receive vote maybe server is down
				}
			}(i, args, reply, newVote)
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
		}

	}

	//gather all votes
	for VOTING_NOW {
		select {
		case incomingVote := <-newVote:
			rf.mu.Lock()
			currentTerm := rf.CurrentTerm
			state := rf.State
			rf.mu.Unlock()
			if incomingVote.vote && incomingVote.term == currentTerm && state == 1 {
				votesReceived++
			}
			if votesReceived > votesNeeded && incomingVote.term == currentTerm && state == 1 {
				rf.mu.Lock()
				//reinitiallize arrays
				rf.NextIndex = make([]int, len(rf.peers))
				rf.MatchIndex = make([]int, len(rf.peers))
				for j, _ := range rf.peers {
					rf.NextIndex[j] = len(rf.Log) + 1
					rf.MatchIndex[j] = 0
				}
				rf.State = 2
				rf.VotesFor = rf.me
				fmt.Println("NEW LEADER IS: ", rf.me, "ME STATE ", rf.State, "STO TERM ", rf.CurrentTerm, "me psifous: ", votesReceived)

				rf.LeaderID = rf.me
				rf.persist()
				rf.mu.Unlock()
				VOTING_NOW = false
				break
			}
			//rf.mu.Unlock()
		default:
			rf.mu.Lock()
			strtEl := rf.startedElection
			rf.mu.Unlock()
			if time.Now().Sub(strtEl) > timeout2 { //rf.generateRandomTimeOut(500, 300) {
				votesReceived = 0
				VOTING_NOW = false
				break
			}
		}
	}
}

func sendAppendEntries(rf *Raft) {

	for i, _ := range rf.peers {
		rf.mu.Lock()
		if i != rf.me && rf.State == 2 {
			args := AppendEntriesArgs{
				Term:         rf.CurrentTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.CommitIndex}
			var reply AppendEntriesReply

			if i >= 0 && i < len(rf.NextIndex) {
				args.PrevLogIndex = rf.NextIndex[i] - 1
				if rf.NextIndex[i]-1 >= 0 && rf.NextIndex[i]-1 < len(rf.Log) {
					////fmt.Println(len(rf.Log), " : ", rf.NextIndex[i]-1)
					args.PrevLogTerm = rf.Log[rf.NextIndex[i]-1].Term
				}

				if len(rf.Log)-1 >= rf.NextIndex[i] { //&& rf.NextIndex[i] >= 0 {
					//needs snapshot
					if rf.NextIndex[i]+rf.DeletedIndexes <= rf.LastSnapshotIndex {
						rf.sendSnapshot(i)
						return
					}
					//rf.sendSnapshot(i)
					args.Entries = rf.Log[rf.NextIndex[i]:]
				}
			}
			go func(i int, args AppendEntriesArgs, reply AppendEntriesReply) {
				fmt.Println(":", rf.me, "ME STATE: ", rf.State, "KAI STELNW APPEND STON ", i, "GIA TO TERM ", rf.CurrentTerm, "TIME: ")
				rf.sendAppendEntries(i, &args, &reply)
			}(i, args, reply)
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) findLogIndex(logIndex int) int {
	fmt.Println(rf.me, "Index transformation will execute: ", logIndex, " - ", rf.DeletedIndexes)
	return logIndex - rf.DeletedIndexes
}

func (rf *Raft) CompactLog(lastLogIndex int) {
	if lastLogIndex == -1 {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if (len(rf.Log) + rf.DeletedIndexes) > lastLogIndex {
		if (rf.previousCompaction != lastLogIndex) && len(rf.Log) > 0 {

			fmt.Println(rf.me, "The log has size:  ", len(rf.Log), " Log is: ", rf.Log)

			rf.LastSnapshotIndex = lastLogIndex
			fmt.Println(rf.me, " ", lastLogIndex, " means : ", rf.findLogIndex(lastLogIndex))

			rf.LastSnapshotTerm = rf.Log[rf.findLogIndex(lastLogIndex)].Term

			lenBeforeCompaction := len(rf.Log)
			rf.Log = rf.Log[rf.findLogIndex(lastLogIndex)+1:]
			lenAfterCompaction := len(rf.Log)
			difference := lenBeforeCompaction - lenAfterCompaction
			fmt.Println(rf.me, " Deleting: ", difference, " entries from the log")
			rf.DeletedIndexes += difference
			fmt.Println(rf.me, " Length before compaction was: ", lenBeforeCompaction, " and now is: ", lenAfterCompaction)

			rf.previousCompaction = lastLogIndex

			if (rf.CommitIndex - rf.DeletedIndexes) >= 0 {
				rf.CommitIndex -= rf.DeletedIndexes
			} else {
				rf.CommitIndex = 0
			}

			if (rf.LastApplied - rf.DeletedIndexes) >= 0 {
				rf.LastApplied -= rf.DeletedIndexes
			} else {
				rf.CommitIndex = 0
			}

			for i, _ := range rf.peers {
				fmt.Println("OUT: ", i, " len is: ", len(rf.peers))
				if (rf.MatchIndex[i] - rf.DeletedIndexes) >= 0 {
					rf.MatchIndex[i] -= rf.DeletedIndexes
				} else {
					rf.MatchIndex[i] = 0
				}
				if (rf.NextIndex[i] - rf.DeletedIndexes) >= 0 {
					rf.NextIndex[i] -= rf.DeletedIndexes
				} else {
					rf.NextIndex[i] = 0
				}
			}
		}
	}

	rf.persist()
}

// InstallSnapshot - RPC function
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		return
	} else if args.Term >= rf.CurrentTerm {
		rf.State = 0
		//rf.previousHeartBeatTime = time.Now()
		rf.VotesFor = -1
		//rf.LeaderID = args.LeaderId
	}

	rf.previousHeartBeatTime = time.Now()

	rf.persister.SaveSnapshot(args.Data) // Save snapshot, discarding any existing snapshot with smaller index

	i := rf.findLogIndex(args.LastIncludedIndex)
	if rf.Log[i].Term == args.LastIncludedTerm {
		// If existing log entry has same index and term as snapshot’s last included entry, retain log entries following it
		rf.Log = rf.Log[i+1:]
	} else { // Otherwise discard the entire log
		rf.Log = make([]LogEntry, 0)
	}

	rf.LastSnapshotIndex = args.LastIncludedIndex
	rf.LastSnapshotTerm = args.LastIncludedTerm
	rf.LastApplied = 0 // LocalApplyProcess will pick this change up and send snapshot

	rf.persist()

	fmt.Println("Snapshot was installed")

}

func (rf *Raft) sendSnapshot(peerIndex int) {
	rf.mu.Lock()

	peer := rf.peers[peerIndex]
	args := InstallSnapshotArgs{
		Term:              rf.CurrentTerm,
		LastIncludedIndex: rf.LastSnapshotIndex,
		LastIncludedTerm:  rf.LastSnapshotTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}

	rf.mu.Unlock()
	ok := peer.Call("Raft.InstallSnapshot", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if reply.Term > rf.CurrentTerm {
			rf.State = 0
			rf.VotesFor = -1
		} else {
			rf.NextIndex[peerIndex] = rf.findLogIndex(args.LastIncludedIndex + 1)
		}
	}
}

func (rf *Raft) generateRandomTimeOut(min int, range_ int) time.Duration {
	rand.Seed(time.Now().UnixNano())
	return time.Duration(rand.Intn(range_)+min) * time.Millisecond
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent State, and also initially holds the most
// recent saved State, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}

	rf.peers = peers
	rf.Killed = false
	rf.persister = persister
	rf.me = me
	rf.State = 0
	rf.previousHeartBeatTime = time.Now()
	rf.applyChan = applyCh
	rf.startedElection = time.Now()
	rf.CurrentTerm = 0
	rf.VotesFor = -1
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.LeaderID = -1
	rf.applyChan = applyCh
	rf.lastTermToVote = -1
	rf.LastSnapshotIndex = -1
	rf.LastSnapshotTerm = 0
	rf.DeletedIndexes = 0
	rf.previousCompaction = -1
	rf.electionStarted = -1
	rf.numberOfPeers = len(peers)
	rf.NextIndex = make([]int, len(rf.peers))
	rf.MatchIndex = make([]int, len(rf.peers))
	//range
	//minHeartbeat := 600
	//maxHeartbeat := 800
	//rf.heartBeatTimeOut = time.Duration(rand.Intn(maxHeartbeat-minHeartbeat)+minHeartbeat) * time.Millisecond
	////range
	//minElection := 600
	//maxElection := 800
	//rf.electionTimeOut = time.Duration(rand.Intn(maxElection-minElection)+minElection) * time.Millisecond

	// initialize from State persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.runServer()
	// Your initialization code here (2A, 2B, 2C).

	return rf
}
