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

	"fmt"
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
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type vote struct {
	vote bool
	term int
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
	previousHeartBeatTime time.Time
	leaderID              int
	electionTimeOut       time.Duration
	applyChan             chan ApplyMsg
	Log                   []LogEntry
	emptyLog              []LogEntry
	nextIndex             []int
	matchIndex            []int
	currentTerm           int
	votesFor              int //index
	commitIndex           int
	lastApplied           int
	lastTermToVote        int
	electionStarted       int64
	killed                bool
	numberOfPeers         int
	heartBeatTimeOut      time.Duration
	startedElection       time.Time
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
	Term       int
	Success    bool
	difference int
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Do not grant vote if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.state = 0
		//rf.previousHeartBeatTime = time.Now()
		//fmt.Println(rf.me, " MOU ESTEILE PIO MEGALOS KAI KANW STEP DOWN, to term mou twra tha einai ", args.Term )
		rf.votesFor = -1
	}
	reply.Term = rf.currentTerm
	////fmt.Println(": ", rf.me, " -----------------ELAVA REQUEST VOTE APO TON ", args.CandidateID, " GIA TO TERM ", args.Term, " votes for = ", rf.votesFor)
	if ((rf.votesFor == -1) || (rf.votesFor == args.CandidateID)) && candidateLogIsUpToDate(args, rf) { // if server has not voted yet
		////fmt.Println(": ", rf.me, " Psifizw sto term: ", args.Term, " ton ", args.CandidateID)
		reply.VoteGranted = true
		rf.lastTermToVote = args.Term
		rf.currentTerm = args.Term
		rf.votesFor = args.CandidateID
		rf.previousHeartBeatTime = time.Now()

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
	//rf.persist()
}

func candidateLogIsUpToDate(args *RequestVoteArgs, rf *Raft) bool {
	if len(rf.Log) > 0 {
		if rf.Log[len(rf.Log)-1].Term != args.LastLogTerm {
			return args.LastLogTerm >= rf.Log[len(rf.Log)-1].Term
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

/*

 */
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
	//defer rf.mu.Unlock()
	////fmt.Println(": ", rf.me, " ELAVA APPEND APO TON LEADER TOU TERM ", args.Term, "TON ", args.LeaderId, " TO DIKO MOU TERM EINAI ", rf.currentTerm, "KAI TO LOGARG EINAI ", args.Entries)
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		rf.mu.Unlock()
	} else if args.Term >= rf.currentTerm {
		reply.Success = true
		rf.currentTerm = args.Term
		rf.votesFor = args.LeaderId
		rf.state = 0
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
			rf.mu.Unlock()
			return
		}

		rf.previousHeartBeatTime = time.Now()
		if args.PrevLogIndex > 0 {
			////fmt.Println("to index einai: ", args.PrevLogIndex, "kai to megethos einai: ",len(rf.Log))
			if rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
				//
				////fmt.Println(": ", rf.me, " DEN EXOUN KOINH VASH TA LOGS MOY ME TOY LEADER  ", args.LeaderId)
				reply.Success = false
				rf.mu.Unlock()
				//rf.mu.Unlock()
				return
			}
		}
		rf.previousHeartBeatTime = time.Now()

		////fmt.Println("\n\n\n\n\n: ", rf.me, " TO PALIO MOU LOG EINAI: ")
		//for i := 0; i < len(rf.Log); i++ {
		//	////fmt.Println("			", rf.Log[i], " ENTRY")
		//}
		//
		//////fmt.Println(": ", rf.me, " MOU ESTEILAN TO LOG: me prevlogindex :", args.PrevLogIndex)
		//for i := 0; i < len(args.Entries); i++ {
		//	////fmt.Println("			", args.Entries[i], " ENTRY")
		//}
		//If an existing entry conflicts with a new one (same index
		//but different terms), delete the existing entry and all that
		//follow it
		////fmt.Println(":", rf.me, " I COMMITED THE ENTRIES FROM LEADER: ", args.LeaderId)
		//reply.Success = true// we will fix everything dont worry
		//if len(rf.Log) > 0{
		logsToCompare := rf.Log[args.PrevLogIndex+1:]
		if thereIsConflict(rf, args.Entries, logsToCompare) || len(logsToCompare) < len(args.Entries) {
			////fmt.Println("EIXAME CONFLICT")
			rf.Log = rf.Log[:args.PrevLogIndex+1]
			rf.Log = append(rf.Log, args.Entries...)
		} else {
			//rf.Log = append(rf.Log, args.Entries...)
		}
		rf.previousHeartBeatTime = time.Now()
		//}else{
		//	rf.Log = append(rf.Log, args.Entries...)
		//}

		////fmt.Println(": ", rf.me, " TO NEO MOU LOG EINAI: ")
		//for i := 0; i < len(rf.Log); i++ {
		//	////fmt.Println("			", rf.Log[i], " ENTRY")
		//}
		//rf.previousHeartBeatTime = time.Now()

		//If leaderCommit > commitIndex, set commitIndex =
		//min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.commitIndex {

			rf.commitIndex = min(args.LeaderCommit, len(rf.Log)-1)
			//send commited changes to apply channel
			////fmt.Println(": ", rf.me, " THA KANW COMMIT APO TO ", rf.lastApplied, " MEXRI TO ", rf.commitIndex)
			go func(rf *Raft) {
				rf.mu.Lock()
				for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
					var applymsg ApplyMsg
					applymsg.Index = i
					applymsg.Command = rf.Log[i].Command
					rf.applyChan <- applymsg
				}
				rf.lastApplied = rf.commitIndex
				rf.mu.Unlock()
			}(rf)

		}
		rf.previousHeartBeatTime = time.Now()
		rf.mu.Unlock()
	}

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

	if ok && rf.state == 2 && args.Term == rf.currentTerm {

		if reply.Term > rf.currentTerm {
			//rf.mu.Lock()
			rf.votesFor = -1
			////fmt.Println("EIMAI LEADER KAI EGINA FOLLOWER ------------------------------------------------")
			rf.state = 0
			//////fmt.Println(":", rf.me, "DEN EIMAI LEADER PIA", "TO TERM MOU EINAI ", rf.currentTerm, "KAPOIOS EIXE TERM: ", reply.Term)
			//rf.previousHeartBeatTime = time.Now() //todo ksanades to
			rf.currentTerm = reply.Term
			rf.leaderID = -1
			return ok
			//rf.mu.Unlock()
		}

		if reply.Success {
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			////fmt.Println("MPHKA")
		} else { //if args.Term >= rf.currentTerm && len(args.Entries)>0{
			if rf.nextIndex[server] > 0 {
				rf.nextIndex[server]--
				////fmt.Println("XAXA")
			}
		}
		//count if majority commited my entries
		for N := len(rf.Log) - 1; rf.commitIndex < N; N-- {
			count := 1

			if rf.Log[N].Term == rf.currentTerm {
				for i := range rf.peers {
					if rf.matchIndex[i] >= N {
						count++
					}
				}
			}
			////fmt.Println("EXOUN TO LOG MOU: ", count, " ENW THA EPREPE ", (len(rf.peers)/2)+1)
			if count > len(rf.peers)/2 {
				rf.commitIndex = N
				//commit logs
				////fmt.Println(":", rf.me, " KANW COMMIT ENTRIES MEXRI TO: ", rf.commitIndex)
				go func(rf *Raft) {
					rf.mu.Lock()
					for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
						var applymsg ApplyMsg
						applymsg.Index = i
						applymsg.Command = rf.Log[i].Command
						rf.applyChan <- applymsg
					}
					rf.lastApplied = rf.commitIndex
					rf.mu.Unlock()
				}(rf)
				break
			}
		}
	}
	return ok

	//if ok && rf.state == 2 && args.Term == rf.currentTerm{
	//	//it is heartbeat
	//	if len(args.Entries) == 0  {
	//		if reply.Success==false{
	//			rf.mu.Lock()
	//			rf.votesFor = -1
	//			////fmt.Println("EIMAI LEADER KAI EGINA FOLLOWER ------------------------------------------------")
	//			rf.state = 0
	//			//////fmt.Println(":", rf.me, "DEN EIMAI LEADER PIA", "TO TERM MOU EINAI ", rf.currentTerm, "KAPOIOS EIXE TERM: ", reply.Term)
	//			rf.previousHeartBeatTime = time.Now()
	//			//rf.currentTerm = reply.Term
	//			rf.leaderID = -1
	//			rf.mu.Unlock()
	//		}
	//	//if not heartbeat
	//	}else if len(args.Entries) > 0{
	//		if reply.Success{
	//			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
	//			rf.nextIndex[server] = rf.matchIndex[server] + 1
	//			////fmt.Println("MPHKA")
	//		}else {//if args.Term >= rf.currentTerm && len(args.Entries)>0{
	//			rf.nextIndex[server]--
	//		}
	//		//count if majority commited my entries
	//		for N := len(rf.Log)-1; rf.commitIndex < N; N-- {
	//			////fmt.Println("MPHKA")
	//			count := 1
	//
	//			if rf.Log[N].Term == rf.currentTerm {
	//				for i := range rf.peers {
	//					if rf.matchIndex[i] >= N {
	//						count++
	//					}
	//				}
	//			}
	//			////fmt.Println("EXOUN TO LOG MOU: ", count, " ENW THA EPREPE ", len(rf.peers)/2)
	//			if count > len(rf.peers) / 2 {
	//				rf.commitIndex = N
	//				//commit logs
	//				////fmt.Println(":", rf.me, " KANW COMMIT ENTRIES MEXRI TO: ", rf.commitIndex)
	//				go func(rf *Raft) {
	//					for i := rf.lastApplied +1; i <= rf.commitIndex; i++ {
	//						rf.applyChan <- ApplyMsg{Index: i, Command: rf.Log[i].Command}
	//					}
	//					rf.lastApplied = rf.commitIndex
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
	if rf.state != 2 {
		return index, term, false
	}

	// Your code here (2B).
	var logentry LogEntry
	logentry.Command = command
	logentry.Term = rf.currentTerm
	index = len(rf.Log) // not -1 because it will increase
	////fmt.Println("ENTRY ADDED: ===============================================================================================", logentry)
	rf.Log = append(rf.Log, logentry)
	//rf.mu.Unlock()
	//sendAppendEntriesToReplicateLog(rf)
	////fmt.Println("LEADER, " ,rf.me," WAS ASKED FROM CLIENT TO EXECUTE COMMAND. LEADER HAS NOW LOG SIZE: ", len(rf.Log))
	//for i := 0; i < len(rf.Log); i++ {
	//	////fmt.Println("			", rf.Log[i], " ENTRY")
	//}
	term = rf.currentTerm
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
	rf.killed = true
}

func (rf *Raft) runServer() {

	for {
		if rf.killed {
			return
		}
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		if state == 0 {
			rf.mu.Lock()
			rand.Seed(int64((rf.me + 1) * 50))
			//////fmt.Println(time.Now().UnixNano(), " ELA RE MALAKA")
			//////fmt.Println(time.Now().UnixNano(), " ELA RE MALAKA")
			timeout := time.Duration(rand.Intn(300)+600) * time.Millisecond
			//timeout := time.Duration(rand.Intn(300) + 600)*time.Millisecond
			if time.Now().Sub(rf.previousHeartBeatTime) > timeout {
				////fmt.Println(timeout, " XAXA")
				//rf.mu.Lock()
				rf.state = 1
				//fmt.Println(":", rf.me, "--------------EKANA TMT KAI KANW EKLOGES GIA TO TERM ", rf.currentTerm+1, "META APO ", timeout)
				rf.mu.Unlock()
			} else {
				rf.mu.Unlock()
			}

		} else if state == 1 {
			rf.mu.Lock()
			rf.currentTerm++
			rf.votesFor = rf.me //vote myself
			rf.lastTermToVote = rf.currentTerm
			rf.mu.Unlock()
			startElection(rf)
		} else { // if leader
			sendAppendEntries(rf)
			time.Sleep(100 * time.Millisecond)

		}

	}
}

func startElection(rf *Raft) {
	//////fmt.Println("---------------------------------------------------------------------------------EXOUME EKLOGES GIA TO TERM: ", rf.currentTerm, " APO TON: ", rf.me, " ME STATE: ", rf.state)

	//rf.mu.Lock()
	VOTING_NOW := true
	votesNeeded := rf.numberOfPeers / 2
	votesReceived := 1
	newVote := make(chan vote)
	rf.startedElection = time.Now()
	//rf.mu.Unlock()

	for i, _ := range rf.peers {
		rf.mu.Lock()
		if i != rf.me && rf.state == 1 {

			//votes needed except the one rf gives to itself
			//myself
			lastLogIndex := len(rf.Log) - 1

			var args = RequestVoteArgs{}
			var reply RequestVoteReply

			args.Term = rf.currentTerm
			args.CandidateID = rf.me

			if lastLogIndex == -1 {
				args.LastLogIndex = 0
				args.LastLogTerm = 0
			} else {
				args.LastLogIndex = lastLogIndex
				args.LastLogTerm = rf.Log[lastLogIndex].Term
			}

			go func(i int, args RequestVoteArgs, reply RequestVoteReply, newVote chan vote) {
				////fmt.Println(":", rf.me, " KAI STELNW REQUEST VOTE STON ", i, "GIA TO TERM ", rf.currentTerm)
				if rf.sendRequestVote(i, &args, &reply) {
					//time.Sleep(10*time.Microsecond)
					var voteargs vote
					voteargs.vote = reply.VoteGranted
					voteargs.term = reply.Term

					if reply.Term == rf.currentTerm {
						newVote <- voteargs
					}
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = 0
						//rf.previousHeartBeatTime = time.Now()
						rf.votesFor = -1
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

	for VOTING_NOW {
		select {
		case incomingVote := <-newVote:
			rf.mu.Lock()
			currentTerm := rf.currentTerm
			state := rf.state
			rf.mu.Unlock()
			if incomingVote.vote && incomingVote.term == currentTerm && state == 1 {
				votesReceived++
			}
			if votesReceived > votesNeeded && incomingVote.term == currentTerm && state == 1 {
				rf.mu.Lock()
				//reinitiallize arrays
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for j, _ := range rf.peers {
					rf.nextIndex[j] = len(rf.Log)
				}
				rf.state = 2
				rf.votesFor = rf.me
				//fmt.Println("NEW LEADER IS: ", rf.me, "ME STATE ", rf.state, "STO TERM ", rf.currentTerm, "me psifous: ", votesReceived)

				rf.leaderID = rf.me
				rf.mu.Unlock()
				VOTING_NOW = false
				break
			}
			//rf.mu.Unlock()
		default:
			rf.mu.Lock()

			rand.Seed(int64((rf.me + 1) * 50))
			//////fmt.Println(time.Now().UnixNano(), " ELA RE MALAKA")
			//////fmt.Println(time.Now().UnixNano(), " ELA RE MALAKA")
			timeout2 := time.Duration(rand.Intn(300)+600) * time.Millisecond
			//////fmt.Println(timeout2, " ELA RE MALAKA")
			if time.Now().Sub(rf.startedElection) > timeout2 { //rf.generateRandomTimeOut(500, 300) {
				votesReceived = 0
				rf.mu.Unlock()
				VOTING_NOW = false
				break
			} else {
				rf.mu.Unlock()
			}
		}
	}
}

func sendAppendEntries(rf *Raft) {
	for i, _ := range rf.peers {
		rf.mu.Lock()
		if i != rf.me && rf.state == 2 {
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				LeaderCommit: rf.commitIndex}
			var reply AppendEntriesReply
			////fmt.Println("MEGETHOS0: ", len(rf.Log), len(rf.Log)-1,  ">=",  rf.nextIndex[i])
			////fmt.Println("MEGETHOS: ", len(rf.Log))
			//args complete
			////fmt.Println(": ", rf.me, " STELNW TO LOG MOU GIA REPLICATION STON ", i, "TO LOG MOU POU THA EPREPE NA EXEI MEGETHOS: ", len(rf.Log)," EINAI:")

			//if args.PrevLogIndex == -1 {
			//	args.PrevLogIndex =0
			//}
			////fmt.Println("--------------------------- ", args.PrevLogIndex)
			if rf.nextIndex[i]-1 >= 0 && rf.nextIndex[i]-1 < len(rf.Log) {
				////fmt.Println(len(rf.Log), " : ", rf.nextIndex[i]-1)
				args.PrevLogTerm = rf.Log[rf.nextIndex[i]-1].Term
			}

			if len(rf.Log)-1 >= rf.nextIndex[i] { //&& rf.nextIndex[i] >= 0 {
				args.Entries = rf.Log[rf.nextIndex[i]:]
			}

			////fmt.Println(": ", rf.me, " LEADER EIMAI KAI STELNW STON ", i, "TO LOG MOU EXEI MEGETHOS: ", len(rf.Log), " kai tha steilw apo to ", rf.nextIndex[i], " kai meta")
			for i := 0; i < len(args.Entries); i++ {
				////fmt.Println("			", args.Entries[i], " ENTRY")
			}
			////fmt.Print(": ", rf.me, " TA NEXTiNDEX MOU EINAI:  ")
			for i := 0; i < len(rf.nextIndex); i++ {
				////fmt.Print(rf.nextIndex[i], " ")
			}
			////fmt.Println()

			//go rf.sendAppendEntries(i, &args, &AppendEntriesReply{})
			//////fmt.Println(":", rf.me, "ME STATE: ", rf.state, "KAI STELNW APPEND STON ", i, "GIA TO TERM ", rf.currentTerm, "TIME: ")
			go func(i int, args AppendEntriesArgs, reply AppendEntriesReply) {
				////fmt.Println(":", rf.me, "ME STATE: ", rf.state, "KAI STELNW APPEND STON ", i, "GIA TO TERM ", rf.currentTerm, "TIME: ")
				rf.sendAppendEntries(i, &args, &reply)
			}(i, args, reply)
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) generateRandomTimeOut(min int, range_ int) time.Duration {
	rand.Seed(time.Now().UnixNano())
	//for{
	//	////fmt.Println(time.Duration(rand.Intn(range_) + min)*time.Millisecond)
	//}
	//////fmt.Println(time.Duration(rand.Intn(100) + min)*time.Millisecond , " EDW")
	return time.Duration(rand.Intn(range_)+min) * time.Millisecond
	//return time.Duration(rand.Intn(700-600)+600) * time.Millisecond
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
	fmt.Println("-------------------------------------------------------------------")
	//////fmt.Println("-------------------------------------------------------------------")
	//////fmt.Println("-------------------------------------------------------------------")
	//////fmt.Println("-------------------------------------------------------------------")

	rf.peers = peers
	rf.killed = false
	rf.persister = persister
	rf.me = me
	rf.state = 0
	rf.previousHeartBeatTime = time.Now()
	rf.applyChan = applyCh
	rf.startedElection = time.Now()
	rf.currentTerm = 0
	rf.votesFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.leaderID = -1
	rf.applyChan = applyCh
	rf.lastTermToVote = -1
	rf.electionStarted = -1
	rf.numberOfPeers = len(peers)
	//range
	//minHeartbeat := 600
	//maxHeartbeat := 800
	//rf.heartBeatTimeOut = time.Duration(rand.Intn(maxHeartbeat-minHeartbeat)+minHeartbeat) * time.Millisecond
	////range
	//minElection := 600
	//maxElection := 800
	//rf.electionTimeOut = time.Duration(rand.Intn(maxElection-minElection)+minElection) * time.Millisecond

	go rf.runServer()
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
