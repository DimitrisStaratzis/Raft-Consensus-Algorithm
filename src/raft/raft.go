package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

func (entry LogEntry) String() string {
	return fmt.Sprintf("(I:%d, T: %d)", entry.Index, entry.Term)
}

// AppendEntries RPC
type AppendEntriesArgs struct {
	Term             int
	LeaderID         string
	PreviousLogIndex int
	PreviousLogTerm  int
	LogEntries       []LogEntry
	LeaderCommit     int
}

type AppendEntriesReply struct {
	Term                int
	Success             bool
	ConflictingLogTerm  int // Term of the conflicting entry, if any
	ConflictingLogIndex int // First index of the log for the above conflicting term
}

// RequestVote RPC
type RequestVoteArgs struct {
	Term         int
	CandidateID  string
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	Id          string
}

func (reply *RequestVoteReply) VoteCount() int {
	if reply.VoteGranted {
		return 1
	}
	return 0
}

// InstallSnapshot RPC
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

// RaftPersistence is persisted to the `persister`, and contains all necessary data to restart a failed node
type RaftPersistence struct {
	CurrentTerm       int
	Log               []LogEntry
	VotedFor          string
	LastSnapshotIndex int
	LastSnapshotTerm  int
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Leader      bool
	Index       int
	Command     interface{}
	UseSnapshot bool
	Snapshot    []byte
}

type ServerState string

const (
	Follower  ServerState = "Follower"
	Candidate             = "Candidate"
	Leader                = "Leader"
)

const HeartBeatInterval = 100 * time.Millisecond
const CommitApplyIdleCheckInterval = 25 * time.Millisecond
const LeaderPeerTickInterval = 10 * time.Millisecond

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	sync.Mutex // Lock to protect shared access to this peer's state

	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state

	// General state
	id               string
	me               int // this peer's index into peers[]
	state            ServerState
	isDecommissioned bool

	// Election state
	currentTerm int
	votedFor    string // Id of candidate that has voted for, this term. Empty string if no vote has been cast.
	leaderID    string

	// Log state
	log         []LogEntry
	commitIndex int
	lastApplied int

	// Log compaction state, if snapshots are enabled
	lastSnapshotIndex int
	lastSnapshotTerm  int

	// Leader state
	nextIndex      []int // For each peer, index of next log entry to send that server
	matchIndex     []int // For each peer, index of highest entry known log entry known to be replicated on peer
	sendAppendChan []chan struct{}

	// Liveness state
	lastHeartBeat time.Time // When this node last received a heartbeat message from the Leader
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.Lock()
	defer rf.Unlock()
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) getLastEntryInfo() (int, int) {
	if len(rf.log) > 0 {
		entry := rf.log[len(rf.log)-1]
		return entry.Index, entry.Term
	}
	return rf.lastSnapshotIndex, rf.lastSnapshotTerm
}

// Returns index within `rf.log` of log entry with index `logIndex`
func (rf *Raft) findLogIndex(logIndex int) (int, bool) {
	for i, e := range rf.log {
		if e.Index == logIndex {
			return i, true
		}
	}
	return -1, false
}

func (rf *Raft) transitionToCandidate() {
	rf.state = Candidate
	// Increment currentTerm and vote for self
	rf.currentTerm++
	rf.votedFor = rf.id
}

func (rf *Raft) transitionToFollower(newTerm int) {
	rf.state = Follower
	rf.currentTerm = newTerm
	rf.votedFor = ""
}

// InstallSnapshot - RPC function
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.Lock()
	defer rf.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	} else if args.Term >= rf.currentTerm {
		rf.transitionToFollower(args.Term)
		rf.leaderID = args.LeaderId
	}

	if rf.leaderID == args.LeaderId {
		rf.lastHeartBeat = time.Now()
	}

	rf.persister.SaveSnapshot(args.Data) // Save snapshot, discarding any existing snapshot with smaller index

	i, isPresent := rf.findLogIndex(args.LastIncludedIndex)
	if isPresent && rf.log[i].Term == args.LastIncludedTerm {
		// If existing log entry has same index and term as snapshot’s last included entry, retain log entries following it
		rf.log = rf.log[i+1:]
	} else { // Otherwise discard the entire log
		rf.log = make([]LogEntry, 0)
	}

	rf.lastSnapshotIndex = args.LastIncludedIndex
	rf.lastSnapshotTerm = args.LastIncludedTerm
	rf.lastApplied = 0 // LocalApplyProcess will pick this change up and send snapshot

	rf.persist()

	RaftInfo("Installed snapshot from %s, LastSnapshotEntry(Index: %d, Term: %d)", rf, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)
}

func (rf *Raft) sendSnapshot(peerIndex int, sendAppendChan chan struct{}) {
	rf.Lock()

	peer := rf.peers[peerIndex]
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.leaderID,
		LastIncludedIndex: rf.lastSnapshotIndex,
		LastIncludedTerm:  rf.lastSnapshotTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}

	rf.Unlock()

	// Send RPC (with timeouts + retries)
	requestName := "Raft.InstallSnapshot"
	request := func() bool {
		return peer.Call(requestName, &args, &reply)
	}
	ok := SendRPCRequest(requestName, request)

	rf.Lock()
	defer rf.Unlock()
	if ok {
		if reply.Term > rf.currentTerm {
			rf.transitionToFollower(reply.Term)
		} else {
			rf.nextIndex[peerIndex] = args.LastIncludedIndex + 1
		}
	}

	sendAppendChan <- struct{}{} // Signal to leader-peer process that there may be appends to send
}

// RequestVote - RPC function
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.Lock()
	defer rf.Unlock()

	lastIndex, lastTerm := rf.getLastEntryInfo()
	logUpToDate := func() bool {
		if lastTerm == args.LastLogTerm {
			return lastIndex <= args.LastLogIndex
		}
		return lastTerm < args.LastLogTerm
	}()

	reply.Term = rf.currentTerm
	reply.Id = rf.id

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if args.Term >= rf.currentTerm && logUpToDate {
		rf.transitionToFollower(args.Term)
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
	} else if (rf.votedFor == "" || args.CandidateID == rf.votedFor) && logUpToDate {
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
	}

	rf.persist()
	RaftInfo("Vote requested for: %s on term: %d. Log up-to-date? %v. Vote granted? %v", rf, args.CandidateID, args.Term, logUpToDate, reply.VoteGranted)
}

func (rf *Raft) sendRequestVote(serverConn *labrpc.ClientEnd, server int, voteChan chan int, args *RequestVoteArgs, reply *RequestVoteReply) {
	requestName := "Raft.RequestVote"
	request := func() bool {
		return serverConn.Call(requestName, args, reply)
	}

	if ok := SendRPCRequest(requestName, request); ok {
		voteChan <- server
	}
}

// AppendEntries - RPC function
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.Lock()
	defer rf.Unlock()

	RaftInfo("Request from %s, w/ %d entries. Args.Prev:[Index %d, Term %d]", rf, args.LeaderID, len(args.LogEntries), args.PreviousLogIndex, args.PreviousLogTerm)

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	} else if args.Term >= rf.currentTerm {
		rf.transitionToFollower(args.Term)
		rf.leaderID = args.LeaderID
	}

	if rf.leaderID == args.LeaderID {
		rf.lastHeartBeat = time.Now()
	}

	// Try to find supplied previous log entry match in our log
	prevLogIndex := -1
	for i, v := range rf.log {
		if v.Index == args.PreviousLogIndex {
			if v.Term == args.PreviousLogTerm {
				prevLogIndex = i
				break
			} else {
				reply.ConflictingLogTerm = v.Term
			}
		}
	}

	PrevIsInSnapshot := args.PreviousLogIndex == rf.lastSnapshotIndex && args.PreviousLogTerm == rf.lastSnapshotTerm
	PrevIsBeginningOfLog := args.PreviousLogIndex == 0 && args.PreviousLogTerm == 0

	if prevLogIndex >= 0 || PrevIsInSnapshot || PrevIsBeginningOfLog {
		if len(args.LogEntries) > 0 {
			RaftInfo("Appending %d entries from %s", rf, len(args.LogEntries), args.LeaderID)
		}

		// Remove any inconsistent logs and find the index of the last consistent entry from the leader
		entriesIndex := 0
		for i := prevLogIndex + 1; i < len(rf.log); i++ {
			entryConsistent := func() bool {
				localEntry, leadersEntry := rf.log[i], args.LogEntries[entriesIndex]
				return localEntry.Index == leadersEntry.Index && localEntry.Term == leadersEntry.Term
			}
			if entriesIndex >= len(args.LogEntries) || !entryConsistent() {
				// Additional entries must be inconsistent, so let's delete them from our local log
				rf.log = rf.log[:i]
				break
			} else {
				entriesIndex++
			}
		}

		// Append all entries that are not already in our log
		if entriesIndex < len(args.LogEntries) {
			rf.log = append(rf.log, args.LogEntries[entriesIndex:]...)
		}

		// Update the commit index
		if args.LeaderCommit > rf.commitIndex {
			var latestLogIndex = rf.lastSnapshotIndex
			if len(rf.log) > 0 {
				latestLogIndex = rf.log[len(rf.log)-1].Index
			}

			if args.LeaderCommit < latestLogIndex {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = latestLogIndex
			}
		}
		reply.Success = true
	} else {
		// §5.3: When rejecting an AppendEntries request, the follower can include the term of the
		//	 	 conflicting entry and the first index it stores for that term.

		// If there's no entry with `args.PreviousLogIndex` in our log. Set conflicting term to that of last log entry
		if reply.ConflictingLogTerm == 0 && len(rf.log) > 0 {
			reply.ConflictingLogTerm = rf.log[len(rf.log)-1].Term
		}

		for _, v := range rf.log { // Find first log index for the conflicting term
			if v.Term == reply.ConflictingLogTerm {
				reply.ConflictingLogIndex = v.Index
				break
			}
		}

		reply.Success = false
	}
	rf.persist()
}

func (rf *Raft) sendAppendEntryRequest(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	requestName := "Raft.AppendEntries"
	request := func() bool {
		return rf.peers[server].Call(requestName, args, reply)
	}
	return SendRPCRequest(requestName, request)
}

func (rf *Raft) sendAppendEntries(peerIndex int, sendAppendChan chan struct{}) {
	rf.Lock()

	if rf.state != Leader || rf.isDecommissioned {
		rf.Unlock()
		return
	}

	var entries []LogEntry = []LogEntry{}
	var prevLogIndex, prevLogTerm int = 0, 0

	peerId := string(rune(peerIndex + 'A'))
	lastLogIndex, _ := rf.getLastEntryInfo()

	if lastLogIndex > 0 && lastLogIndex >= rf.nextIndex[peerIndex] {
		if rf.nextIndex[peerIndex] <= rf.lastSnapshotIndex { // We don't have the required entry in our log; sending snapshot.
			rf.Unlock()
			rf.sendSnapshot(peerIndex, sendAppendChan)
			return
		} else {
			for i, v := range rf.log { // Need to send logs beginning from index `rf.nextIndex[peerIndex]`
				if v.Index == rf.nextIndex[peerIndex] {
					if i > 0 {
						lastEntry := rf.log[i-1]
						prevLogIndex, prevLogTerm = lastEntry.Index, lastEntry.Term
					} else {
						prevLogIndex, prevLogTerm = rf.lastSnapshotIndex, rf.lastSnapshotTerm
					}
					entries = make([]LogEntry, len(rf.log)-i)
					copy(entries, rf.log[i:])
					break
				}
			}
			RaftInfo("Sending log %d entries to %s", rf, len(entries), peerId)
		}
	} else { // We're just going to send a heartbeat
		if len(rf.log) > 0 {
			lastEntry := rf.log[len(rf.log)-1]
			prevLogIndex, prevLogTerm = lastEntry.Index, lastEntry.Term
		} else {
			prevLogIndex, prevLogTerm = rf.lastSnapshotIndex, rf.lastSnapshotTerm
		}
	}

	reply := AppendEntriesReply{}
	args := AppendEntriesArgs{
		Term:             rf.currentTerm,
		LeaderID:         rf.id,
		PreviousLogIndex: prevLogIndex,
		PreviousLogTerm:  prevLogTerm,
		LogEntries:       entries,
		LeaderCommit:     rf.commitIndex,
	}
	rf.Unlock()

	ok := rf.sendAppendEntryRequest(peerIndex, &args, &reply)

	rf.Lock()
	defer rf.Unlock()

	if !ok {
		RaftDebug("Communication error: AppendEntries() RPC failed", rf)
	} else if rf.state != Leader || rf.isDecommissioned || args.Term != rf.currentTerm {
		RaftInfo("Node state has changed since request was sent. Discarding response", rf)
	} else if reply.Success {
		if len(entries) > 0 {
			RaftInfo("Appended %d entries to %s's log", rf, len(entries), peerId)
			lastReplicated := entries[len(entries)-1]
			rf.matchIndex[peerIndex] = lastReplicated.Index
			rf.nextIndex[peerIndex] = lastReplicated.Index + 1
			rf.updateCommitIndex()
		} else {
			RaftDebug("Successful heartbeat from %s", rf, peerId)
		}
	} else {
		if reply.Term > rf.currentTerm {
			RaftInfo("Switching to follower as %s's term is %d", rf, peerId, reply.Term)
			rf.transitionToFollower(reply.Term)
		} else {
			RaftInfo("Log deviation on %s. T: %d, nextIndex: %d, args.Prev[I: %d, T: %d], FirstConflictEntry[I: %d, T: %d]", rf, peerId, reply.Term, rf.nextIndex[peerIndex], args.PreviousLogIndex, args.PreviousLogTerm, reply.ConflictingLogIndex, reply.ConflictingLogTerm)
			// Log deviation, we should go back to `ConflictingLogIndex - 1`, lowest value for nextIndex[peerIndex] is 1.
			rf.nextIndex[peerIndex] = Max(reply.ConflictingLogIndex-1, 1)
			sendAppendChan <- struct{}{} // Signals to leader-peer process that appends need to occur
		}
	}
	rf.persist()
}

func (rf *Raft) updateCommitIndex() {
	// §5.3/5.4: If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
	for i := len(rf.log) - 1; i >= 0; i-- {
		if v := rf.log[i]; v.Term == rf.currentTerm && v.Index > rf.commitIndex {
			replicationCount := 1
			for j := range rf.peers {
				if j != rf.me && rf.matchIndex[j] >= v.Index {
					if replicationCount++; replicationCount > len(rf.peers)/2 { // Check to see if majority of nodes have replicated this
						RaftInfo("Updating commit index [%d -> %d] as replication factor is at least: %d/%d", rf, rf.commitIndex, v.Index, replicationCount, len(rf.peers))
						rf.commitIndex = v.Index // Set index of this entry as new commit index
						//fmt.Println(rf.me, " i am leader and commiting until: " , rf.commitIndex)
						break
					}
				}
			}
		} else {
			break
		}
	}
}

func (rf *Raft) startLocalApplyProcess(applyChan chan ApplyMsg) {
	rf.Lock()
	RaftInfo("Starting commit process - Last log applied: %d", rf, rf.lastApplied)
	rf.Unlock()

	for {
		//fmt.Println(rf.me, "commiting")
		rf.Lock()
		//if rf.state == Leader{
		//	fmt.Println(rf.me, " Leader about to send Applying message until index: ", rf.commitIndex)
		//
		//}

		if rf.commitIndex >= 0 && rf.commitIndex > rf.lastApplied {
			if rf.lastApplied < rf.lastSnapshotIndex { // We need to apply latest snapshot
				RaftInfo("Locally applying snapshot with latest index: %d", rf, rf.lastSnapshotIndex)
				rf.Unlock()

				applyChan <- ApplyMsg{UseSnapshot: true, Snapshot: rf.persister.ReadSnapshot()}

				rf.Lock()
				rf.lastApplied = rf.lastSnapshotIndex
				rf.Unlock()
			} else {
				startIndex, _ := rf.findLogIndex(rf.lastApplied + 1)
				startIndex = Max(startIndex, 0) // If start index wasn't found, it's because it's a part of a snapshot

				endIndex := -1
				for i := startIndex; i < len(rf.log); i++ {
					if rf.log[i].Index <= rf.commitIndex {
						endIndex = i
					}
				}
				if endIndex >= 0 { // We have some entries to locally commit
					entries := make([]LogEntry, endIndex-startIndex+1)
					copy(entries, rf.log[startIndex:endIndex+1])

					//fmt.Printf("Locally applying %d log entries. lastApplied: %d. commitIndex: %d", rf.me, len(entries), rf.lastApplied, rf.commitIndex)
					rf.Unlock()

					for _, v := range entries { // Hold no locks so that slow local applies don't deadlock the system
						RaftDebug("Locally applying log: %s", rf, v)

						//fmt.Println(rf.me, " Raft Applying message with index: ", v.Index)
						rf.Lock()
						boolean := rf.state == Leader
						rf.Unlock()
						applyChan <- ApplyMsg{Index: v.Index, Command: v.Command, Leader: boolean}
					}
					rf.Lock()
					rf.lastApplied += len(entries)
				} else {
					//fmt.Println(rf.me, " there are no moe entries to apply. I have applied until: ", rf.lastApplied)
				}
				rf.Unlock()
			}
		} else {
			rf.Unlock()
			<-time.After(CommitApplyIdleCheckInterval)
		}
	}
}

func (rf *Raft) startElectionProcess() {
	electionTimeout := func() time.Duration { // Randomized timeouts between [500, 600)-ms
		return (200 + time.Duration(rand.Intn(300))) * time.Millisecond
	}

	currentTimeout := electionTimeout()
	currentTime := <-time.After(currentTimeout)

	rf.Lock()
	defer rf.Unlock()
	if !rf.isDecommissioned {
		// Start election process if we're not a leader and the haven't received a heartbeat for `electionTimeout`
		if rf.state != Leader && currentTime.Sub(rf.lastHeartBeat) >= currentTimeout {
			RaftInfo("Election timer timed out. Timeout: %fs", rf, currentTimeout.Seconds())
			go rf.beginElection()
		}
		go rf.startElectionProcess()
	}
}

func (rf *Raft) beginElection() {
	rf.Lock()

	rf.transitionToCandidate()
	RaftInfo("Election started", rf)

	// Request votes from peers
	lastIndex, lastTerm := rf.getLastEntryInfo()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.id,
		LastLogTerm:  lastTerm,
		LastLogIndex: lastIndex,
	}
	replies := make([]RequestVoteReply, len(rf.peers))
	voteChan := make(chan int, len(rf.peers))
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendRequestVote(rf.peers[i], i, voteChan, &args, &replies[i])
		}
	}
	rf.persist()
	rf.Unlock()

	// Count votes from peers as they come in
	votes := 1
	for i := 0; i < len(replies); i++ {
		reply := replies[<-voteChan]
		rf.Lock()

		// §5.1: If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
		if reply.Term > rf.currentTerm {
			RaftInfo("Switching to follower as %s's term is %d", rf, reply.Id, reply.Term)
			rf.transitionToFollower(reply.Term)
			break
		} else if votes += reply.VoteCount(); votes > len(replies)/2 { // Has majority vote
			// Ensure that we're still a candidate and that another election did not interrupt
			if rf.state == Candidate && args.Term == rf.currentTerm {
				RaftInfo("Election won. Vote: %d/%d", rf, votes, len(rf.peers))
				go rf.promoteToLeader()
				break
			} else {
				RaftInfo("Election for term %d was interrupted", rf, args.Term)
				break
			}
		}
		rf.Unlock()
	}
	rf.persist()
	rf.Unlock()
}

func (rf *Raft) promoteToLeader() {
	rf.Lock()
	defer rf.Unlock()

	rf.state = Leader
	fmt.Println("NEW LEADER IS ", rf.me)
	rf.leaderID = rf.id

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.sendAppendChan = make([]chan struct{}, len(rf.peers))

	for i := range rf.peers {
		if i != rf.me {
			rf.nextIndex[i] = len(rf.log) + 1 // Should be initialized to leader's last log index + 1
			rf.matchIndex[i] = 0              // Index of highest log entry known to be replicated on server
			rf.sendAppendChan[i] = make(chan struct{}, 1)

			// Start routines for each peer which will be used to monitor and send log entries
			go rf.startLeaderPeerProcess(i, rf.sendAppendChan[i])
		}
	}
}

func (rf *Raft) startLeaderPeerProcess(peerIndex int, sendAppendChan chan struct{}) {
	ticker := time.NewTicker(LeaderPeerTickInterval)

	// Initial heartbeat
	rf.sendAppendEntries(peerIndex, sendAppendChan)
	lastEntrySent := time.Now()

	for {
		rf.Lock()
		if rf.state != Leader || rf.isDecommissioned {
			ticker.Stop()
			rf.Unlock()
			break
		}
		rf.Unlock()

		select {
		case <-sendAppendChan: // Signal that we should send a new append to this peer
			lastEntrySent = time.Now()
			rf.sendAppendEntries(peerIndex, sendAppendChan)
		case currentTime := <-ticker.C: // If traffic has been idle, we should send a heartbeat
			if currentTime.Sub(lastEntrySent) >= HeartBeatInterval {
				lastEntrySent = time.Now()
				rf.sendAppendEntries(peerIndex, sendAppendChan)
			}
		}
	}
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
	term, isLeader := rf.GetState()

	if !isLeader {
		return -1, term, isLeader
	}

	rf.Lock()
	defer rf.Unlock()

	nextIndex := func() int {
		if len(rf.log) > 0 {
			return rf.log[len(rf.log)-1].Index + 1
		}
		return Max(1, rf.lastSnapshotIndex+1)
	}()

	entry := LogEntry{Index: nextIndex, Term: rf.currentTerm, Command: command}
	rf.log = append(rf.log, entry)
	RaftInfo("New entry appended to leader's log: %s", rf, entry)

	return nextIndex, term, isLeader
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		id:          string(rune(me + 'A')),
		state:       Follower,
		commitIndex: 0,
		lastApplied: 0,
	}

	RaftInfo("Node created", rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.startElectionProcess()
	go rf.startLocalApplyProcess(applyCh)

	return rf
}

// --- Persistence ---

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	buf := new(bytes.Buffer)
	gob.NewEncoder(buf).Encode(
		RaftPersistence{
			CurrentTerm:       rf.currentTerm,
			Log:               rf.log,
			VotedFor:          rf.votedFor,
			LastSnapshotIndex: rf.lastSnapshotIndex,
			LastSnapshotTerm:  rf.lastSnapshotTerm,
		})

	RaftDebug("Persisting node data (%d bytes)", rf, buf.Len())
	rf.persister.SaveRaftState(buf.Bytes())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	obj := RaftPersistence{}
	d.Decode(&obj)

	rf.votedFor, rf.currentTerm, rf.log = obj.VotedFor, obj.CurrentTerm, obj.Log
	rf.lastSnapshotIndex, rf.lastSnapshotTerm = obj.LastSnapshotIndex, obj.LastSnapshotTerm
	rf.lastApplied = rf.lastSnapshotIndex
	RaftInfo("Loaded persisted node data (%d bytes). Last applied index: %d", rf, len(data), rf.lastApplied)
}

func (rf *Raft) Kill() {
	rf.Lock()
	defer rf.Unlock()

	rf.isDecommissioned = true
	RaftInfo("Node killed", rf)
}

// --- Log compaction ---

func (rf *Raft) CompactLog(lastLogIndex int) {
	rf.Lock()
	defer rf.Unlock()

	if lastLogIndex > rf.commitIndex {
		RaftInfo("Failed to compact log as log index: %d is larger than commit index: %d", rf, lastLogIndex, rf.commitIndex)
	}

	if i, isPresent := rf.findLogIndex(lastLogIndex); isPresent {
		entry := rf.log[i]
		rf.lastSnapshotIndex = entry.Index
		rf.lastSnapshotTerm = entry.Term

		RaftInfo("Compacting log. Removing %d log entries. LastSnapshotEntry(Index: %d, Term: %d)", rf, i+1, entry.Index, entry.Term)
		rf.log = rf.log[i+1:]
	}

	rf.persist()
}

/*


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
	Leader		bool
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
}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.CurrentTerm
	if rf.State == 2 {
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()

	return term, isleader
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Do not grant vote if term < CurrentTerm
	if args.Term < rf.CurrentTerm {
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		return
	}

	if rf.CurrentTerm < args.Term {
		rf.CurrentTerm = args.Term
		rf.State = 0
		//rf.previousHeartBeatTime = time.Now()
		//fmt.Println(rf.me, " MOU ESTEILE PIO MEGALOS KAI KANW STEP DOWN, to term mou twra tha einai ", args.Term )
		rf.VotesFor = -1
		rf.persist()
	}
	reply.Term = rf.CurrentTerm
	////fmt.Println(": ", rf.me, " -----------------ELAVA REQUEST VOTE APO TON ", args.CandidateID, " GIA TO TERM ", args.Term, " votes for = ", rf.VotesFor)
	if ((rf.VotesFor == -1) || (rf.VotesFor == args.CandidateID)) && candidateLogIsUpToDate(args, rf) { // if server has not voted yet
		////fmt.Println(": ", rf.me, " Psifizw sto term: ", args.Term, " ton ", args.CandidateID)
		reply.VoteGranted = true
		rf.lastTermToVote = args.Term
		rf.CurrentTerm = args.Term
		rf.VotesFor = args.CandidateID
		rf.previousHeartBeatTime = time.Now()
		rf.persist()

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
	//defer rf.mu.Unlock()
	////fmt.Println(": ", rf.me, " ELAVA APPEND APO TON LEADER TOU TERM ", args.Term, "TON ", args.LeaderId, " TO DIKO MOU TERM EINAI ", rf.CurrentTerm, "KAI TO LOGARG EINAI ", args.Entries)
	reply.Term = rf.CurrentTerm

	if args.Term < rf.CurrentTerm {
		reply.Success = false
		reply.firstIndexOfConflictingTerm = len(rf.Log)
		rf.mu.Unlock()
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
				//reply.firstIndexOfConflictingTerm = - 50

				//optimization suggested on paper
				for i := 0; i < len(rf.Log); i++ {
					if rf.Log[i].Term == rf.Log[args.PrevLogIndex].Term {
						reply.firstIndexOfConflictingTerm = i
						break
					}
				}

				rf.persist()
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

		//If leaderCommit > CommitIndex, set CommitIndex =
		//min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.CommitIndex {

			rf.CommitIndex = min(args.LeaderCommit, len(rf.Log)-1)
			//send commited changes to apply channel
			////fmt.Println(": ", rf.me, " THA KANW COMMIT APO TO ", rf.LastApplied, " MEXRI TO ", rf.CommitIndex)
			go func(rf *Raft) {
				rf.mu.Lock()
				if rf.Killed {
					rf.mu.Unlock()
					return
				}
				for i := rf.LastApplied + 1; i <= rf.CommitIndex; i++ {
					var applymsg ApplyMsg
					applymsg.Index = i
					applymsg.Leader = false
					applymsg.Command = rf.Log[i].Command
					rf.applyChan <- applymsg
				}
				rf.LastApplied = rf.CommitIndex
				rf.persist()
				rf.mu.Unlock()
			}(rf)

		}
		rf.previousHeartBeatTime = time.Now()
		rf.persist()
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

	if ok && rf.State == 2 && args.Term == rf.CurrentTerm {

		if reply.Term > rf.CurrentTerm {
			//rf.mu.Lock()
			rf.VotesFor = -1
			////fmt.Println("EIMAI LEADER KAI EGINA FOLLOWER ------------------------------------------------")
			rf.State = 0
			//////fmt.Println(":", rf.me, "DEN EIMAI LEADER PIA", "TO TERM MOU EINAI ", rf.CurrentTerm, "KAPOIOS EIXE TERM: ", reply.Term)
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
			////fmt.Println("XAXA")
		}

		//search all entries from last to commit index.
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
			////fmt.Println("EXOUN TO LOG MOU: ", count, " ENW THA EPREPE ", (len(rf.peers)/2)+1)
			//if the majority has it apply entries to state machine
			if count > len(rf.peers)/2 {
				rf.CommitIndex = N
				rf.persist()
				//commit logs
				////fmt.Println(":", rf.me, " KANW COMMIT ENTRIES MEXRI TO: ", rf.CommitIndex)
				go func(rf *Raft) {
					rf.mu.Lock()
					if rf.Killed {
						rf.mu.Unlock()
						return
					}
					for i := rf.LastApplied + 1; i <= rf.CommitIndex; i++ {
						var applymsg ApplyMsg
						applymsg.Index = i
						applymsg.Leader = true
						applymsg.Command = rf.Log[i].Command
						rf.applyChan <- applymsg
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
	//			////fmt.Println("EIMAI LEADER KAI EGINA FOLLOWER ------------------------------------------------")
	//			rf.State = 0
	//			//////fmt.Println(":", rf.me, "DEN EIMAI LEADER PIA", "TO TERM MOU EINAI ", rf.CurrentTerm, "KAPOIOS EIXE TERM: ", reply.Term)
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
	//			////fmt.Println("EXOUN TO LOG MOU: ", count, " ENW THA EPREPE ", len(rf.peers)/2)
	//			if count > len(rf.peers) / 2 {
	//				rf.CommitIndex = N
	//				//commit logs
	//				////fmt.Println(":", rf.me, " KANW COMMIT ENTRIES MEXRI TO: ", rf.CommitIndex)
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
		return index, term, false
	}

	// Your code here (2B).
	var logentry LogEntry
	logentry.Command = command
	logentry.Term = rf.CurrentTerm
	index = len(rf.Log) // not -1 because it will increase
	////fmt.Println("ENTRY ADDED: ===============================================================================================", logentry)
	rf.Log = append(rf.Log, logentry)
	//rf.mu.Unlock()
	//sendAppendEntriesToReplicateLog(rf)
	////fmt.Println("LEADER, " ,rf.me," WAS ASKED FROM CLIENT TO EXECUTE COMMAND. LEADER HAS NOW LOG SIZE: ", len(rf.Log))
	//for i := 0; i < len(rf.Log); i++ {
	//	////fmt.Println("			", rf.Log[i], " ENTRY")
	//}
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
	rf.Killed = true
	//rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) runServer() {

	rand.Seed(time.Now().UnixNano())
	random := rand.Intn(100)
	rand.Seed(int64(random))
	timeout := time.Duration(rand.Intn(200)+300) * time.Millisecond
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
				//fmt.Println(time.Now().Sub(rf.previousHeartBeatTime), " /----/ ", timeout, " XAXA")
				//rf.mu.Lock()
				rf.State = 1
				rf.persist()
				//fmt.Println(":", rf.me, "--------------EKANA TMT KAI KANW EKLOGES GIA TO TERM ", rf.CurrentTerm+1, "META APO ", timeout)
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
	//////fmt.Println("---------------------------------------------------------------------------------EXOUME EKLOGES GIA TO TERM: ", rf.CurrentTerm, " APO TON: ", rf.me, " ME STATE: ", rf.State)
	//rf.mu.Lock()
	VOTING_NOW := true
	votesNeeded := rf.numberOfPeers / 2
	votesReceived := 1
	newVote := make(chan vote)

	rand.Seed(time.Now().UnixNano())
	random := rand.Intn(100)
	rand.Seed(int64(random))
	timeout2 := time.Duration(rand.Intn(200)+300) * time.Millisecond

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
				////fmt.Println(":", rf.me, " KAI STELNW REQUEST VOTE STON ", i, "GIA TO TERM ", rf.CurrentTerm)
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
						rf.mu.Unlock()
						rf.persist()
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
					rf.NextIndex[j] = len(rf.Log)
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
					args.Entries = rf.Log[rf.NextIndex[i]:]
				}
			}

			//go rf.sendAppendEntries(i, &args, &AppendEntriesReply{})
			//////fmt.Println(":", rf.me, "ME STATE: ", rf.State, "KAI STELNW APPEND STON ", i, "GIA TO TERM ", rf.CurrentTerm, "TIME: ")
			go func(i int, args AppendEntriesArgs, reply AppendEntriesReply) {
				////fmt.Println(":", rf.me, "ME STATE: ", rf.State, "KAI STELNW APPEND STON ", i, "GIA TO TERM ", rf.CurrentTerm, "TIME: ")
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
// save its persistent State, and also initially holds the most
// recent saved State, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	//fmt.Println("-------------------------------------------------------------------")
	//////fmt.Println("-------------------------------------------------------------------")
	//////fmt.Println("-------------------------------------------------------------------")
	//////fmt.Println("-------------------------------------------------------------------")

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

	// initialize from State persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.runServer()
	// Your initialization code here (2A, 2B, 2C).

	return rf
}

*/
