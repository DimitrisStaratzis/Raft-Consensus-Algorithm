package raftkv

import (
	"bytes"
	//"debug/elf"
	"encoding/gob"
	"fmt"
	"labrpc"
	//"math/rand"
	"raft"
	"sync"
	"time"
)

//func DPrintf(format string, a ...interface{}) (n int, err error) {
//	if Debug > 0 {
//		log.Printf(format, a...)
//	}
//	return
//}

func (kv *RaftKV) saveSnapshot(index int) {

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	//_ = e.Encode(kv.PendingCommitedOperations)
	_ = e.Encode(kv.PutAppedOperations)
	_ = e.Encode(kv.GetOperations)
	_ = e.Encode(kv.Store)
	//_ = e.Encode(kv.Persister)

	data := w.Bytes()
	kv.Persister.SaveSnapshot(data)

	kv.rf.CompactLog(index)

	// Compact raft log til index.
	//kv.rf.CompactLog(logIndex)
}

func (kv *RaftKV) ReadSnapshot(data []byte) {

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	//_ = d.Decode(&kv.PendingCommitedOperations)
	_ = d.Decode(&kv.PutAppedOperations)
	_ = d.Decode(&kv.GetOperations)
	_ = d.Decode(&kv.Store)
	//_ = d.Decode(&kv.Persister)
}

type Op struct {
	Type      string
	Key       string
	Value     string
	Cid       int64
	SequenceN int
	Leader    bool

	//Seq		int

	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate              int // snapshot if log grows this big
	PendingCommitedOperations map[int]chan raft.ApplyMsg
	Store                     map[string]string
	PutAppedOperations        map[int64]int
	GetOperations             map[int64]int
	killed                    bool
	snapshotsEnabled          bool
	Persister                 *raft.Persister

	// Your definitions here.
}

//
func (kv *RaftKV) ListenerForCommitedEntries() {
	//waitingForAppliedCommands := true
	var leader bool

	for {

		kv.mu.Lock()
		killed := kv.killed
		kv.mu.Unlock()
		if killed && len(kv.PendingCommitedOperations) == 0 {
			return
		}
		select {
		case applyMsg := <-kv.applyCh:

			kv.mu.Lock()
			killed := kv.killed
			kv.mu.Unlock()
			if killed && len(kv.PendingCommitedOperations) == 0 {
				return
			}

			if applyMsg.UseSnapshot {
				kv.mu.Lock()
				kv.ReadSnapshot(applyMsg.Snapshot)
				kv.mu.Unlock()
				continue
			}

			operation := applyMsg.Command.(Op)
			index := applyMsg.Index
			_, leader = kv.rf.GetState()

			kv.mu.Lock()
			//
			fmt.Println(kv.me, " Apply message with operation:, ", operation.Type, " for index: ", index, " arrived", "raft is leader: ", leader)

			sequenceN, putExists := kv.PutAppedOperations[operation.Cid]
			sequenceNG, getExists := kv.GetOperations[operation.Cid]

			if operation.Type == "Put" {
				if (operation.SequenceN > sequenceN) || (leader && !putExists) {
					fmt.Println(kv.me, " Client: ", operation.Cid, " KV putting value: ", operation.Value, " at key: ", operation.Key, " at server: ", kv.me)
					kv.Store[operation.Key] = operation.Value
					fmt.Println(kv.me, " Client: ", operation.Cid, " Store at: ", operation.Key, " now has value: ", kv.Store[operation.Key], "\n\n")
					kv.PutAppedOperations[operation.Cid] = operation.SequenceN
				}

			} else if operation.Type == "Append" {
				if (operation.SequenceN > sequenceN) || (leader && !putExists) {
					fmt.Println(kv.me, " Client: ", operation.Cid, " KV appending value: ", operation.Value, " at key: ", operation.Key, " at server: ", kv.me)
					kv.Store[operation.Key] += operation.Value
					fmt.Println(kv.me, " Client: ", operation.Cid, " Store at: ", operation.Key, " now has value: ", kv.Store[operation.Key], "\n\n")
					kv.PutAppedOperations[operation.Cid] = operation.SequenceN

				}
			} else {
				if (operation.SequenceN > sequenceNG) || (leader && !getExists) {
					fmt.Println(kv.me, " Client: ", operation.Cid, " KV retrieving value: ", operation.Value, "from key: ", operation.Key)
					kv.GetOperations[operation.Cid] = operation.SequenceN
				}
			}

			if leader {
				ch, ok := kv.PendingCommitedOperations[index]
				if ok {
					fmt.Println(kv.me, " Client: ", operation.Cid, " Got channel for index: ", index, " and operation:  ", operation.Type)
					ch <- applyMsg
				} else {
					fmt.Println(kv.me, " Client: ", operation.Cid, " Didnt get channel for index: ", index, " and operation:  ", operation.Type)
				}
			}

			//if kv.snapshotsEnabled && 1-(kv.Persister.RaftStateSize()/kv.maxraftstate) <= 5/100 {
			kv.saveSnapshot(applyMsg.Index)
			//}

			kv.mu.Unlock()
			//kv.saveSnapshot()

			//rf.mu.Unlock()
		default:
			//if leader{
			//	//
			//fmt.Println(kv.me, " Waiting for applyMessages")
			//
			//}

			//maybe wait for some time.
			//if kv.killed {
			//	return
			//}
		}
	}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	//
	// Your code here.
	operation := Op{Type: "Get", Key: args.Key}

	fmt.Println(kv.me, " Client:  ", operation.Cid, " received get")

	index, _, isLeader := kv.rf.Start(operation)

	if !isLeader {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false

	fmt.Println(kv.me, " Client: ", operation.Cid, " KV sent get with key: ", operation.Key)

	newChannel := make(chan raft.ApplyMsg)
	kv.mu.Lock()
	kv.PendingCommitedOperations[index] = newChannel
	kv.mu.Unlock()
	fmt.Println(kv.me, " Client: ", operation.Cid, " KV Made new channel at index: ", index)

	select {
	case incomingMsg := <-newChannel:
		fmt.Println(kv.me, " Client: ", operation.Cid, " KV get operation was applied and I received it at index: ", index)

		kv.mu.Lock()
		killed := kv.killed
		kv.mu.Unlock()
		if killed && len(kv.PendingCommitedOperations) == 0 {
			return
		}
		op := incomingMsg.Command.(Op)
		kv.mu.Lock()
		delete(kv.PendingCommitedOperations, index)
		kv.mu.Unlock()

		if op == operation {
			fmt.Println(kv.me, " Client: ", operation.Cid, " Store: ", kv.Store, " value: ", kv.Store[op.Key])
			fmt.Println(kv.me, " Client: ", operation.Cid, " KV finished, returning Store vale: ", kv.Store[op.Key], " from server: ", kv.me)
			//var valueExists bool
			value, exists := kv.Store[op.Key]

			if exists {
				reply.Value = value
				reply.Err = OK
			} else {
				reply.Value = ErrNoKey
			}

			return
		}
	case <-time.After(2000 * time.Millisecond):
		fmt.Println(kv.me, " Client: ", operation.Cid, " get expired for key: ", args.Key)
		reply.WrongLeader = true
		kv.mu.Lock()
		delete(kv.PendingCommitedOperations, index)
		kv.mu.Unlock()
		return
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	operation := Op{Type: args.Op, Key: args.Key, Value: args.Value, Cid: args.Cid, SequenceN: args.SequenceN}

	fmt.Println(kv.me, " Client: ", operation.Cid, " received putappend")

	index, _, isLeader := kv.rf.Start(operation)

	if !isLeader {
		//
		fmt.Println(kv.me, " Client: ", operation.Cid, " putAppend failed because I am not leader ")
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false

	fmt.Println(kv.me, " Client: ", operation.Cid, " KV sent PutAppend with key: ", operation.Key, " value: ", operation.Value)

	newChannel := make(chan raft.ApplyMsg)
	kv.mu.Lock()
	kv.PendingCommitedOperations[index] = newChannel
	kv.mu.Unlock()
	//
	fmt.Println(kv.me, " Client: ", operation.Cid, " KV Made new channel at index: ", index)

	select {
	case incomingMsg := <-newChannel:
		kv.mu.Lock()
		killed := kv.killed
		kv.mu.Unlock()
		if killed && len(kv.PendingCommitedOperations) == 0 {
			return
		}
		fmt.Println(kv.me, " Client: ", operation.Cid, " KV PutAppend operation was applied and I received it at index: ", index)

		op := incomingMsg.Command.(Op)
		kv.mu.Lock()
		delete(kv.PendingCommitedOperations, index)
		kv.mu.Unlock()

		if op == operation {
			reply.WrongLeader = false
			return
		}

	case <-time.After(2000 * time.Millisecond):
		//
		fmt.Println(kv.me, " Client: ", operation.Cid, " putAppend expired for key: ", args.Key, " and value: ", args.Value)
		reply.WrongLeader = true
		kv.mu.Lock()
		delete(kv.PendingCommitedOperations, index)
		kv.mu.Unlock()
		reply.timedOut = true
		return

	}

}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	time.Sleep(1200 * time.Millisecond)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.killed = true
	kv.rf.Kill()
	//kv.PendingCommitedOperations = make(map[int]chan raft.ApplyMsg)
	//kv.PutAppedOperations = make (map[int64]int)
	//kv.GetOperations = make (map[int64]int)

	//kv.saveSnapshot()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should Store snapshots with Persister.SaveSnapshot(),
// and Raft should save its state (including log) with Persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.snapshotsEnabled = true
	//
	fmt.Println(kv.me, " KV created")
	kv.maxraftstate = maxraftstate
	kv.Store = make(map[string]string)
	kv.PendingCommitedOperations = make(map[int]chan raft.ApplyMsg)
	kv.Persister = persister

	kv.PutAppedOperations = make(map[int64]int)
	kv.GetOperations = make(map[int64]int)
	kv.killed = false

	// Your initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)

	if data := persister.ReadSnapshot(); kv.snapshotsEnabled && data != nil && len(data) > 0 {
		//fmt.Println(kv.me, " I loaded data from saveSnapshot")
		kv.ReadSnapshot(data)
	}

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.ListenerForCommitedEntries()

	return kv
}
