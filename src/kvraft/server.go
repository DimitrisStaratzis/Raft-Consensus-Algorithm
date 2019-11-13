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

func (kv *RaftKV) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	//_ = e.Encode(kv.PendingCommitedOperations)
	_ = e.Encode(kv.PutAppedOperations)
	_ = e.Encode(kv.GetOperations)
	_ = e.Encode(kv.Store)
	//_ = e.Encode(kv.Persister)

	data := w.Bytes()
	kv.Persister.SaveSnapshot(data)

	// Compact raft log til index.
	//kv.rf.CompactLog(logIndex)
}

func (kv *RaftKV) loadPersist(data []byte) {

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
		if kv.killed {
			break
		}

		select {
		case applyMsg := <-kv.applyCh:

			operation := applyMsg.Command.(Op)
			index := applyMsg.Index
			leader = applyMsg.Leader

			kv.mu.Lock()
			//
			fmt.Println(kv.me, " Apply message with operation:, ", operation.Type, " for index: ", index, " arrived", "raft is leader: ", leader)

			sequenceN, _ := kv.PutAppedOperations[operation.Cid]
			sequenceNG, _ := kv.GetOperations[operation.Cid]

			if operation.Type == "Put" {
				if operation.SequenceN > sequenceN {
					fmt.Println(kv.me, " KV putting value: ", operation.Value, " at key: ", operation.Key, " at server: ", kv.me)
					kv.Store[operation.Key] = operation.Value
					fmt.Println(kv.me, " Store at: ", operation.Key, " now has value: ", kv.Store[operation.Key], "\n\n")
					kv.PutAppedOperations[operation.Cid] = operation.SequenceN
					fmt.Println(kv.me, " Duplicate detection updates succesfully")
				}

			} else if operation.Type == "Append" {
				if operation.SequenceN > sequenceN {
					fmt.Println(kv.me, " KV appending value: ", operation.Value, " at key: ", operation.Key, " at server: ", kv.me)
					kv.Store[operation.Key] += operation.Value
					fmt.Println(kv.me, " Store at: ", operation.Key, " now has value: ", kv.Store[operation.Key], "\n\n")
					kv.PutAppedOperations[operation.Cid] = operation.SequenceN
					fmt.Println(kv.me, " Duplicate detection updates succesfully")

				}
			} else {
				if operation.SequenceN > sequenceNG {
					fmt.Println(kv.me, " KV retrieving value: ", operation.Value, "from key: ", operation.Key)
					kv.GetOperations[operation.Cid] = operation.SequenceN
					fmt.Println(kv.me, " Duplicate detection updates succesfully")
				}
			}
			kv.persist()

			if leader {
				ch, ok := kv.PendingCommitedOperations[index]

				if ok {
					//
					fmt.Println(kv.me, " Got channel")
					ch <- applyMsg
				} else {
					//
					fmt.Println(kv.me, " Didnt get channel")
					////
					fmt.Println(kv.PendingCommitedOperations)

				}
			}
			//kv.persist()
			kv.mu.Unlock()

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
	fmt.Println(kv.me, " received get")
	// Your code here.
	operation := Op{Type: "Get", Key: args.Key}

	index, _, isLeader := kv.rf.Start(operation)

	if !isLeader {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false

	fmt.Println(kv.me, " KV sent get with key: ", operation.Key)

	newChannel := make(chan raft.ApplyMsg)
	kv.mu.Lock()
	kv.PendingCommitedOperations[index] = newChannel
	kv.mu.Unlock()
	//
	fmt.Println(kv.me, " KV Made new channel at index: ", index)

	select {
	case incomingMsg := <-newChannel:
		//
		fmt.Println(kv.me, " KV get operation was applied and I received it at index: ", index)

		op := incomingMsg.Command.(Op)

		if op == operation {

			fmt.Println(kv.me, " Store: ", kv.Store, " value: ", kv.Store[op.Key])
			//
			fmt.Println(kv.me, " KV finished, returning Store vale: ", kv.Store[op.Key], " from server: ", kv.me)
			//var valueExists bool
			value, exists := kv.Store[op.Key]

			if exists {
				reply.Value = value
				reply.Err = OK
			} else {
				reply.Value = ErrNoKey
			}
			kv.mu.Lock()
			delete(kv.PendingCommitedOperations, index)
			kv.mu.Unlock()
			return
		}

		//rf.mu.Unlock()
	case <-time.After(800 * time.Millisecond):
		//
		fmt.Println(kv.me, " get expired for key: ", args.Key)
		reply.WrongLeader = true
		kv.mu.Lock()
		delete(kv.PendingCommitedOperations, index)
		kv.mu.Unlock()
		return

	}

}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	//timeStarted := time.Now()
	//
	fmt.Println(kv.me, " received putappend")

	operation := Op{Type: args.Op, Key: args.Key, Value: args.Value, Cid: args.Cid, SequenceN: args.SequenceN}

	index, _, isLeader := kv.rf.Start(operation)

	if !isLeader {
		//
		fmt.Println(kv.me, " putAppend failed because I am not leader ")
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false

	fmt.Println(kv.me, " KV sent PutAppend with key: ", operation.Key, " value: ", operation.Value)

	newChannel := make(chan raft.ApplyMsg)
	kv.mu.Lock()
	kv.PendingCommitedOperations[index] = newChannel
	kv.mu.Unlock()
	//
	fmt.Println(kv.me, " KV Made new channel at index: ", index)

	select {
	case incomingMsg := <-newChannel:
		//
		fmt.Println(kv.me, " KV PutAppend operation was applied and I received it at index: ", index)

		op := incomingMsg.Command.(Op)

		if op == operation {
			reply.WrongLeader = false
			kv.mu.Lock()
			delete(kv.PendingCommitedOperations, index)
			kv.mu.Unlock()
			return

		} else {
			reply.WrongLeader = true
			return

		}

	case <-time.After(800 * time.Millisecond):
		//
		fmt.Println(kv.me, " putAppend expired for key: ", args.Key, " and value: ", args.Value)
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
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.killed = true
	kv.rf.Kill()

	//kv.PendingCommitedOperations = make(map[int]chan raft.ApplyMsg)
	//kv.persist()
	//
	//kv.PutAppedOperations = make (map[int64]int)
	//kv.GetOperations = make (map[int64]int)
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
		fmt.Println(kv.me, " I loaded data from persist")
		kv.loadPersist(data)
	}

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.ListenerForCommitedEntries()

	return kv
}
