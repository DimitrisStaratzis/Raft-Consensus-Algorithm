package raftkv

import (
	"crypto/rand"
	"fmt"
	//"time"

	//"go/ast"
	"labrpc"
	"math/big"
)

//"time"

//"time"

const Debug = true

type Clerk struct {
	servers      []*labrpc.ClientEnd
	sequenceN    int
	sequenceNGet int
	cid          int64
	Leader       int
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.cid = nrand()
	ck.sequenceN = 0
	ck.sequenceNGet = 0
	ck.Leader = -1
	fmt.Println("CLIENT MADE")
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	//TODO OPTIMIZATION, KEEP LEADER
	if Debug {
		fmt.Println("GET operation requested for key: ", key, " sequence: ", ck.sequenceNGet)
	}
	ck.sequenceNGet++
	args := GetArgs{
		Key:       key,
		SequenceN: ck.sequenceNGet,
		Cid:       ck.cid}
	j := 0
	for {
		fmt.Println("sendin")
		var reply GetReply
		var ok bool
		var index int

		if ck.Leader != -1 {
			index = ck.Leader
		} else {
			index = j % len(ck.servers)
		}
		fmt.Println("sendingGet  to server: ", index)
		ok = ck.servers[index].Call("RaftKV.Get", &args, &reply)
		if ok {
			if !reply.WrongLeader {
				if reply.Err == ErrNoKey {
					return ""
				}
				if Debug {
					fmt.Println(index, " Will return: ", reply.Value)
				}
				ck.Leader = index
				return reply.Value
			} else {
				fmt.Println(index, " wrong leader")
				ck.Leader = -1
			}
		} else {
			fmt.Println("RPC for get failed for server: ", index)
			ck.Leader = -1
		}
		j++
		//time.Sleep(10 * time.Millisecond)

	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	// You will have to modify this function.
	//TODO OPTIMIZATION, KEEP LEADER

	i := 0
	ck.sequenceN++
	if Debug {
		fmt.Println("PutAppend operation requested with key: ", key, " and value: ", value, " Sequence: ", ck.sequenceN)
	}
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Cid:       ck.cid,
		SequenceN: ck.sequenceN,
		Op:        op}

	for {
		var reply PutAppendReply
		var index int

		if ck.Leader != -1 {
			index = ck.Leader
		} else {
			index = i % len(ck.servers)
		}
		fmt.Println("sendinPutAppend to server:  ", index)

		ok := ck.servers[index].Call("RaftKV.PutAppend", &args, &reply)

		if ok {
			if !reply.WrongLeader {
				fmt.Println("PutAppend success")
				ck.Leader = index
				return
			} else {
				fmt.Println(index, " Wrong leader")
				ck.Leader = -1
			}
		} else {
			fmt.Println("RPC for putAppend failed for server: ", index)
			ck.Leader = -1
		}
		i++
		//time.Sleep(10 * time.Millisecond)

	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
