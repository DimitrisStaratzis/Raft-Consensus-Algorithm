package raftkv

import (
	"crypto/rand"
	"fmt"
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
	fmt.Print("--------RANDOM IS:", x)
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	//time.Sleep(time.Millisecond*800)
	ck := new(Clerk)
	ck.servers = servers
	ck.cid = nrand()
	ck.sequenceN = 1
	ck.sequenceNGet = 1
	ck.Leader = -1
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
	ck.sequenceNGet++
	if Debug {
		//
		//
		fmt.Println("GET operation requested for key: ", key, " sequence: ", ck.sequenceNGet)
	}

	//ck.sequenceNGet++
	args := GetArgs{
		Key:       key,
		SequenceN: ck.sequenceNGet,
		Cid:       ck.cid}
	j := 0
	//time.Sleep(time.Millisecond*500)
	for { //i:=0; i < len(ck.servers); i++{
		////
		//
		fmt.Println("sendin")
		var reply GetReply
		var ok bool
		in := j % len(ck.servers)
		//
		//
		fmt.Println("sendingG  ", in)

		ok = ck.servers[in].Call("RaftKV.Get", &args, &reply)
		if ok {
			if !reply.WrongLeader {
				if reply.Err == ErrNoKey {
					return ""
				}
				if Debug {
					//
					//
					fmt.Println(in, " Will return: ", reply.Value)
				}
				return reply.Value
			} else {
				//
				//
				fmt.Println(in, " wrong leader")
				//if reply.killed{
				//
				//	break
				//}
			}
		} else {
			//
			//
			fmt.Println("RPC for get failed")
		}
		j++
	}
	//
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
		//
		//
		fmt.Println("PutAppend operation requested with key: ", key, " and value: ", value, " Sequence: ", ck.sequenceN)
	}
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Cid:       ck.cid,
		SequenceN: ck.sequenceN,
		Op:        op}

	for {
		fmt.Println("sendinP ")
		var reply PutAppendReply
		in := i % len(ck.servers)

		ok := ck.servers[in].Call("RaftKV.PutAppend", &args, &reply)

		fmt.Println("sentA  ", in)
		if ok {
			if !reply.WrongLeader {
				//
				//
				fmt.Println("PutAppend success")

				return
			} else {
				//
				//
				fmt.Println(in, " Wrong leader")
				//if reply.killed{
				//	break
				//}
			}
		}
		i++
	}
	//

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
