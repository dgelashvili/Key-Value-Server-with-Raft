package kvraft

import (
	"6.5840/labrpc"
	"fmt"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	ClientId     int64
	SerialNumber int64
	Leader       int
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
	// You'll have to add code here.
	ck.ClientId = nrand()
	ck.SerialNumber = 0
	ck.Leader = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.SerialNumber++

	args := GetArgs{key, ck.ClientId, ck.SerialNumber}
	reply := GetReply{}

	server := ck.Leader
	for {
		ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err == OK {
			ck.Leader = server
			return reply.Value
		}
		if ok && reply.Err == ErrNoKey {
			ck.Leader = server
			return ""
		}
		if !ok || reply.Err == ErrWrongLeader {
			server = (server + 1) % len(ck.servers)
			time.Sleep(10 * time.Millisecond)
		} else {
			break
		}
	}

	fmt.Println("How did we get here?")
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.SerialNumber++

	args := PutAppendArgs{key, value, ck.ClientId, ck.SerialNumber}
	reply := PutAppendReply{}

	server := ck.Leader
	for {
		ok := ck.servers[server].Call("KVServer."+op, &args, &reply)
		if ok && reply.Err == OK {
			ck.Leader = server
			return
		}
		if ok && reply.Err == ErrNoKey {
			ck.Leader = server
			return
		}
		if !ok || reply.Err == ErrWrongLeader {
			server = (server + 1) % len(ck.servers)
			time.Sleep(10 * time.Millisecond)
		} else {
			break
		}
	}

	fmt.Println("How did we put/append here?")
	return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
