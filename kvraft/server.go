package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation    string
	Key          string
	Value        string
	ClientId     int64
	SerialNumber int64
}

type KVServer struct {
	mu        sync.Mutex
	me        int
	rf        *raft.Raft
	applyCh   chan raft.ApplyMsg
	dead      int32 // set by Kill()
	persister *raft.Persister

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	KV                 map[string]string
	ClientSerialNumber map[int64]int64
	Pending            map[int]chan Op
}

func (kv *KVServer) ApplyMessages() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			//fmt.Println("54 locking")
			kv.mu.Lock()
			//fmt.Println("56 locked")
			op := msg.Command.(Op)

			if kv.ClientSerialNumber[op.ClientId] < op.SerialNumber {
				kv.ClientSerialNumber[op.ClientId] = op.SerialNumber
				if op.Operation == "PUT" {
					kv.KV[op.Key] = op.Value
				} else if op.Operation == "APPEND" {
					kv.KV[op.Key] += op.Value
				}
			}

			ch, exists := kv.Pending[msg.CommandIndex]
			if exists {
				ch <- op
				close(ch)
				delete(kv.Pending, msg.CommandIndex)
			}

			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)

				err := e.Encode(kv.KV)
				if err != nil {
					fmt.Println("kv.KV not encoded")
					return
				}
				err = e.Encode(kv.ClientSerialNumber)
				if err != nil {
					fmt.Println("kv.ClientSerialNumber not encoded")
					return
				}
				snapshot := w.Bytes()

				kv.rf.Snapshot(msg.CommandIndex, snapshot)
			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			//fmt.Println("95 locking")
			kv.mu.Lock()
			//fmt.Println("97 locked")

			r := bytes.NewBuffer(msg.Snapshot)
			d := labgob.NewDecoder(r)

			var kvData map[string]string
			err := d.Decode(&kvData)
			if err != nil {
				fmt.Println("kvData decode error")
				kv.mu.Unlock()
				return
			}

			var clientSN map[int64]int64
			err = d.Decode(&clientSN)
			if err != nil {
				fmt.Println("clientSN decode error")
				kv.mu.Unlock()
				return
			}

			kv.KV = kvData
			kv.ClientSerialNumber = clientSN

			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//fmt.Println("128 locking")
	kv.mu.Lock()
	//fmt.Println("130 locked")

	if kv.ClientSerialNumber[args.ClientId] >= args.SerialNumber {
		value, exists := kv.KV[args.Key]
		if !exists {
			reply.Err = ErrNoKey
		} else {
			reply.Value = value
			reply.Err = OK
		}
		kv.mu.Unlock()
		return
	}

	op := Op{"GET", args.Key, "", args.ClientId, args.SerialNumber}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}

	notify := make(chan Op, 1)
	kv.Pending[index] = notify
	kv.mu.Unlock()

	//for {
	select {
	case result := <-notify:
		//fmt.Println("160 locking")
		kv.mu.Lock()
		//fmt.Println("162 locked")

		currTerm, isLeader := kv.rf.GetState()
		if !isLeader || currTerm != term || result != op {
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		}

		value, exists := kv.KV[args.Key]
		if !exists {
			reply.Err = ErrNoKey
		} else {
			reply.Value = value
			reply.Err = OK
		}

		kv.mu.Unlock()
		return

	case <-time.After(400 * time.Millisecond):
		//fmt.Println("183 locking")
		//kv.mu.Lock()
		//fmt.Println("185 locked")
		/*currTerm, isLeader := kv.rf.GetState()
		if !isLeader || currTerm != term {
			delete(kv.Pending, index)
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()*/
		reply.Err = ErrWrongLeader
		return
	}
	//}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//fmt.Println("200 locking")
	kv.mu.Lock()
	//fmt.Println("202 locked")

	if kv.ClientSerialNumber[args.ClientId] >= args.SerialNumber {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	op := Op{"PUT", args.Key, args.Value, args.ClientId, args.SerialNumber}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}

	notify := make(chan Op, 1)
	kv.Pending[index] = notify
	kv.mu.Unlock()

	//for {
	select {
	case result := <-notify:
		//fmt.Println("226 locking")
		kv.mu.Lock()
		//fmt.Println("228 locked")

		currTerm, isLeader := kv.rf.GetState()
		if !isLeader || currTerm != term || result != op {
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		}

		reply.Err = OK
		kv.mu.Unlock()
		return

	case <-time.After(400 * time.Millisecond):
		//fmt.Println("242 locking")
		//kv.mu.Lock()
		//fmt.Println("244 locked")
		/*currTerm, isLeader := kv.rf.GetState()
		if !isLeader || currTerm != term {
			delete(kv.Pending, index)
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()*/
		reply.Err = ErrWrongLeader
		return
	}
	//}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//fmt.Println("259 locking")
	kv.mu.Lock()
	//fmt.Println("261 locked")

	if kv.ClientSerialNumber[args.ClientId] >= args.SerialNumber {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	op := Op{"APPEND", args.Key, args.Value, args.ClientId, args.SerialNumber}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}

	notify := make(chan Op, 1)
	kv.Pending[index] = notify
	kv.mu.Unlock()

	//for {
	select {
	case result := <-notify:
		//fmt.Println("285 locking")
		kv.mu.Lock()
		//fmt.Println("287 locked")

		currTerm, isLeader := kv.rf.GetState()
		if !isLeader || currTerm != term || result != op {
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		}

		reply.Err = OK
		kv.mu.Unlock()
		return

	case <-time.After(400 * time.Millisecond):
		//fmt.Println("301 locking")
		//kv.mu.Lock()
		//fmt.Println("303 locked")
		/*currTerm, isLeader := kv.rf.GetState()
		if !isLeader || currTerm != term {
			delete(kv.Pending, index)
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()*/
		reply.Err = ErrWrongLeader
		return
	}
	//}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.KV = make(map[string]string)
	kv.ClientSerialNumber = make(map[int64]int64)
	kv.Pending = make(map[int]chan Op)

	snapshot := persister.ReadSnapshot()
	if snapshot != nil && len(snapshot) > 0 {
		//fmt.Println("369 locking")
		kv.mu.Lock()
		//fmt.Println("371 locked")

		r := bytes.NewBuffer(snapshot)
		d := labgob.NewDecoder(r)

		var kvData map[string]string
		err := d.Decode(&kvData)
		if err != nil {
			fmt.Println("kvData decode error")
		}

		var clientSN map[int64]int64
		err = d.Decode(&clientSN)
		if err != nil {
			fmt.Println("clientSN decode error")
		}

		kv.KV = kvData
		kv.ClientSerialNumber = clientSN

		kv.mu.Unlock()
	}
	go kv.ApplyMessages()

	return kv
}
