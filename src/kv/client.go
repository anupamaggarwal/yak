package kv

import "net/rpc"
import "crypto/rand"
import "math/big"
import "sync"
import "time"
import "glog"

type Clerk struct {
	servers  []string
	mu       sync.Mutex
	xidHeard int64 //which RPC this client has heard a reply from so that server can disregard this
	uuid     int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	//assign a random id for identification
	ck.uuid = nrand()
	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
//
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	glog.Infoln(err)
	return false
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	//generate a unique xid
	xid := nrand()
	retVal := ""
	//contact every server for the operation in round robin fashion break if you got a response else move on
	getReq := &GetArgs{}
	getReq.Xid = xid
	getReq.Key = key
	getRep := &GetReply{}
	tries := 0

	for {
		tries = 1 + tries
		glog.Infoln(ck.uuid, " client trying for ", tries)
		time.Sleep(100 * time.Millisecond)
		index := 0
		for index < len(ck.servers) {
			glog.Infoln(ck.uuid, "Get Calling server ", index, " for request ", ck.servers)
			ret := call(ck.servers[index], "KVPaxos.Get", getReq, getRep)
			if ret {
				//we got a response check if invalid response in which case try again
				if getRep.Err != OK {
					index++
					continue
				}
				retVal = getRep.Value
				return retVal
			} else {
				glog.Infoln(ck.uuid, "RPC broke retrying get request with another server", ck.servers)
				index++
			}
		}
	}
	return retVal
}

//
// shared by Put and Append.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	xid := nrand()
	putReq := &PutAppendArgs{}
	putRep := &PutAppendReply{}
	putReq.Xid = xid
	putReq.Value = value
	putReq.Op = op
	putReq.Key = key
	tries := 0

	for {
		time.Sleep(100 * time.Millisecond)
		tries = tries + 1
		glog.Infoln(" Loop count", tries)
		for index, _ := range ck.servers {
			glog.Infoln(ck.uuid, "PutAppend Calling server ", index, " for request ", putReq.Op)
			ret := call(ck.servers[index], "KVPaxos.PutAppend", putReq, putRep)
			if ret {
				//we got a response check if invalid response in which case try again
				if putRep.Err != OK {
					continue
				}
				return
			} else {
				glog.Warningln(ck.uuid, "RPC broke, retrying for a different server ", ck.servers)
				continue
			}
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
