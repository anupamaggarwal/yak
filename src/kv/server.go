package kv

import "net"

import "net/rpc"
import "glog"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "encoding/gob"
import "time"

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Operation int

const (
	Get Operation = iota + 1
	Put
	Append
)

type Op struct {
	Optype Operation
	Server int
	Xid    int64
	Key    string
	Value  string
}

var dummyOp = Op{Get, 0, 0, "test", ""}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos
	kvData     map[string]string
	seenMap    map[int64]bool
	toprocess  int
}

func (kv *KVPaxos) waitForPaxosAgreement(seq int, op Op) bool {

	status, val := kv.px.Status(seq)

	if status == paxos.Decided {
		//check if value was truly what we sent originally or some morphed value
		// check if val was what was trying to be inserted

		if val.(Op).Xid == op.Xid {
			glog.Infoln(kv.me, "===>> Agreement reached at seq with value", seq, " with value:", val)
			return true
		} else {
			glog.Infoln(kv.me, "++++Agreement reached for seq by other Server#:", val.(Op).Server, " for seq ", seq, " processed till now:", kv.toprocess, " Min/Max", kv.px.Min(), "/", kv.px.Max())
			return false
		}
	}
	if status == paxos.Forgotten {
		glog.Infoln("WARNING...!!!")
		//this has already been applied by the application cannot agree on this
		return false

	}
	//in pending we wait
	return false
}

//apply all operations while keeping track of xid starting from minIndex to maxIndex all included
//assumes lock is held on the map
func (kv *KVPaxos) applyBackLogOps(maxIndx int) {
	glog.Infoln(kv.me, "<<<Applying from ", kv.toprocess, " to ", maxIndx, ">>>")
	index := kv.toprocess
	iter := 1

	kv.mu.Lock()
	for index <= maxIndx {
		//status is guranteed to be decide here or else we woudn't have reached here

		status, val := kv.px.Status(index)
		//glog.Infoln(kv.me, "reevaluating status at ", index, " status:", status)

		if status == paxos.Decided {
			paxosStruct := val.(Op)
			//apply stuff in paxosStruct and return result
			glog.Infoln(kv.me, "SUCCESS:Applying at index ", index, " operation ", paxosStruct.Optype)
			switch paxosStruct.Optype {
			case Get:
				if _, ok := kv.seenMap[paxosStruct.Xid]; !ok {
					//old get which can be skipped
					glog.Infoln(kv.me, "Applied get fo  key", paxosStruct.Key)
					kv.seenMap[paxosStruct.Xid] = true
				}
			case Put:
				if _, ok := kv.seenMap[paxosStruct.Xid]; !ok {
					kv.kvData[paxosStruct.Key] = paxosStruct.Value
					glog.Infoln(kv.me, "Applied put for value", paxosStruct.Value, " key", paxosStruct.Key)
					kv.seenMap[paxosStruct.Xid] = true
				}
			case Append:

				if _, ok := kv.seenMap[paxosStruct.Xid]; !ok {
					kv.kvData[paxosStruct.Key] = kv.kvData[paxosStruct.Key] + paxosStruct.Value
					glog.Infoln(kv.me, "Applied append for value", paxosStruct.Value, " key", paxosStruct.Key)
					kv.seenMap[paxosStruct.Xid] = true
				}
			}
			glog.Infoln(kv.me, "State of my server", kv.kvData, "with index:::", index)
			kv.px.Done(index)
			index++
			kv.toprocess = index
		} else {
			if status == paxos.Pending {
				glog.Infoln(kv.me, "Iteration completed", iter, " for seq", index)
				iter++
				glog.Infoln("STARTING paxos again to converge at index:", index)
				//try to converge by applying value last accepted
				kv.px.Start(index, dummyOp)
				time.Sleep(100 * time.Millisecond)
				continue
			} else {
				glog.Infoln(kv.me, "WARNING!! Paxos forgotten for index", index, " at val", val)
				index++
			}

		}
	}

	kv.mu.Unlock()

}

func (kv *KVPaxos) process(indexOfAgreement int) {
	//processing value in op
	// first time we are seeing this xid need to process this
	glog.Infoln(kv.me, "In process with index of agreement", indexOfAgreement, "with current kvtprocess", kv.toprocess)
	kv.applyBackLogOps(indexOfAgreement) //these are both inclusive
	//only signal done till applyBackLogOps
	//increment kv.toprocess
	//finally call done here signalling we are done processing this

}

func (kv *KVPaxos) paxosAgreement(op Op) bool {
	//initiates a paxos agreement for this operation
	glog.Infoln(kv.me, "Initiating Paxos agreement for ", op)
	//Start with a sequence number and increment it if it fails
	//if agreement reached check if it was really for the value I suggested or not

	agreementReached := false
	index := kv.px.Max() + 1

	to := 100 * time.Millisecond
	for !agreementReached {
		glog.Infoln(kv.me, "[paxosAgreement]Index to try for paxos agreement", index)
		kv.px.Start(index, op)
		time.Sleep(to)
		agreementReached = kv.waitForPaxosAgreement(index, op)
		if agreementReached {
			//agreement has been reached interpret value from last time around keeping into account xid
			kv.process(index)
			break
		} else {
			if to < 10*time.Second {
				to = to + 50
			}
			glog.Infoln(kv.me, " SPINNING, no decision for seq number ", index, " trying higher number")
			index++
		}
		// if agreement reached then act on it, evaluate state uptill last
		//wait till status turns to agreed
	}
	return false
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	glog.Infoln(kv.me, "[GET]Got client request -->", args)
	//this server is a client for the paxos library
	//initiate a paxos agreement for this operation
	op := Op{}
	op.Optype = Get
	op.Xid = args.Xid
	op.Key = args.Key
	op.Server = kv.me
	kv.paxosAgreement(op)
	reply.Value = kv.kvData[args.Key]
	reply.Err = OK
	glog.Infoln(kv.me, "[Get] returned ", reply.Value)
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	glog.Infoln(kv.me, "[PUTAPPEND]Got client request -->", args)
	op := Op{}
	if args.Op == "Put" {
		op.Optype = Put
	} else {
		op.Optype = Append
	}
	op.Xid = args.Xid
	op.Key = args.Key
	op.Value = args.Value
	op.Server = kv.me
	kv.paxosAgreement(op)
	reply.Err = OK
	glog.Infoln(kv.me, "[Put] returned")
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	kv := new(KVPaxos)
	kv.me = me
	kv.seenMap = make(map[int64]bool)
	kv.kvData = make(map[string]string)
	kv.toprocess = 1
	rpcs := rpc.NewServer()
	rpcs.Register(kv)
	kv.px = paxos.Make(servers, me, rpcs)
	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		glog.Fatal("listen error: ", e)
	}
	kv.l = l

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				glog.Warningln("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
