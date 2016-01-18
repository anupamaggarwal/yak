package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "glog"
import "time"
import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "math/rand"
import "flag"

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Reply int

const (
	Accept Reply = iota + 1
	Reject
)

type State struct {
	//this keeps track of highest proposal seen by this host
	proposal int
	//keeps track of proposal number of highest accept
	highestproposalaccepted int
	decision                Fate
	v                       value
}

type Paxos struct {
	mu                sync.Mutex
	l                 net.Listener
	dead              int32 // for testing
	unreliable        int32 // for testing
	rpcCount          int32 // for testing
	peers             []string
	me                int // index into peers[]
	lastDoneSignalled int
	maxCanDisregard   int //upto how much this application can safely disregard
	//All the following are keyed on the paxos seq number
	stateMap map[int]State
}

//global map of values
type value interface{}

type PrepareReq struct {
	Proposal int
	Seq      int
}
type PrepareRep struct {
	Proposal          int
	ProposalAccepted  int
	Status            Reply
	V                 value
	LastDoneSignalled int
}

type AcceptReq struct {
	Proposal int
	Seq      int
	V        value
}
type AcceptReply struct {
	Status Reply
}

type DecideReq struct {
	V   value
	Seq int
}
type DecideReply struct{}

type LastDoneMsg struct {
}
type LastDoneReply struct {
	Done int
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// tplys contents are only valid if call() returned true.
//
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			glog.Warningln("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	glog.Infoln(err)
	return false
}

//---------------- shared state functions -------------

//---------------- paxos communication functions ------

func (px *Paxos) promise(seq int, state *State) bool {
	glog.Infoln(px.me, "[promise] proposalNum#", state.proposal, " seqNum#", seq, "value ", state.v)
	prepareRep := &PrepareRep{}
	prepareRep.LastDoneSignalled = -1
	prepareReq := &PrepareReq{}
	prepareReq.Proposal = state.proposal
	prepareReq.Seq = seq

	highestProposalInReply := -2
	var valueInHighestProposalReply value
	numAccepts := 0
	maxCanDisregard := 1000000

	for indx, server := range px.peers {
		ret := false
		if indx == px.me {
			ret = (px.PromiseHandler(prepareReq, prepareRep) == nil)
		} else {
			ret = call(server, "Paxos.PromiseHandler", prepareReq, prepareRep)

		}
		//update highestProposalInReply and value in highestProposaled Reply
		if ret {
			//RPC or method call successul
			if prepareRep.Status == Accept {
				glog.Infoln(px.me, "Printing promise reply", prepareRep)
				numAccepts++
				if prepareRep.ProposalAccepted > highestProposalInReply {
					highestProposalInReply = prepareRep.ProposalAccepted
					valueInHighestProposalReply = prepareRep.V
					glog.Infoln(px.me, " Updating highest proposal in reply", highestProposalInReply, " with value", valueInHighestProposalReply)

				}
			}
			//handle LastDoneSignalled to update maxCanDisregard
			if prepareRep.LastDoneSignalled < maxCanDisregard {
				maxCanDisregard = prepareRep.LastDoneSignalled
			}

		}
	}
	glog.Infoln(px.me, "after promise round MaxCanDisregard set to ", maxCanDisregard)
	//maxCanDisregard needs to be updated here
	if maxCanDisregard != -1 {
		if maxCanDisregard > px.maxCanDisregard {
			px.maxCanDisregard = maxCanDisregard
			glog.Infoln(px.me, "Updating value of maxCanDisregard to :", px.maxCanDisregard)
			//deleting here as much as I can
			for key, _ := range px.stateMap {
				if key < px.maxCanDisregard {
					glog.Infoln(px.me, "Deleting key#", key)
					delete(px.stateMap, key)
				}
			}

		}
	}

	//there are 2 cases: if majority assembled move over to accept phase and update value from reply,
	//if reply had nil value proposer uses own value else uses reply from highest valeud proposal
	//px.mu.Lock()
	glog.Infoln(px.me, "Printing peers for myself", px.peers, " with numAccepts", numAccepts)

	if 2*numAccepts > len(px.peers) {
		glog.Infoln(px.me, "[promise] Peers accepted proposalNum#", state.proposal, " seqNum#", seq, "value ", state.v)
		if highestProposalInReply != -1 {
			//we saw a proposal which was accepted
			state.v = valueInHighestProposalReply
			glog.Infoln(px.me, "[promise] Updating value to be proposed in next round proposalNum#", state.proposal, " seqNum#", seq, "value ", state.v)

		} else {
			//using own proposal
			glog.Infoln(px.me, "[promise] own value to use for accept round ", state.v)
		}
		return true
	} else {
		glog.Infoln(px.me, "Peers rejected proposal", state.proposal, " with value ", state.v)
		//add some random determinism based on id
		rand.Seed(int64(px.me))
		tosleep := rand.Intn(50)
		glog.Infoln(px.me, "Paxos sleeping for random time", tosleep)
		time.Sleep(time.Duration(int(tosleep)) * time.Millisecond)
		//if majority not assembled check highestReplied proposal and if > this proposal no chance for this to succeed
		//but before replying back immediately wait so as to not start a proposal with higher number immediately
		return false
	}

}

func (px *Paxos) PromiseHandler(req *PrepareReq, rep *PrepareRep) error {
	//handles promise messages following is the pseudo code
	//if this is itself the proposer for same instance and incoming seq numner is highr it should back off
	//if this proposal is encountered for the first time for instance it should be accepted and acked yes
	px.mu.Lock()
	rep.LastDoneSignalled = px.lastDoneSignalled
	if val, ok := px.stateMap[req.Seq]; ok {
		//we have already seen this paxos seq but we might have a different proposal number for it
		if val.proposal > req.Proposal {
			//reject this proposal
			//	glog.Infoln(px.me, "[PromisHandler] Proposal #", req.Proposal, " rejected since own proposalNum#", val.proposal, " was higher")
			rep.Status = Reject
			rep.V = nil
			//set proposal number in return msg
			rep.Proposal = -1
			px.mu.Unlock()
			return nil
		} else {
			//make sure I have not already decided
			//proposal will be accepted and value accepted returned if not nil
			rep.Status = Accept
			rep.ProposalAccepted = val.highestproposalaccepted
			rep.V = val.v
			//return back locally accepted value if any
			//update highest proposal seen
			val.proposal = req.Proposal
			//	glog.Infoln(px.me, "[PromisHandler] Proposal #", req.Proposal, " accepted, since", "incoming propsal is higher, printing v", val, " sending rep", rep)
			glog.Infoln(px.me, "[PromiseHandler] printing my state map", px.stateMap)
			px.stateMap[req.Seq] = val
			px.mu.Unlock()
			return nil
		}

	} else {
		//no record exists for this paxos seq
		//proposal will be accepted and value accepted returned if not nil
		val := State{}
		rep.Status = Accept
		//return back locally accepted value if any
		rep.V = nil
		//update highest proposal seen
		val.proposal = req.Proposal
		val.highestproposalaccepted = -1
		rep.Proposal = -1
		rep.ProposalAccepted = -1
		val.decision = Pending
		px.stateMap[req.Seq] = val
		glog.Infoln(px.me, "[PromisHandler] Proposal #", req.Proposal, " accepted")
		px.mu.Unlock()
		return nil
	}

}

//--------------------------------------------------
func (px *Paxos) accept(seq int, state *State) bool {
	//send accept message with state in
	acceptReq := &AcceptReq{}
	acceptRep := &AcceptReply{}
	acceptReq.Proposal = state.proposal
	acceptReq.V = state.v
	acceptReq.Seq = seq
	numAccepts := 0

	glog.Infoln(px.me, "[Accept] hadnler with seq", seq)
	for indx, server := range px.peers {
		ret := false
		if indx == px.me {
			ret = (px.AcceptHandler(acceptReq, acceptRep) == nil)
		} else {
			ret = call(server, "Paxos.AcceptHandler", acceptReq, acceptRep)
		}
		//update highestProposalInReply and value in highestProposaled Reply
		if ret {
			//RPC or method call successul
			if acceptRep.Status == Accept {
				numAccepts++
			}
		}
	}
	if 2*numAccepts > len(px.peers) {
		glog.Infoln(px.me, "[Accept] majority accepted proposal decision reached will send decide soon ", seq)
		//do not update internal state yet
		return true
	} else {
		glog.Infoln(px.me, "[Accept]Peers rejected accept proposal having #", state.proposal, " will go to next round for slot#", seq)
	}
	return false
}

func (px *Paxos) AcceptHandler(req *AcceptReq, rep *AcceptReply) error {
	//accept request handler will need to toggle global state
	px.mu.Lock()
	instance := px.stateMap[req.Seq]
	if instance.proposal > req.Proposal {
		//send reject
		rep.Status = Reject
		glog.Infoln(px.me, "[ACCEPTHANDLEr]Peer rejected accept message with proposal#", req.Proposal, "since it has seen higher proposal ", instance.proposal)
	} else {
		//accept this request covers equality case
		rep.Status = Accept
		instance.highestproposalaccepted = req.Proposal
		instance.v = req.V
		//this should not be necessary here since accept messages should have same proposal number
		instance.proposal = req.Proposal
		instance.decision = Pending
		px.stateMap[req.Seq] = instance
		glog.Infoln(px.me, " [AcceptHandler]Peer accepted accept message with proposal#", req.Proposal, "and value ", req.V, " for seq", req.Seq)

	}
	px.mu.Unlock()
	return nil
}
func (px *Paxos) LastDoneHandler(req *LastDoneMsg, rep *LastDoneReply) error {
	//accept request handler will need to toggle global state

	rep.Done = px.lastDoneSignalled
	return nil
}

//--------------------------------------------------
func (px *Paxos) decide(seq int, state *State) bool {
	//decide handler updates self state and sends decide message to peers
	//proposal number is inconsequential here, we can possibly clean state here
	decideReq := &DecideReq{}
	decideRep := &DecideReply{}
	decideReq.V = state.v
	decideReq.Seq = seq

	glog.Infoln(px.me, "[Decide] handler with seq", seq)
	for indx, server := range px.peers {
		if indx == px.me {
			px.DecideHandler(decideReq, decideRep)
		} else {
			ret := call(server, "Paxos.DecideHandler", decideReq, decideRep)
			if !ret {
				glog.Infoln(px.me, "Decide message failed for index", indx)
			}
		}
		//update highestProposalInReply and value in highestProposaled Reply
		//we don't care about return code here we just update intrnal state
	}

	return true
}

func (px *Paxos) DecideHandler(req *DecideReq, rep *DecideReply) error {
	//updates internal state here
	glog.Infoln(px.me, "[DecideHandler] decision reached with value ", req.V)
	px.mu.Lock()

	if instance, ok := px.stateMap[req.Seq]; ok {
		//instance has to be present
		instance.decision = Decided
		instance.v = req.V
		px.stateMap[req.Seq] = instance
		glog.Infoln(px.me, "[DecideHandler]Priting my stateMap", px.stateMap, " for seq", req.Seq)

	} else {
		instance := State{}
		instance.decision = Decided
		instance.v = req.V
		px.stateMap[req.Seq] = instance
		glog.Infoln(px.me, "[DecideHandler] WARNING!! decide message but no state reached, possible crash recovery")

	}

	px.mu.Unlock()
	return nil
}

//--------------------------------------------------
//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Xtart() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {

	glog.Infoln(px.me, "[Start] called with seq#", seq, " and value#", v)
	go px.takeDecision(seq, v)

}

func (px *Paxos) takeDecision(seq int, v interface{}) {
	//make sure only one instance of takeDecision is called per instance
	//maybe we can use channels to synchronize here btw same application processes
	//allocate a new state object
	state := State{}
	px.mu.Lock()

	//check if lastCleared by this peer is > what came in request
	if px.maxCanDisregard >= seq {
		glog.Infoln(px.me, "WARNING!!! PAXOS election already undertaken and decision reached seq#", seq)
		//unlock the mutex
		px.mu.Unlock()
		return

	}

	if state, ok := px.stateMap[seq]; ok {
		if state.decision == Pending {
			//this is a bug calling state again on something which is underway
			glog.Infoln(px.me, "WARNING!!! PAXOS election already underway for  seq#, please wait before calling ", seq)
			//check if I have already accepted a value in which case i use the same value
			if state.highestproposalaccepted == -1 {
				glog.Infoln(px.me, "Competing proposal ")
				state.v = v
			}
			state.proposal = px.me
			px.stateMap[seq] = state

			//px.mu.Unlock()
			//return
		} else {
			//could have alrady been decided but not cleared yet since application did not read its value?
			glog.Infoln(px.me, "WARNING!!! PAXOS election already done, decision reached:", px.stateMap[seq].decision, " for seq#:", seq)
			px.mu.Unlock()
			return

		}

	} else {
		//start called for paxos seq which has been already iniated by this application possible error
		//initialize the value of state for this paxos instance and put in global map
		state.decision = Pending
		state.proposal = px.me
		state.highestproposalaccepted = -1
		state.v = v
		px.stateMap[seq] = state
		glog.Infoln(px.me, "Initializing state with value", " for seq#", seq)

	}

	state = px.stateMap[seq]
	px.mu.Unlock()

	for state.decision != Decided && !px.isdead() {

		glog.Infoln(px.me, "[Start] In proposal#", state.proposal, " seq#", seq, " with value", state.v)
		//start with a Proposal # and keep on incrementing the proposal untill decision is reached

		if !px.promise(seq, &state) {
			//promise phase failed will have to restart with updated proposalNum
			state.proposal = state.proposal + len(px.peers)
			glog.Infoln(px.me, "Incrementing proposal num , new proposal #", state.proposal)
			continue
		}
		glog.Infoln(px.me, " PROMISE STAGE OVER for proposal #", state.proposal)
		if !px.accept(seq, &state) {
			//accept failed
			state.proposal = px.me + len(px.peers)
			continue
		}

		if !px.decide(seq, &state) {
			//decide failed
		} else {
			//PAXOS decision reached / everything is setup in state we store it in the map
			px.mu.Lock()
			state = px.stateMap[seq]
			state.decision = Decided
			px.stateMap[seq] = state
			px.mu.Unlock()
		}
	}
	glog.Infoln(px.me, "Decision reached with value", state.v)
	return
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	glog.Infoln(px.me, "[Done] called with ", seq)
	px.mu.Lock()
	if seq > px.lastDoneSignalled {
		px.lastDoneSignalled = seq
		glog.Infoln(px.me, "[Done] updating done#", seq, " value of done ", px.lastDoneSignalled)
	}

	//clear everything below maxCanDisregard
	px.mu.Unlock()

}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	max := 0
	px.mu.Lock()
	for key, _ := range px.stateMap {
		if key > max {
			max = key
		}

	}
	px.mu.Unlock()
	glog.Infoln("[Max] called with value ", max)
	return max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	min := 100000
	lastDoneReq := &LastDoneMsg{}
	lastDoneRep := &LastDoneReply{}
	for indx, server := range px.peers {
		if indx == px.me {
			px.LastDoneHandler(lastDoneReq, lastDoneRep)
		} else {
			call(server, "Paxos.LastDoneHandler", lastDoneReq, lastDoneRep)
		}
		if lastDoneRep.Done < min {
			min = lastDoneRep.Done
			glog.Infoln(px.me, "Updating min to ", min)
		}
		//update highestProposalInReply and value in highestProposaled Reply
		//we don't care about return code here we just update intrnal state
	}
	px.mu.Lock()
	if min > px.maxCanDisregard {

		px.maxCanDisregard = min
	}
	px.mu.Unlock()
	return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {

	px.mu.Lock()

	if val, ok := px.stateMap[seq]; ok {
		px.mu.Unlock()

		glog.Infoln(px.me, "state printing for seq", seq, " decision ", val.decision, " map", px.stateMap)
		return val.decision, val.v

	} else {
		//might be forgotten or not seen
		if seq < px.maxCanDisregard {
			px.mu.Unlock()
			return Forgotten, nil
		}

	}
	px.mu.Unlock()
	return Pending, nil
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.stateMap = make(map[int]State)
	px.maxCanDisregard = -1
	px.lastDoneSignalled = -1

	flag.Parse()
	px.me = me

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			glog.Fatalf("listen error: ", e)
		}
		px.l = l

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					atomic.AddInt32(&px.rpcCount, 1)
					go rpcs.ServeConn(conn)
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					//////fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
