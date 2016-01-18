package kvpaxos

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	Xid   int64  //needed for uniqueness
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	Xid int64
}

type GetReply struct {
	Err   Err
	Value string
}
