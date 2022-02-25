package kvraft

import "log"

const (
	Sync =    1
	Puts    = 2
	Appends = 3
)

type RequestArgs struct {
	Type    uint8 //syn, put, append
	Key     string
	Value   string
	CkId    int64
	CkIndex uint32
}
type GetRequestArgs struct {
	SynIndex int
	Key string
}
//func (req *RequestArgs) GetId()uint64  {
//	return (uint64(req.CkId)<<32)+uint64(req.CkIndex)
//}

func Max(a uint32, b uint32) uint32 {
	if a > b {
		return a
	} else {
		return b
	}
}

type ExecuteReply struct {
	RequestApplied bool
	SyncIndex       int
}
type GetsReply struct {
	RequestApplied bool
	Value          string
}

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
