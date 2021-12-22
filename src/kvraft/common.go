package kvraft

import "log"

const (
	Gets = 1
	Puts = 2
	Appends = 3
)


type RequestArgs struct {
	Type  uint8 // 0 => get, 1 => put, 2 => append
	Key   string
	Value string
	CkId  int64
	CkIndex int64
}

type ExecuteReply struct {
    RequestApplied bool
	Value string
}

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

