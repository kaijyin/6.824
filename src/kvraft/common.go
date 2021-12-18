package kvraft

import "log"

const (
	Get = 1
	Put = 2
	Append = 3
)


type RequestArgs struct {
	Type  uint8 // 0 => get, 1 => put, 2 => append
	Key   string
	Value string
}

type ExecuteReply struct {
    RequestApplied bool
	Value string
}

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

