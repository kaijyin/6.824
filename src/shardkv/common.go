package shardkv

import (
	"log"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

const (
	Gets    = 1
	Puts    = 2
	Appends = 3
)

type RequestArgs struct {
	ISCK bool
	Shard   int

	ConfigNum int

	CkId    int64
	CkIndex uint32
	Type    uint8 // 0 => get, 1 => put, 2 => append
	Key     string
	Value   string
}

type ExecuteReply struct {
	Err
	Value string

	ShardData map[string]string
	ShardLastCkIndex map[int64]uint32
}

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
