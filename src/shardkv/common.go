package shardkv

import (
	"6.824/shardctrler"
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
	OK             = 1
	ErrTimeOut     =  2
	ErrNoData      =  3
	ErrNoKey       = 4
	ErrWrongGroup  = 5
	ErrConfigToOld = 6
	ErrWrongLeader = 7
	ErrNotCurLeader = 8
)

type Err int

const (
	Gets    = 1
	Puts    = 2
	Appends = 3
	FetchShard  = 4
	DeleteShard = 5
	InstallShard = 6
	InstallConfig = 7
)

type Args struct {

	Type    int
	InstallInvalid bool
	ShardData     []byte
	Config shardctrler.Config

	Shard int

	RemoteInvalid bool
	ConfigNum     int

	RequestInvalid bool
	CkId    int64
	CkIndex uint32
	Key     string
	Value   string
}

func (a *Args) Copy() Args{
     arg:=*a
     if a.Type==InstallConfig{
     	arg.Config=a.Config.Copy()
	 }
	 if a.Type==InstallShard {
	 	arg.ShardData =make([]byte,len(a.ShardData))
	 	copy(arg.ShardData,a.ShardData)
	 }
	 return arg
}



type Reply struct {
	Err
	Value     string
	ShardData []byte
}

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
