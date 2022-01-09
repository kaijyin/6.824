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
	OK             = "OK"
	ErrTimeOut     =  "ErrTimeOut"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrConfigToOld = "ErrConfigToOld"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

const (
	Gets    = 1
	Puts    = 2
	Appends = 3
	FetchShard  = 4
	DeleteShard = 5
	InstallShard = 6
	InstallConfig = 7
)
const (
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
