package shardctrler

import (
	"log"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}
func (c *Config)Copy()Config{
	config:=Config{
		Num:    c.Num,
		Shards: c.Shards,
	}
	config.Groups=make(map[int][]string)
	for g,names:=range c.Groups{
		config.Groups[g]=make([]string,len(names))
		copy(config.Groups[g],names)
	}
	return config
}
func (c *Config) Print()  {
	DPrintf("-------Config-----------")
	DPrintf("num:%d",c.Num)
	DPrintf("shard:")
	for i,g:=range c.Shards{
		DPrintf("index:%d  group:%d",i,g)
	}
	//DPrintf("Groups:")
	//for g,_:=range c.Groups{
	//	DPrintf("%d",g)
		//for _,name:=range names{
		//	DPrintf(name)
		//}
	//}
}
const (
	OK = "OK"
)
const (
	Join = 1
	Leave = 2
	Move = 3
	Query =4
)


type Args struct {
	CkId    int64
	CkIndex uint32
	Type    int
	JoinArgs
	LeaveArgs
	QueryArgs
}

func (a Args) Copy() Args {
	args:=a
	if a.Type==Join{
		args.Servers=make(map[int][]string)
		for g,server:=range a.Servers{
			args.Servers[g]=make([]string,len(server))
			copy(args.Servers[g],server)
		}
	}
	if a.Type==Leave{
		args.GIDs=make([]int,len(a.GIDs))
		copy(args.GIDs,a.GIDs)
	}
	return args
}
type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
}

type LeaveArgs struct {
	GIDs []int
}


type QueryArgs struct {
	Num int // desired config number
}
type Reply struct {
	RequestApplied bool
	Config      Config
}


// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

