package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"6.824/labrpc"
)
import "crypto/rand"
import "math/big"
import "6.824/shardctrler"
import "time"

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.

	me             int64
	lastShardIndex map[int]uint32
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	ck.lastShardIndex = make(map[int]uint32)
	ck.me = nrand()
	return ck
}
func (ck *Clerk) execute(args *Args, reply *Reply) {
	time.Sleep(time.Microsecond)
	shard := args.Shard
	for {
		//DPrintf("send msg")
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				ch := make(chan Reply, 1)
				arg:=args.Copy()
				go func() {
					reply := Reply{}
					ok := srv.Call("ShardKV.Do", &arg, &reply)
					if ok {
						ch <- reply
					}
				}()
				select {
				case <-time.After(time.Millisecond * 300):
				case rep:= <-ch:
					if rep.Err == OK {
						*reply=rep
						return
					} else if rep.Err == ErrWrongGroup {
						break
					}
				}
			}
			ck.config = ck.sm.Query(-1)
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}
func (ck *Clerk) Get(key string) string {
	shard := key2shard(key)
	ck.lastShardIndex[shard]++
	args := Args{
		Shard:          shard,
		RequestInvalid: true,
		Type:           Gets,
		Key:            key,
		CkId:           ck.me,
		CkIndex:        ck.lastShardIndex[shard],
	}
	reply := Reply{}
	ck.execute(&args, &reply)
	return reply.Value
}
func (ck *Clerk) Put(key string, value string) {
	shard := key2shard(key)
	ck.lastShardIndex[shard]++
	args := Args{Shard: shard,
		RequestInvalid: true,
		Type:           Puts,
		Key:            key,
		Value:          value,
		CkId:           ck.me,
		CkIndex:        ck.lastShardIndex[shard],
	}
	reply := Reply{}
	ck.execute(&args, &reply)
}
func (ck *Clerk) Append(key string, value string) {
	shard := key2shard(key)
	ck.lastShardIndex[shard]++
	args := Args{Shard: shard,
		RequestInvalid: true,
		Type:           Appends,
		Key:            key,
		Value:          value,
		CkId:           ck.me,
		CkIndex:        ck.lastShardIndex[shard],
	}
	reply := Reply{}
	ck.execute(&args, &reply)
}
