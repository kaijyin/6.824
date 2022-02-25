package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
	"sync"
	"sync/atomic"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	mu sync.Mutex
	servers []*labrpc.ClientEnd
	me int64
    index uint32
	lastLeader int64
	total int64
	// Your data here.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.total= int64(len(ck.servers))
	ck.me=nrand()
	ck.index=0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.index++
	args := QueryArgs{Num: num}
	// Your code here.
	reply:=&Reply{}
	ck.Execute(&Args{
		CkId: ck.me,
		CkIndex: ck.index,
		Type:    Query,
		QueryArgs: args,
	},reply)
	return reply.Config
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.index++
	args := JoinArgs{Servers: servers}
	ck.Execute(&Args{
		CkId: ck.me,
		CkIndex: ck.index,
        Type:    Join,
		JoinArgs: args,
	},&Reply{})
}

func (ck *Clerk) Leave(gids []int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.index++
	args := LeaveArgs{GIDs: gids}
	ck.Execute(&Args{
		CkId: ck.me,
		CkIndex: ck.index,
		Type:    Leave,
		LeaveArgs: args,
	},&Reply{})
}

func (ck *Clerk) Execute(args *Args,reply *Reply){
	time.Sleep(time.Microsecond)
	for server:=atomic.LoadInt64(&ck.lastLeader);;server=(server+1)%ck.total{
		ch:=make(chan Reply,1)
		arg:=args.Copy() //每个请求都独立
		go func(i int64) {
			reply:=Reply{}
			ok:=ck.servers[i].Call("ShardCtrler.Do", &arg, &reply)
			if ok {
				ch<-reply
			}
		}(server)
		select {
		case <-time.After(time.Millisecond*300):
		case *reply=<-ch:
			if reply.RequestApplied{
				atomic.StoreInt64(&ck.lastLeader,server)
				return
			}
		}
	}
}