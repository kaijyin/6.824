package kvraft

import (
	"6.824/labrpc"
	"sync"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd

	me         uint32
	index      uint32

	total      int64
	lastLeader int64
	mu         sync.Mutex
	// You will have to modify this struct.
}

func nrand() uint32 {
	max := big.NewInt(int64(1) << 32)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return uint32(x)
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.total=int64(len(ck.servers))
	// You'll have to add code here.
	ck.me=nrand()
	DPrintf("ck.me:%d",ck.me)
	return ck
}
func (ck *Clerk) Execute(args *RequestArgs,reply *ExecuteReply){
	time.Sleep(time.Microsecond)
	for server:=atomic.LoadInt64(&ck.lastLeader);;server=(server+1)%ck.total{
		ch:=make(chan ExecuteReply,1)
		go func(i int64) {
			reply:=ExecuteReply{}
			ok:=ck.servers[i].Call("KVServer.Do", args, &reply)
			if ok {
               ch<-reply
			}
		}(server)
		select {
		case <-time.After(time.Second):
		case *reply=<-ch:
			if reply.RequestApplied{
				atomic.StoreInt64(&ck.lastLeader,server)
				return
			}
		}
	}
}
func (ck *Clerk) Get(key string) string{
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.index++
	args:=RequestArgs{
		Type:  Gets,
		Key:   key,
		CkId: ck.me,
		CkIndex: ck.index,
	}
	reply:=ExecuteReply{}
	ck.Execute(&args,&reply)
	return reply.Value
}
func (ck *Clerk) Put(key string, value string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.index++
	args:=RequestArgs{
		Type:  Puts,
		Key:   key,
		Value: value,
		CkId: ck.me,
		CkIndex: ck.index,
	}
	reply:=ExecuteReply{}
	ck.Execute(&args,&reply)
}
func (ck *Clerk) Append(key string, value string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.index++
	args:=RequestArgs{
		Type:  Appends,
		Key:   key,
		Value: value,
		CkId: ck.me,
		CkIndex: ck.index,
	}
	reply:=ExecuteReply{}
	ck.Execute(&args,&reply)
}
