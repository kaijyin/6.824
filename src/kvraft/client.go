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

	me         int64
	index      int64

	total      int64
	lastLeader int64
	mu         sync.Mutex
	// You will have to modify this struct.
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
	ck.total=int64(len(ck.servers))
	// You'll have to add code here.
	ck.me =nrand()
	return ck
}
func (ck *Clerk) Execute(args *RequestArgs,reply *ExecuteReply){
	timer:=time.NewTimer(time.Duration(2)*time.Second)
	for server:=atomic.LoadInt64(&ck.lastLeader);;server=(server+1)%ck.total{
		ch:=make(chan ExecuteReply)
		timer.Reset(time.Duration(2)*time.Second)
		go func(i int64,requestArgs *RequestArgs) {
			reply:=ExecuteReply{}
			ok:=ck.servers[i].Call("KVServer.Do", requestArgs, &reply)
			if ok {
               ch<-reply
			}
		}(server,args)
		select {
		case <-timer.C:{
			continue
		}
		case *reply=<-ch:
			//DPrintf("receive reply invalid%v",reply.RequestApplied)
			if reply.RequestApplied{
				//DPrintf("%d reply invalid%v index:%d type:%d",ck.me,reply.RequestApplied,args.CkIndex,args.Type)
				//if args.Type==Gets{
				//	DPrintf("get value %s",reply.Value)
				//}
				atomic.StoreInt64(&ck.lastLeader,server)
				return
			}
			//time.Sleep(time.Duration(2)*time.Second)
		}
	}
}
func (ck *Clerk) getNewCommandIndex()int64  {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.index++
    return ck.index
}
func (ck *Clerk) Get(key string) string{
	args:=RequestArgs{
		Type:  Gets,
		Key:   key,
		CkId: ck.me,
		CkIndex: ck.getNewCommandIndex(),
	}
	reply:=ExecuteReply{}
	ck.Execute(&args,&reply)
	return reply.Value
}
func (ck *Clerk) Put(key string, value string) {
	args:=RequestArgs{
		Type:  Puts,
		Key:   key,
		Value: value,
		CkId: ck.me,
		CkIndex: ck.getNewCommandIndex(),
	}
	reply:=ExecuteReply{}
	ck.Execute(&args,&reply)
}
func (ck *Clerk) Append(key string, value string) {
	args:=RequestArgs{
		Type:  Appends,
		Key:   key,
		Value: value,
		CkId: ck.me,
		CkIndex: ck.getNewCommandIndex(),
	}
	reply:=ExecuteReply{}
	ck.Execute(&args,&reply)
}
