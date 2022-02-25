package kvraft

import (
	"6.824/labrpc"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd

	me    int64
	index uint32

	total      int
	lastLeader int
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
	ck.total = len(ck.servers)
	// You'll have to add code here.
	ck.me = nrand()
	return ck
}
func (ck *Clerk) Execute(args *RequestArgs, reply *ExecuteReply) {
	for server := ck.lastLeader; ; server = (server + 1) % ck.total {
		ch := make(chan ExecuteReply, 1)
		go func(i int, req RequestArgs) {
			reply := ExecuteReply{}
			ok := ck.servers[i].Call("KVServer.Do", &req, &reply)
			if ok {
				ch <- reply
			}
		}(server, *args)
		select {
		case <-time.After(time.Millisecond * 500):
		case *reply = <-ch:
			if reply.RequestApplied {
				ck.lastLeader=server
				return
			}
		}
	}
}
//Get可以重复执行,更新操作执行顺序又客户端决定
func (ck *Clerk) Get(key string) string {
	syncReply := ExecuteReply{}
	ck.Execute(&RequestArgs{
		Type:    Sync,
	}, &syncReply)
	getReply:=GetsReply{}
	for server := 0; ; server = (server + 1) % ck.total {//从第一个从节点开始请求
		if server==ck.lastLeader{
			continue
		}
		ch := make(chan GetsReply, 1)
		go func(i int, req GetRequestArgs) {
			reply := GetsReply{}
			ok := ck.servers[i].Call("KVServer.Get", &req, &reply)
			if ok {
				ch <- reply
			}
		}(server, GetRequestArgs{
			SynIndex: syncReply.SyncIndex,
			Key:      key,
		})
		select {
		case <-time.After(time.Millisecond * 300):
		case getReply= <-ch:
		}
		if getReply.RequestApplied {
			break
		}
	}
	return getReply.Value
}
func (ck *Clerk) Put(key string, value string) {
	ck.index++
	args := RequestArgs{
		Type:    Puts,
		Key:     key,
		Value:   value,
		CkId:    ck.me,
		CkIndex: ck.index,
	}
	reply := ExecuteReply{}
	ck.Execute(&args, &reply)
}
func (ck *Clerk) Append(key string, value string) {
	ck.index++
	args := RequestArgs{
		Type:    Appends,
		Key:     key,
		Value:   value,
		CkId:    ck.me,
		CkIndex: ck.index,
	}
	reply := ExecuteReply{}
	ck.Execute(&args, &reply)
}
