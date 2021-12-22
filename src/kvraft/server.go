package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"sync"
	"sync/atomic"
	"time"
)

type Op struct {
	RequestArgs
	Server    int
	StartTime int64
}
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	curIndex    int
	kvMap       map[string]string
	chMap       map[int64]chan ExecuteReply
	ckLastIndex map[int64]int64
	ckGetValue  map[int64]string
}

func (kv *KVServer) lock() {
	kv.mu.Lock()
}
func (kv *KVServer) unlock() {
	// Your code here.
	kv.mu.Unlock()
}

func (kv *KVServer) Do(args *RequestArgs, reply *ExecuteReply) {
	//DPrintf("%d do ck:%d index:%d op:%d", kv.me, args.CkId, args.CkIndex, args.Type)
	ch := make(chan ExecuteReply)
	kv.lock()
	//去除掉以及处理了的请求,小优化,对网络不可信的情况下很常见
	lastIdx,has:=kv.ckLastIndex[args.CkId]
	if has&&lastIdx>=args.CkIndex{
		kv.unlock()
		return
	}
	curTime := time.Now().UnixNano()
	kv.chMap[curTime] = ch
	_, _, ok := kv.rf.Start(Op{
		RequestArgs: *args,
		Server:      kv.me,
		StartTime:   curTime,
	})
	if !ok {
		delete(kv.chMap,curTime)
		kv.unlock()
		return
	}
	kv.unlock()
	*reply=<-ch
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
func (kv *KVServer) SnapShot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.curIndex)
	e.Encode(kv.kvMap)
	e.Encode(kv.ckLastIndex)
	//可以开线程去执行snapshot
	go kv.rf.Snapshot(kv.curIndex, w.Bytes())
}
func (kv *KVServer) InstallSnapShot(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	d.Decode(&kv.curIndex)
	d.Decode(&kv.kvMap)
	d.Decode(&kv.ckLastIndex)
}
func (kv *KVServer) doExecute() {
	for !kv.killed() {
		args := <-kv.applyCh
		kv.lock()
		if args.CommandValid {
			op := args.Command.(Op)
			ch, ok := kv.chMap[op.StartTime]
			reply := ExecuteReply{}
			//由我的实现,通过请求当前server产生的log一定会被放入applyCh,通过commandInvalid来标识是否提交
			if kv.curIndex < args.CommandIndex {//序列化
				kv.curIndex = args.CommandIndex
				//DPrintf("commit ")
				ck := op.CkId
				lastIndex, _ := kv.ckLastIndex[op.CkId]
				if lastIndex+1 == op.CkIndex { //避免同一客户端重复提交多次执行
					kv.ckLastIndex[ck] = op.CkIndex
					reply.RequestApplied = op.Server == kv.me
					if op.Type == Gets && ok && reply.RequestApplied {
						reply.Value = kv.kvMap[op.Key]
						//DPrintf("execute reply value %s",reply.Value)
					} else if op.Type == Puts {
						kv.kvMap[op.Key] = op.Value
					} else if op.Type == Appends {
						kv.kvMap[op.Key] += op.Value
					}
				}
				if kv.maxraftstate!=-1&&kv.rf.GetRaftStateSize() >= kv.maxraftstate {
					kv.SnapShot()
				}
			}
			//可能kv index之前的并没有被实际执行,而是直接安装snapshot,所以还是需要把等待的请求给去除掉
			if ok && op.Server==kv.me{
				DPrintf("%d send reply to :%d index:%d request invalid:%v",kv.me,op.CkId,op.CkIndex,reply.RequestApplied)
				ch <- reply
				delete(kv.chMap,op.StartTime)
			}
		} else if args.SnapshotValid && kv.curIndex < args.SnapshotIndex {
			// 必须先安装日志,再改具体的kv存储
			kv.rf.CondInstallSnapshot(args.SnapshotTerm, args.SnapshotIndex, args.Snapshot)
			kv.InstallSnapShot(args.Snapshot)
		} else if !args.CommandValid && !args.SnapshotValid { //invalid apply,没有成功提交的请求返回false
			op := args.Command.(Op)
			ch, ok := kv.chMap[op.StartTime]
			if ok &&op.Server==kv.me{
				//DPrintf("recive uncommited apply ck:%d index:%d",op.CkId,op.CkIndex)
				ch <- ExecuteReply{RequestApplied: false}
				delete(kv.chMap, op.StartTime)
			}
		}
		kv.unlock()
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(map[string]string{})
	labgob.Register(Op{})
	labgob.Register(map[int64]int64{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.chMap = make(map[int64]chan ExecuteReply)
	kv.kvMap = make(map[string]string)
	kv.ckLastIndex = make(map[int64]int64)
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 20)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.

	kv.InstallSnapShot(persister.ReadSnapshot())
	go kv.doExecute()
	return kv
}
