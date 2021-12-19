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
	server int
}
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvMap map[string]string
	chMap map[int]chan ExecuteReply
	cd  sync.Cond
}
func (kv *KVServer) lock() {
	kv.mu.Lock()
}
func (kv *KVServer) unlock() {
	// Your code here.
	kv.mu.Unlock()
}

func (kv *KVServer) Do(args *RequestArgs, reply *ExecuteReply) {
	kv.lock()
	_,index,ok:=kv.rf.Start(Op{
		RequestArgs: *args,
		server:      kv.me,
		time:        time.Now(),
	})
    if !ok{
    	reply.RequestApplied=false
    	kv.unlock()
    	return
	}
	ch,hasVal:=kv.chMap[index]
	if hasVal{
		ch<-ExecuteReply{
			RequestApplied: false,
		}
	}else{
		ch=make(chan ExecuteReply)
		kv.chMap[index]=ch
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
func (kv *KVServer) SnapShot(){
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvMap)
	//可以开线程去执行snapshot
	go kv.rf.Snapshot(kv.curIndex,w.Bytes())
}
func (kv *KVServer) InstallSnapShot(snapshotIndex int, snapshot []byte){
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	d.Decode(&kv.kvMap)
    kv.curIndex=snapshotIndex
}
func (kv *KVServer) doExecute()  {
   for !kv.killed(){
   	  args:=<-kv.applyCh
   	  kv.lock()
   	  if args.CommandValid&&kv.curIndex<args.CommandIndex{
         	index:=args.CommandIndex
         	op :=args.Command.(Op)
			 ch,ok:=kv.chMap[index]
			 reply:=ExecuteReply{RequestApplied: op.server==kv.me}
			 if op.Type==Get&&ok&&reply.RequestApplied{
			 	reply.Value=kv.kvMap[op.Key]
			}else if op.Type==Put{
                kv.kvMap[op.Key]= op.Value
			}else if op.Type==Append{
                kv.kvMap[op.Key]+= op.Value
			}
			if ok {
				ch<-reply
				delete(kv.chMap,index)
			}
         	kv.curTime=op.time
         	kv.cd.Broadcast()
         	if len(kv.rf.GetRaftStateData())>=kv.maxraftstate{
         		kv.SnapShot()
			}
	  }else if args.SnapshotValid &&kv.curIndex<args.SnapshotIndex{
	  	// 必须先安装日志,再改具体的kv存储
	  	  kv.rf.CondInstallSnapshot(args.SnapshotTerm,args.SnapshotIndex,args.Snapshot)
	  	  kv.InstallSnapShot(args.SnapshotIndex,args.Snapshot)
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

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg,20)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.chMap=make(map[int]chan ExecuteReply)
	kv.kvMap=make(map[string]string)
    go kv.doExecute()
	return kv
}
