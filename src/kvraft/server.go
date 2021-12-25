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
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	chMap       sync.Map
	// Your definitions here.
	curIndex    int
	kvMap       map[string]string
	ckLastIndex map[uint32]uint32
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
	ch := make(chan ExecuteReply,1)
	kv.lock()
	//去除掉以及处理了的请求,小优化,对网络不可信的情况下很常见
	lastIdx,_:=kv.ckLastIndex[args.CkId]
	if lastIdx>=args.CkIndex{
		reply.RequestApplied=true
		if args.Type==Gets{
			reply.Value=kv.kvMap[args.Key]
		}
		kv.unlock()
		return
	}
	id:=args.GetId()
	kv.chMap.Store(id,ch)
	defer kv.chMap.Delete(id)
	_, _, ok := kv.rf.Start(Op{
		RequestArgs: *args,
		Server:      kv.me,
	})
	if !ok {
		kv.unlock()
		return
	}
	kv.unlock()
	select{
	case <-time.After(time.Second):
	case *reply=<-ch://do execute向chenel发送reply之后就删除id的映射
	}
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
	kv.rf.Kill()
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
	close(kv.applyCh)
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
	kv.rf.Snapshot(kv.curIndex, w.Bytes())
}
func (kv *KVServer) InstallSnapShot(snapshot []byte) {
	if snapshot==nil||len(snapshot)<1{
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	kv.curIndex=0
	kv.kvMap=nil
	kv.ckLastIndex=nil
	d.Decode(&kv.curIndex)
	d.Decode(&kv.kvMap)
	d.Decode(&kv.ckLastIndex)
}
func (kv *KVServer) doExecute() {
	for !kv.killed() {
		args := <-kv.applyCh
		kv.lock()
		if kv.killed(){
			kv.unlock()
			return
		}
		if args.CommandValid&&kv.curIndex+1==args.CommandIndex{//序列化
			kv.curIndex = args.CommandIndex
			op := args.Command.(Op)
			ch, ok := kv.chMap.Load(op.GetId())
			reply := ExecuteReply{}
				kv.curIndex = args.CommandIndex
				ck := op.CkId
				lastIndex, _ := kv.ckLastIndex[op.CkId]
				if lastIndex+1 == op.CkIndex { //避免同一客户端重复提交多次执行
					kv.ckLastIndex[ck] = op.CkIndex
					reply.RequestApplied = op.Server == kv.me
					if op.Type == Gets && ok && reply.RequestApplied {
						reply.Value = kv.kvMap[op.Key]
					} else if op.Type == Puts {
						kv.kvMap[op.Key] = op.Value
					} else if op.Type == Appends {
						kv.kvMap[op.Key] += op.Value
					}
				}
				if kv.maxraftstate!=-1&&kv.rf.GetRaftStateSize() >= kv.maxraftstate {
					kv.SnapShot()
				}
			//可能kv index之前的并没有被实际执行,而是直接安装snapshot,所以还是需要把等待的请求给去除掉
			if ok && op.Server==kv.me{
				ch.(chan ExecuteReply) <- reply
			}
		} else if args.SnapshotValid && kv.curIndex < args.SnapshotIndex {
			// 必须先安装日志,再改具体的kv存储
			kv.rf.CondInstallSnapshot(args.SnapshotTerm, args.SnapshotIndex, args.Snapshot)
			kv.InstallSnapShot(args.Snapshot)
			kv.curIndex=args.SnapshotIndex
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
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.kvMap = make(map[string]string)
	kv.ckLastIndex = make(map[uint32]uint32)
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 20)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.

	kv.InstallSnapShot(persister.ReadSnapshot())
	go kv.doExecute()
	return kv
}
