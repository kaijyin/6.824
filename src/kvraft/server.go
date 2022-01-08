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
	Server int
	Time   int64
}

type KVServer struct {
	clockMu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	chMap sync.Map
	// Your definitions here.
	curIndex    int
	kvMap       map[string]string
	ckLastIndex map[int64]uint32
}

func (kv *KVServer) Do(args *RequestArgs, reply *ExecuteReply) {
	if kv.killed() {
		return
	}
	kv.clockMu.Lock()//每个请求通过时间戳唯一
	time.Sleep(time.Microsecond)
	now := time.Now().UnixNano()
	kv.clockMu.Unlock()
	ch := make(chan ExecuteReply, 1)
	kv.chMap.Store(now, ch)
	defer kv.chMap.Delete(now)
	_, _, ok := kv.rf.Start(Op{
		RequestArgs: *args,
		Server:      kv.me,
		Time:        now,
	})
	if !ok {
		return
	}
	select {
	case <-time.After(time.Millisecond * 500):
	case *reply = <-ch: //do execute向chenel发送reply之后就删除id的映射
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
	if kv.killed() {
		return
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.curIndex)
	e.Encode(kv.kvMap)
	e.Encode(kv.ckLastIndex)
	//不开线程去执行snapshot,因为可能会导致多次执行,增加负担
	kv.rf.Snapshot(kv.curIndex, w.Bytes())
}
func (kv *KVServer) InstallSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	kv.curIndex = 0
	kv.kvMap = nil
	kv.ckLastIndex = nil
	d.Decode(&kv.curIndex)
	d.Decode(&kv.kvMap)
	d.Decode(&kv.ckLastIndex)
}
func (kv *KVServer) doExecute() {
	for args := range kv.applyCh {
		if kv.killed() {
			return
		}
		if args.CommandValid && kv.curIndex+1 == args.CommandIndex { //序列化
			kv.curIndex = args.CommandIndex
			op := args.Command.(Op)
			ch, ok := kv.chMap.Load(op.Time)
			reply := ExecuteReply{}
			ck := op.CkId
			lastIndex := kv.ckLastIndex[op.CkId]
			reply.RequestApplied=true
			if lastIndex+1 == op.CkIndex { //避免同一客户端重复提交多次执行
				kv.ckLastIndex[ck] = op.CkIndex
               if op.Type == Puts {
					kv.kvMap[op.Key] = op.Value
				} else if op.Type == Appends {
					kv.kvMap[op.Key] += op.Value
				}
			}
			if op.Type == Gets && ok && op.Server == kv.me {
				reply.Value = kv.kvMap[op.Key]
			}
			if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
				kv.SnapShot()
			}
			//可能kv index之前的并没有被实际执行,而是直接安装snapshot,所以还是需要把等待的请求给去除掉
			if ok && kv.me == op.Server {
				ch.(chan ExecuteReply) <- reply
			}
		} else if args.SnapshotValid && kv.rf.CondInstallSnapshot(args.SnapshotTerm, args.SnapshotIndex, args.Snapshot) {
			// 必须先安装日志,再改具体的kv存储
			kv.InstallSnapShot(args.Snapshot)
		}
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
	labgob.Register(RequestArgs{})
	labgob.Register(ExecuteReply{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.kvMap = make(map[string]string)
	kv.ckLastIndex = make(map[int64]uint32)
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 20)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.

	kv.InstallSnapShot(persister.ReadSnapshot())
	go kv.doExecute()
	return kv
}
