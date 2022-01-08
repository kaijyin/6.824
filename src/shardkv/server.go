package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

type Op struct {
	RequestArgs
	Server int
	Time   int64
}

type ShardKV struct {
	me           int
	rf           *raft.Raft
	dead         int32 // set by Kill()
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	mck     *shardctrler.Clerk

	config    shardctrler.Config

	shardMu        [shardctrler.NShards]sync.Mutex
	shardChMap     [shardctrler.NShards]sync.Map
	curIndex       int
	kvMap          map[int]map[string]string
	shardLastIndex map[int]map[int64]uint32
}

func (kv *ShardKV) lockShard(shard int) {
	kv.shardMu[shard].Lock()
}
func (kv *ShardKV) unlockShard(shard int) {
	kv.shardMu[shard].Unlock()
}

func (kv *ShardKV) Do(args *RequestArgs, reply *ExecuteReply) {
	shard := args.Shard
	if kv.killed() {
		return
	}
	kv.shardMu[shard].Lock()
	time.Sleep(time.Microsecond)
	now := time.Now().UnixNano()
	kv.shardMu[shard].Unlock()
	ch := make(chan ExecuteReply, 1)
	kv.shardChMap[shard].Store(now, ch)
	defer kv.shardChMap[shard].Delete(now)
	_, _, ok := kv.rf.Start(Op{
		RequestArgs:*args,
		Server:      kv.me,
		Time:        now,
	})
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	select {
	case <-time.After(time.Millisecond * 500):
	case *reply = <-ch: //do execute向chenel发送reply之后就删除id的映射
	}
}

func (kv *ShardKV) SnapShot() {
	if kv.killed() {
		return
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.curIndex)
	e.Encode(kv.kvMap)
	e.Encode(kv.shardLastIndex)
	//不开线程去执行snapshot,因为可能会导致多次执行,增加负担
	kv.rf.Snapshot(kv.curIndex, w.Bytes())
}
func (kv *ShardKV) InstallSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	kv.curIndex = 0
	kv.kvMap = nil
	kv.shardLastIndex = nil
	d.Decode(&kv.curIndex)
	d.Decode(&kv.kvMap)
	d.Decode(&kv.shardLastIndex)
}
func (kv *ShardKV) doExecute() {
	for args := range kv.applyCh {
		if kv.killed() {
			return
		}
		if args.CommandValid && kv.curIndex+1 == args.CommandIndex { //序列化
			kv.curIndex = args.CommandIndex
			op := args.Command.(Op)
			shard:=op.Shard
			ch, ok := kv.shardChMap[shard].Load(op.Time)
			reply := ExecuteReply{}
			if !op.ISCK{
				if op.ConfigNum>kv.config.Num{
					go kv.fetchNewConfig()
				}
				kv.shardMu[shard].Lock()
			}else{
				lastMap, has := kv.shardLastIndex[shard]
				if !has {
					reply.Err = ErrWrongGroup
				} else {
					lastIndex := lastMap[op.CkId]
					reply.Err = OK
					if lastIndex+1 == op.CkIndex { //避免同一客户端重复提交多次执行
						lastMap[op.CkId] = op.CkIndex//map是引用传递,可以直接用
						if op.Type == Puts {
							kv.kvMap[shard][op.Key] = op.Value
						} else if op.Type == Appends {
							kv.kvMap[shard][op.Key] += op.Value
						}
					}
					if op.Type == Gets && ok && op.Server == kv.me {
						reply.Value = kv.kvMap[shard][op.Key]
					}
					if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
						kv.SnapShot()
					}
				}
			}
			if ok && kv.me == op.Server {
				ch.(chan ExecuteReply) <- reply
			}
		} else if args.SnapshotValid && kv.rf.CondInstallSnapshot(args.SnapshotTerm, args.SnapshotIndex, args.Snapshot) {
			// 必须先安装日志,再改具体的kv存储
			kv.InstallSnapShot(args.Snapshot)
		}
	}
}
func (kv *ShardKV)ShardQuery(){

}
func (kv *ShardKV)fetchNewConfig(){
	newConfig:=kv.mck.Query(kv.config.Num+1)
	if newConfig.Num==kv.config.Num{
		return
	}
	if kv.config.Num==0{

	}

	for i,g:=range newConfig.Shards{
		if g==kv.gid&&kv.config.Shards[i]!=kv.gid{//如果是新加入的分片
			kv.shardMu[i].Lock()
		}
	}
	kv.config=newConfig
}
func (kv *ShardKV) pollConfig() {
     for!kv.killed() {
     	kv.fetchNewConfig()
     	time.Sleep(80*time.Millisecond)
	 }
}

func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
	close(kv.applyCh)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(ExecuteReply{})
    labgob.Register(RequestArgs{})
	labgob.Register(ShardQueryArgs{})
	labgob.Register(ShardQueryReply{})
	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.config=shardctrler.Config{
		Num:    0,
		Shards: [10]int{},
		Groups: nil,
	}

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	go kv.pollConfig()

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.InstallSnapShot(persister.ReadSnapshot())

    go kv.doExecute()
	return kv
}
