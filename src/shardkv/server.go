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
	Args
	Server int
	Time   int64
}

type ShardKV struct {
	fechmu       sync.Mutex
	me           int
	rf           *raft.Raft
	dead         int32 // set by Kill()
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	mck *shardctrler.Clerk

	shardMu    [shardctrler.NShards]sync.Mutex
	shardChMap [shardctrler.NShards]sync.Map

	config             shardctrler.Config
	shardLastConfigNum map[int]int
	managedShards      map[int]bool
	kvMap              map[int]map[string]string
	shardLastIndex     map[int]map[int64]uint32
}

func (kv *ShardKV) FetchShardData(shard int) (reply Reply) {
	args := Args{
		RemoteInvalid: true,
		Type: FetchShard,
		Shard:         shard,
		ConfigNum:     kv.config.Num,
	}
	kv.shardExecute(&args, &reply)
	return
}
func (kv *ShardKV) InstallShardData(shard int, data []byte) (reply Reply) {
	args := Args{
		InstallInvalid: true,
		Type: InstallShard,
		Shard:         shard,
		ConfigNum:     kv.config.Num,
	}
	//深拷贝
	args.ShardData = make([]byte, len(data))
	copy(args.ShardData, data)
	kv.Do(&args, &reply)
	return reply
}
func (kv *ShardKV) InstallConfig(config shardctrler.Config) (reply Reply) { //确认更新config
	args := Args{
		InstallInvalid: true,
		Type: InstallConfig,
		Config:        config.Copy(), //深拷贝
	}
	kv.Do(&args, &reply)
	return reply
}
func (kv *ShardKV) DeleteShardData(shard int) (reply Reply) {
	args := Args{
		RemoteInvalid: true,
		Type: DeleteShard,
		Shard:         shard,
		ConfigNum:     kv.config.Num,
	}
	kv.shardExecute(&args, &reply)
	return
}
func (kv *ShardKV) shardExecute(args *Args, reply *Reply) {
	shard := args.Shard
	desGid := kv.config.Shards[shard]
	for {
		if servers, ok := kv.config.Groups[desGid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				ch := make(chan Reply, 1)
				//深拷贝,对每个service发送的请求一定要是单独的
				arg := args.Copy()
				go func() {
					reply := Reply{}
					ok := srv.Call("ShardKV.Do", &arg, &reply)
					if ok {
						ch <- reply
					}
				}()
				select {
				case <-time.After(time.Millisecond * 300):
				case *reply = <-ch:
				}
				if reply.Err == OK || kv.killed() {
					return
				}
			}
		}
	}
}
func (kv *ShardKV) Do(args *Args, reply *Reply) {
	if kv.killed() {
		return
	}
	shard := args.Shard
	kv.shardMu[shard].Lock()
	time.Sleep(time.Microsecond)
	now := time.Now().UnixNano()
	kv.shardMu[args.Shard].Unlock()
	ch := make(chan Reply, 1)
	kv.shardChMap[shard].Store(now, ch)
	defer kv.shardChMap[shard].Delete(now)
	_, _, ok := kv.rf.Start(Op{
		Args:   *args,
		Server: kv.me,
		Time:   now,
	})
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	select {
	case <-time.After(time.Millisecond * 500):
		reply.Err = ErrTimeOut
	case *reply = <-ch: //do execute向chenel发送reply之后就删除id的映射
	}
}

func (kv *ShardKV) SnapShot(index int) {
	if kv.killed() {
		return
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvMap)
	e.Encode(kv.shardLastIndex)
	e.Encode(kv.managedShards)
	e.Encode(kv.config)
	//不开线程去执行snapshot,因为可能会导致多次执行,增加负担
	kv.rf.Snapshot(index, w.Bytes())
}
func (kv *ShardKV) InstallSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	kv.kvMap = nil
	kv.shardLastIndex = nil
	kv.managedShards = nil
	kv.config = shardctrler.Config{
		Num:    0,
		Shards: [10]int{},
		Groups: nil,
	}
	d.Decode(&kv.kvMap)
	d.Decode(&kv.shardLastIndex)
	d.Decode(&kv.managedShards)
	d.Decode(&kv.config)
}
func (kv *ShardKV) getShardData(shard int) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvMap[shard])
	e.Encode(kv.shardLastIndex[shard])
	return w.Bytes()
}
func (kv *ShardKV) installShard(shard int, shardData []byte) {
	if shardData == nil || len(shardData) < 1 {
		kv.kvMap[shard] = make(map[string]string)
		kv.shardLastIndex[shard] = make(map[int64]uint32)
		return
	}
	r := bytes.NewBuffer(shardData)
	d := labgob.NewDecoder(r)
	kv.kvMap[shard] = nil
	kv.shardLastIndex[shard] = nil
	//不行就换深拷贝,或者把数据传递改为map,不用Byte
	d.Decode(kv.kvMap[shard])
	d.Decode(kv.shardLastIndex[shard])
}
func (kv *ShardKV) doExecute() {
	for args := range kv.applyCh {
		if kv.killed() {
			return
		}
		if args.CommandValid { //序列化
			op := args.Command.(Op)
			shard := op.Shard
			ch, ok := kv.shardChMap[shard].Load(op.Time)
			reply := Reply{}
			reply.Err = OK
			if op.InstallInvalid {
				if op.Type == InstallConfig &&kv.kvMap[shard]==nil{
					kv.installShard(shard, op.ShardData)
				} else if op.Type == InstallConfig {
					kv.config = op.Config
					for sd, g := range kv.config.Shards {
						if g == kv.gid {
							kv.managedShards[g] = true
							kv.shardLastConfigNum[sd] = kv.config.Num
						}
					}
				}
			}
			if op.RemoteInvalid {
				if op.ConfigNum > kv.config.Num { //当前配置没跟上,返回等待跟上配置
					reply.Err = ErrConfigToOld
					go kv.fetchNewConfig()
				} else {
					lastCigNum := kv.shardLastConfigNum[shard]
					if lastCigNum+1 == op.ConfigNum {
						kv.shardLastConfigNum[shard] = op.ConfigNum
						if op.Type == DeleteShard { //删除实际的数据
							delete(kv.shardLastIndex, shard)
							delete(kv.kvMap, shard)
						}
					}
					if op.Type == FetchShard && kv.kvMap[shard] != nil && ok && op.Server == kv.me {
						reply.ShardData = kv.getShardData(shard)
						delete(kv.managedShards, shard) //别人获取后自己不再管理
					}
				}
			}
			if op.RequestInvalid {
				if !kv.managedShards[shard] { //不再管理该分区
					reply.Err = ErrWrongGroup
				} else {
					lastMap := kv.shardLastIndex[shard] //引用传递,直接用
					lastIndex := lastMap[op.CkId]
					if lastIndex+1 == op.CkIndex { //避免同一客户端重复提交多次执行
						lastMap[op.CkId] = op.CkIndex //map是引用传递,可以直接用
						if op.Type == Puts {
							kv.kvMap[shard][op.Key] = op.Value
						} else if op.Type == Appends {
							kv.kvMap[shard][op.Key] += op.Value
						}
					}
					if op.Type == Gets && ok && op.Server == kv.me {
						reply.Value = kv.kvMap[shard][op.Key]
					}
				}
			}
			if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
				kv.SnapShot(args.CommandIndex)
			}
			if ok && kv.me == op.Server {
				ch.(chan Reply) <- reply
			}
		} else if args.SnapshotValid && kv.rf.CondInstallSnapshot(args.SnapshotTerm, args.SnapshotIndex, args.Snapshot) {
			// 必须先安装日志,再改具体的kv存储
			kv.InstallSnapShot(args.Snapshot)
		}
	}
}

//只需添加完当前一轮config新管理的shards,当前config就算完成(更新config num)
func (kv *ShardKV) fetchNewConfig() {
	kv.fechmu.Lock()
	defer kv.fechmu.Unlock()
	if _, ok := kv.rf.GetState(); !ok { //不是leader不处理
		return
	}
	newConfig := kv.mck.Query(kv.config.Num + 1)
	if newConfig.Num == kv.config.Num {
		return
	}
	if kv.config.Num == 0 { //第一个配置,自己管自己就行
		for shard, g := range newConfig.Shards {
			if g == kv.gid {
				reply := kv.InstallShardData(shard, nil)
				if reply.Err != OK { //有任何一个没安装上,都直接返回
					return
				}
			}
		}
		return
	}

	for shard, g := range newConfig.Shards {
		if !kv.managedShards[shard] && g == kv.gid { //当前config 新增的shard
			reply := kv.FetchShardData(shard) //有任何一个步骤出错都重来
			if reply.Err != OK {
				return
			}
			reply = kv.InstallShardData(shard, reply.ShardData)
			if reply.Err != OK {
				return
			}
			reply = kv.DeleteShardData(shard)
			if reply.Err != OK {
				return
			}
		}
	}
	kv.InstallConfig(newConfig)
}
func (kv *ShardKV) pollConfig() {
	for !kv.killed() {
		kv.fetchNewConfig()
		time.Sleep(80 * time.Millisecond)
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
	labgob.Register(Reply{})
	labgob.Register(Args{})
	labgob.Register(shardctrler.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)


	kv.applyCh = make(chan raft.ApplyMsg)
	kv.shardLastIndex = make(map[int]map[int64]uint32)
	kv.kvMap = make(map[int]map[string]string)
	kv.managedShards = make(map[int]bool)
	kv.config = shardctrler.Config{}
	kv.shardLastConfigNum = make(map[int]int)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.InstallSnapShot(persister.ReadSnapshot())

	go kv.pollConfig()
	go kv.doExecute()
	return kv
}
