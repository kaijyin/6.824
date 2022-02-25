package shardctrler

import (
	"6.824/labgob"
	"6.824/raft"
	"time"
)
import "6.824/labrpc"
import "sync"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs  []Config // indexed by config num


	chMap sync.Map
	ckLastIndex map[int64]uint32
}

type Op struct {
	// Your data here.
	Args
	Config
	Server int
	Time   int64
}

func (sc *ShardCtrler) lock() {
	sc.mu.Lock()
}

func (sc *ShardCtrler) unlock() {
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Do(args *Args, reply *Reply) {
	sc.lock()
	time.Sleep(time.Microsecond) //强制操作线性化
	now := time.Now().UnixNano()
	sc.unlock()
	newConfig:=Config{}
	if args.Type==Join{
		newConfig=sc.join(args)
	}else if args.Type==Leave{
		newConfig=sc.leave(args)
	}
	ch := make(chan Reply, 1)
	sc.chMap.Store(now, ch)
	defer sc.chMap.Delete(now)
	_, _, ok := sc.rf.Start(Op{
		Args: *args,
		Config: newConfig,
		Server: sc.me,
		Time:   now,
	})
	if !ok {
		return
	}
	select {
	case <-time.After(time.Millisecond*300):
	case *reply = <-ch: //do execute向chenel发送reply之后就删除id的映射
	}
}

func (sc *ShardCtrler) join(args *Args)Config {
	sc.lock()
	newConfig := sc.configs[len(sc.configs)-1].Copy()
	sc.unlock()
	newConfig.Num++
	for gid,sever:=range args.Servers{
		newConfig.Groups[gid] = make([]string, len(sever))
		copy(newConfig.Groups[gid], sever)
		if len(newConfig.Groups)== 1 { //唯一一组,特判
			for i, _ := range newConfig.Shards {
				newConfig.Shards[i] = gid
			}
			continue
		}
		if len(newConfig.Groups)>NShards{
			continue
		}
		low := NShards / len(newConfig.Groups)
		gnum := make(map[int]int)
		for _, g := range newConfig.Shards {
			gnum[g]++
		}
		lownum := len(newConfig.Groups) - (NShards % len(newConfig.Groups))
		//先填满小的,再填大的
		for i, g := range newConfig.Shards {
			if gnum[g] > low && lownum > 0 {
				gnum[g]--
				if gnum[g] == low {
					lownum--
				}
				newConfig.Shards[i] = gid
			} else if gnum[g] > low+1 {
				gnum[g]--
				newConfig.Shards[i] = gid
			}
		}
	}
	return newConfig
}
func (sc *ShardCtrler) leave(args *Args)Config {
	sc.lock()
	newConfig := sc.configs[len(sc.configs)-1].Copy()
	sc.unlock()
	newConfig.Num++
	unusedGrps:=make(map[int]bool)
	for g, _ := range newConfig.Groups {
		unusedGrps[g]=true
	}
	for _, g := range newConfig.Shards {
		delete(unusedGrps,g)
	}
	for _, gid := range args.GIDs {
		delete(newConfig.Groups,gid)
		if len(newConfig.Groups) == 0 { //唯一一组,特判
			for i, _ := range newConfig.Shards {
				newConfig.Shards[i] = 0
			}
			continue
		}
		if unusedGrps[gid]{//如果删除的group并没有使用
			delete(unusedGrps,gid)
			continue
		}
		if len(unusedGrps)>0{//还有多余的group
			g:=-1
			for g=range unusedGrps{
				break
			}
			for i,curg:=range newConfig.Shards{//用原本多出来的group去替换删除的group
				if curg==gid{
					newConfig.Shards[i]=g
				}
			}
			delete(unusedGrps,g)
			continue
		}
		low := NShards / len(newConfig.Groups)
		gnum := make(map[int]int)
		for _, g := range newConfig.Shards {
			if g != gid {
				gnum[g]++
			}
		}
		upnum := NShards % len(newConfig.Groups)
		//先填满大的,再填小的
		for i, gg := range newConfig.Shards {
			if gg == gid {
				for g, _ := range newConfig.Groups {
					if gnum[g] < low+1 && upnum > 0 {
						gnum[g]++
						if gnum[g] == low+1 {
							upnum--
						}
						newConfig.Shards[i] = g
						break
					} else if gnum[g] < low {
						gnum[g]++
						newConfig.Shards[i] = g
						break
					}
				}
			}
		}
	}
	return newConfig
}
func (sc *ShardCtrler) query(args QueryArgs) (config Config) { //深拷贝
	if args.Num == -1 || args.Num >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1].Copy()
	} else {
		return sc.configs[args.Num].Copy()
	}
}
func (sc *ShardCtrler) installConfig(config *Config)bool{
	sc.lock()
	defer sc.unlock()
	if config.Num!=len(sc.configs){
		return false
	}
	sc.configs=append(sc.configs,config.Copy())
	return true
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	close(sc.applyCh)
}
func (sc *ShardCtrler) doExecute() {
	for args := range sc.applyCh {
		if args.CommandValid{
			op := args.Command.(Op)
			ch, ok := sc.chMap.Load(op.Time)
			reply := Reply{}
			lastIndex := sc.ckLastIndex[op.CkId]
			reply.RequestApplied = true
			if lastIndex+1 == op.CkIndex { //避免同一客户端重复提交多次执行
				if op.Type==Join||op.Type==Leave{
                     if sc.installConfig(&op.Config){
                     	 sc.ckLastIndex[op.CkId]=op.CkIndex
					 }else{
					 	reply.RequestApplied=false
					 }
				}else{
					sc.ckLastIndex[op.CkId]=op.CkIndex
				}
			}
			if op.Type==Query&&	ok && op.Server == sc.me {
			    reply.Config = sc.query(op.QueryArgs)
			}
			if ok && sc.me == op.Server {
				ch.(chan Reply) <- reply
			}
		}
	}
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func
StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.ckLastIndex = make(map[int64]uint32)

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(Reply{})
	labgob.Register(QueryArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(JoinArgs{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	go sc.doExecute()
	return sc
}
