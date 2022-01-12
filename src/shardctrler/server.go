package shardctrler

import (
	"6.824/labgob"
	"6.824/raft"
	"sort"
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
	// Your definitions here.
	curIndex    int
	kvMap       map[string]string
	ckLastIndex map[int64]uint32
}

type Op struct {
	// Your data here.
	Args
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
	//去除掉以及处理了的请求,对网络不可信的情况下很常见,必须,因为我读请求也只会执行一次,类似zookepper,
	lastIdx := sc.ckLastIndex[args.CkId]
	if lastIdx >= args.CkIndex {
		reply.RequestApplied = true
		if args.Type == Query {
			queryArgs := args.Reqargs.(QueryArgs)
			reply.Config = sc.query(queryArgs)
		}
		sc.unlock()
		return
	}
	time.Sleep(time.Microsecond) //强制操作线性化
	now := time.Now().UnixNano()
	ch := make(chan Reply, 1)
	sc.chMap.Store(now, ch)
	defer sc.chMap.Delete(now)
	_, _, ok := sc.rf.Start(Op{
		Args:   *args,
		Server: sc.me,
		Time:   now,
	})
	if !ok {
		sc.unlock()
		return
	}
	sc.unlock()
	select {
	case <-time.After(time.Second):
	case *reply = <-ch: //do execute向chenel发送reply之后就删除id的映射
	}
}

func (sc *ShardCtrler) join(args JoinArgs) {
	preConfig := sc.configs[len(sc.configs)-1]
	newConfig := preConfig.Copy()
	newConfig.Num++
	gs:=Gset{}
	for gid, sever := range args.Servers {
		gs=append(gs,Gpair{
			gid:     gid,
			service: sever,
		} )
	}
	sort.Sort(gs)
	for _,g:=range gs{
		gid:=g.gid
		sever:=g.service
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
	sc.configs = append(sc.configs, newConfig)
}
func (sc *ShardCtrler) move(args MoveArgs) {
	preConfig := sc.configs[len(sc.configs)-1]
	newConfig := preConfig.Copy()
	newConfig.Num++
	newConfig.Shards[args.Shard]=args.GID
	sc.configs=append(sc.configs,newConfig)
}
func (sc *ShardCtrler) leave(args LeaveArgs) {
	preConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{Num: len(sc.configs)}
	newConfig.Groups = make(map[int][]string)
	unusedGrps:=make(map[int]int)
	for k, v := range preConfig.Groups {
		newConfig.Groups[k] = make([]string, len(v))
		copy(newConfig.Groups[k], v)
		unusedGrps[k]++
	}
	for i, g := range preConfig.Shards {
		newConfig.Shards[i] = g
		delete(unusedGrps,g)
	}
	gs:=[]int{}
	for g:=range unusedGrps{
		gs=append(gs,g)
	}
	sort.Ints(gs)
	for _, gid := range args.GIDs {
		delete(newConfig.Groups,gid)
		if len(newConfig.Groups) == 0 { //唯一一组,特判
			for i, _ := range newConfig.Shards {
				newConfig.Shards[i] = 0
			}
			continue
		}
		if unusedGrps[gid]!=0{//如果删除的gid并没有使用
			delete(unusedGrps,gid)
			continue
		}
		if len(unusedGrps)>0{//还有空位
			g:=gs[0]
			gs=gs[1:]
			for unusedGrps[g]==0{//不存在,说明被上一条件删除了
				g=gs[0]
				gs=gs[1:]
			}
			for i,curg:=range newConfig.Shards{
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
	sc.configs = append(sc.configs, newConfig)
}
func (sc *ShardCtrler) query(args QueryArgs) (config Config) { //深拷贝
	if args.Num == -1 || args.Num >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1].Copy()
	} else {
		return sc.configs[args.Num].Copy()
	}
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
		sc.lock()
		if args.CommandValid && sc.curIndex+1 == args.CommandIndex { //序列化
			sc.curIndex = args.CommandIndex
			op := args.Command.(Op)
			ch, ok := sc.chMap.Load(op.Time)
			reply := Reply{}
			lastIndex := sc.ckLastIndex[op.CkId]
			if lastIndex+1 == op.CkIndex { //避免同一客户端重复提交多次执行
				sc.ckLastIndex[op.CkId] = op.CkIndex
				reply.RequestApplied = true
				//DPrintf("%d execute CkId:%d index:%d",sc.me,op.CkId,op.CkIndex)
				switch op.Type {
				case Query:
					if ok && op.Server == sc.me {
						queryArgs := op.Reqargs.(QueryArgs)
						reply.Config = sc.query(queryArgs)
					}
				case Join: //是否需要改为值传递
					joinArgs := op.Reqargs.(JoinArgs)
					sc.join(joinArgs)
				case Move:
					moveArgs := op.Reqargs.(MoveArgs)
					sc.move(moveArgs)
				case Leave:
					leaveArgs := op.Reqargs.(LeaveArgs)
					sc.leave(leaveArgs)
				}
			}
			//可能kv index之前的并没有被实际执行,而是直接安装snapshot,所以还是需要把等待的请求给去除掉
			if ok && sc.me == op.Server {
				ch.(chan Reply) <- reply
			}
		}
		sc.unlock()
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

	sc.kvMap = make(map[string]string)
	sc.ckLastIndex = make(map[int64]uint32)

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(Reply{})
	labgob.Register(QueryArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(JoinArgs{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	go sc.doExecute()
	return sc
}
