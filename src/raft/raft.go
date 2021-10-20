package raft



//this is an outline of the API that raft must expose to
//the service (or tester). see comments below for
//each of these functions for more details.
//
//rf = Make(...)
//  create a new Raft server.
//rf.Start(command interface{}) (index, term, isleader)
//  start agreement on a new log entry
//rf.GetState() (term, isLeader)
//  ask a Raft for its current term, and whether it thinks it is leader
//ApplyMsg
//  each time a new entry is committed to the log, each Raft peer
//  should send an ApplyMsg to the service (or tester)
//  in the same server.


import (
	"bytes"
	//"labgob"
	"math/rand"
	//"bytes"
	"sync"
	"sync/atomic"
	time "time"

	"6.824/labgob"
	"6.824/labrpc"
)


//
//as each Raft peer becomes aware that successive log entries are
//committed, the peer should send an ApplyMsg to the service (or
//tester) on the same server, via the applyCh passed to Make(). set
//CommandValid to true to indicate that the ApplyMsg contains a newly
//committed log entry.

//in part 2D you'll want to send other kinds of messages (e.g.,
//snapshots) on the applyCh, but set CommandValid to false for these
//other uses.

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//

const(
	Flower int=1
	Candidate int=2
	Leader int=3
)
//设定election时间
const (
	electionTimeoutTop int64= 300
	elctionTimeoutDown int64= 200
	hertbeatInterval int64 = 80
)
type Log_ struct {
	Comand interface{}
	Term_ int
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh     chan ApplyMsg
	logs        []Log_
	logLen      int
	voteFor     int
	votes       int
	term        int
	lastReceive time.Time

	commit  int
	applied int
	peerCount int

	statu_  int
	next_   []int
	match_  []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock();
	defer rf.mu.Unlock()
	return rf.term, rf.statu_==Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	//DPrintf("%d:encode",rf.me)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.logs)
	e.Encode(rf.voteFor)
	e.Encode(rf.term)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var  logs []Log_
	d.Decode(&logs)
	rf.logs =logs
	var votefor int
	d.Decode(&votefor)
	rf.voteFor =votefor
	var term int
	d.Decode(&term)
	rf.term =term
	rf.logLen=len(rf.logs)-1
	//DPrintf("%d:decode loglen:%d term:%d votefor:%d ",rf.me,rf.logLen,rf.term,rf.voteFor)
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted int
}


//example code to send a RequestVote RPC to a server.
//server is the index of the target server in rf.peers[].
//expects RPC arguments in args.
//fills in *reply with RPC reply, so caller should
//pass &reply.
//the types of the args and reply passed to Call() must be
//the same as the types of the arguments declared in the
//handler function (including whether they are pointers).
//
//The labrpc package simulates a lossy network, in which servers
//may be unreachable, and in which requests and replies may be lost.
//Call() sends a request and waits for a reply. If a reply arrives
//within a timeout interval, Call() returns true; otherwise
//Call() returns false. Thus Call() may not return for a while.
//A false return can be caused by a dead server, a live server that
//can't be reached, a lost request, or a lost reply.
//
//Call() is guaranteed to return (perhaps after a delay) *except* if the
//handler function on the server side does not return.  Thus there
//is no need to implement your own timeouts around Call().
//
//look at the comments in ../labrpc/labrpc.go for more details.
//
//if you're having trouble getting RPC to work, check that you've
//capitalized all field names in structs passed over RPC, and
//that the caller passes the address of the reply struct with &, not
//the struct itself.
type ApendArgs struct {
	Term int
	LeaderId int
	PrevLogIdx int
	PrevLogTerm int
	Entries  []Log_
	LeaderCommit int
}
type ApendReply struct {
	Term int
	Idx int
}


/*
添加日志,做持久化,发送消息
*/
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.lock()
	if rf.killed()||rf.statu_!=Leader{
		rf.unlock()
		return -1,-1,false
	}
	rf.logLen++;
	rf.logs =append(rf.logs,Log_{Term_: rf.term,Comand: command})
	index:=rf.logLen
	term:=rf.term
	rf.persist()
	rf.unlock()
	rf.sendMsg(false)
	return index, term, true
}
func (rf *Raft) sendMsg(heartbeat bool)  {
	rf.lock()
	if rf.killed()||rf.statu_!=Leader{
		rf.unlock()
		return
	}
	rf.unlock()
	//日志复制不需要等待,只需要在收到回复后统计结果就行,结果只对Leader自身的comit有影响
	//而Leader选举就需要等待,因为需要统计结果,判断是否能成为Leader,保证一轮只有一个Leader
	for i:=0;i<rf.peerCount;i++{
		if i==rf.me {
			continue
		}
		go func(i int) {
			rf.lock()
			var entry []Log_ =nil
			entry=rf.logs[rf.next_[i]:]
			args := ApendArgs{rf.term, rf.me, rf.next_[i]-1, rf.logs[rf.next_[i]-1].Term_, entry, rf.commit}
			reply := ApendReply{}
			rf.unlock()
			send:=rf.sendApendence(i,&args,&reply)
			if send{
				rf.recieveApendReplay(i,&reply)
			}
		}(i)
	}
}

func (rf *Raft) Apendence(args *ApendArgs,reply *ApendReply)  {//reply的idx表示和Leader日志中一致的位置
	rf.lock()
	defer rf.unlock()
	if rf.killed(){
		return
	}
	//自己的任期比请求任期高,有可能是RPC延迟,或者之前的Leader掉线后又重连
	if rf.term >args.Term{
		reply.Idx=rf.commit
		reply.Term=rf.term
		return
	}
	//在RPC请求中,收到高任期的请求一定要跟新自己的任期,并成为flower
	if rf.term <args.Term{
		rf.BeFlower(args.Term)
	}
	//收到Rpc一定要刷新选举超时时间
	rf.flashRpc()
	previdx := args.PrevLogIdx
	prevTerm := args.PrevLogTerm
	//收到不可靠的Rpc
	//条件1:如果说是有一个拓机很久又重连的,然后Leader初始的next又比较大
	//条件2:如果发送前一个不匹配,需要再往前退
	//条件3:由于RPC超时,或者重新选举(Leader已经复制日志到大部分成功并提交,但是flower选举超时,重新成为leader)造成,
	//操作:直接退回到commit,减少RPC请求次数
	if rf.logLen <previdx || rf.logs[previdx].Term_!=prevTerm||previdx<rf.commit{
		reply.Idx=rf.commit
		reply.Term=rf.term
		return
	}
	//此时当前服务机previdx前的日志都全部正确,再把新发送的日志复制
	previdx++
	j:=0
	//复制日志
	lengh :=len(args.Entries)
	for ;previdx<=rf.logLen &&j< lengh;{
		if rf.logs[previdx].Term_!=args.Entries[j].Term_{
			rf.logs = rf.logs[:previdx]
			break
		}
		previdx++
		j++
	}
	for ;j< lengh;j++{
		rf.logs =append(rf.logs,args.Entries[j])
	}
	rf.logLen = len(rf.logs)-1
	//日志更改,需要做持久化操作,持久化操作要在Leader收到reply并comit之前做,所以收到就处理是最合适的
	rf.persist()
	//提交日志
	if args.LeaderCommit>rf.commit{//有可能因为网络延迟没有刷新Flower的RPCtimer,重新选举,原本的Leader的尽管commit更高,但是也成为了Flower
		rf.commit=args.LeaderCommit
		for rf.applied<rf.commit{
			rf.applied++
			rf.applyCh<-ApplyMsg{CommandValid: true, Command: rf.logs[rf.applied].Comand, CommandIndex: rf.applied}
		}
	}
	//此时服务机收到复制请求的部分就和Leader是一样的了,只要Leader不更改,并收到大部分复制日志成功的reply后跟新comit,下次再发送日志复制请求自己也跟着跟新comit
	//repley.idx=rf.len不对,可能Flower的日志比Leader的日志要长,但是前面确实吻合的,这是由于超时后重新选举造成的
	//你可能会问,这不是不对嘛,投票的时候,不是更长的优先级更高吗,是啊,但是只要先进入选举,任期就更高,原本Leader收到请求,只能乖乖变为flower,虽然曾经的Leader不会投票给他
	//但是只要收到过半的投票就行了呀
	reply.Idx=args.PrevLogIdx+ len(args.Entries)
	reply.Term=rf.term
}
func (rf *Raft) recieveApendReplay(i int,reply *ApendReply){
	rf.lock()
	defer rf.unlock()
	//时刻进行状态检查,减少多余的计算
	if rf.killed()||rf.statu_!=Leader{
		return
	}
	//收到高任期的reply
	if reply.Term>rf.term { //可能是之前Flower集体拓机,之后新的Leader也拓机,Flower重新选举...总之一定要保持term一致
		rf.BeFlower(reply.Term)
		return
	}
	//不属于同一任期的不参与判断
	if reply.Term!=rf.term {//可能是RPC延迟导致的,没用的reply,直接丢弃
		return
	}

	//Flower的reply.Idx以及之前的log已经和Leader相同

	//更新下一次发送的next标记
	rf.next_[i]=reply.Idx+1
	rf.match_[i]=rf.next_[i]-1

	//开始进入Leader提交日志判断
	//条件1:返回的replyidx不是当前任期发送的日志,即日志没有复制成功
	//条件2:日志提交comit都大于reply.idx了,不用再用来判断是否更新commit
	if rf.logs[reply.Idx].Term_!=rf.term|| rf.commit>=reply.Idx{
		return
	}
	//判断是否集群中大部分服务器都复制了idx以及之前的日志,如果是,则Leader提交日志到idx
	count:=1
	for i:=0;i<rf.peerCount;i++{
		if i==rf.me {
			continue
		}
		if rf.match_[i]>=reply.Idx{
			count++
		}
	}
	if count>=(rf.peerCount+1)/2{//大部分都已经复制,提交
		rf.commit=reply.Idx
		for rf.applied<rf.commit{
			rf.applied++
			rf.applyCh<-ApplyMsg{CommandValid: true, Command: rf.logs[rf.applied].Comand, CommandIndex: rf.applied}
		}
	}
}
func (rf *Raft) sendApendence(server int,args *ApendArgs,reply *ApendReply)bool  {
	//DPrintf("%d send appendecne to %d",rf.me,server)
	ok := rf.peers[server].Call("Raft.Apendence", args, reply)
	return ok
}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//DPrintf("%d send req to %d",rf.me,server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) getElectionTime() time.Duration  {//获取down-up的随机超时时间
	i := rand.Int63()%(electionTimeoutTop-elctionTimeoutDown) + elctionTimeoutDown
	//DPrintf("server %d election time :%d",rf.me,i)
	return time.Millisecond*time.Duration(i)
}
func (rf *Raft) flashRpc()  {
	rf.lastReceive=time.Now()
}
func (rf *Raft) lock()  {
	rf.mu.Lock()
}
func (rf *Raft) unlock()  {
	//DPrintf("%d unlock",rf.me)
	rf.mu.Unlock()
}
func (rf *Raft) election()  {
	//DPrintf("%d开始选举",rf.me)
	rf.lock()
	if rf.killed() ||rf.statu_==Leader{
		rf.unlock()
		return
	}
	rf.BeCandidate()
	electionTerm :=rf.term
	request := RequestVoteArgs{rf.term, rf.me, rf.logLen, rf.logs[rf.logLen].Term_}
	rf.unlock()
	finish:=int64(1)
	ok:=false
	cond := sync.NewCond(new(sync.Mutex))
	for i:=0;i<rf.peerCount;i++{
		if i==rf.me{
			continue
		}
		reply := RequestVoteReply{-1,0}
		go func(server int) {
			send := rf.sendRequestVote(server, &request, &reply)
			if send&&rf.reciveVoteReply(&reply){
				ok=true
			}
			atomic.AddInt64(&finish,1)
			cond.Broadcast()
		}(i)
	}
	cond.L.Lock()
	defer cond.L.Unlock()
	//阻塞,等待收到所有人的回复,或者大部分人投票通过,或者RPC请求延迟,进入新的任期,收到其余服务器的RPC后状态改变后,停止阻塞
	for  !rf.killed(){
		rf.lock()
		if !ok&&int(finish)!=rf.peerCount&&rf.term== electionTerm {
			rf.unlock()
			cond.Wait()
		}else {
			rf.unlock()
			break
		}
	}
	//DPrintf("%d election in %d finished",rf.me,electionTerm)
}
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//DPrintf("%d recive req from %d",rf.me,args.CandidateId)
	rf.lock()
	defer rf.unlock()
	reply.VoteGranted =0
	lastIdx :=rf.logLen
	lastTerm :=rf.logs[rf.logLen].Term_
	if rf.killed()||args.Term <rf.term {
		reply.Term =rf.term
		return
	}
	//收到高任期RPC要成为Flower,并继续投票
	if args.Term >rf.term {
		rf.BeFlower(args.Term)
	}
	//在当前任期中已经投票过了,不再参与投票
	if rf.voteFor !=-1{
		reply.Term =rf.term
		return
	}
	//可以进行投票,开始比较谁的日志更新
	//最后的日志任期更大,说明已经接受到新Leader的日志复制,并成功
	//如果相同,则长度越长,更可能当leader
	if lastTerm >args.LastLogTerm {
		reply.Term =rf.term
		return
	}
	if lastTerm ==args.LastLogTerm && lastIdx >args.LastLogIndex {
		reply.Term =rf.term
		return
	}
	//投票
	reply.VoteGranted =1
	rf.voteFor =args.CandidateId
	reply.Term =rf.term
	//收到RPC请求刷新选举超时时间
	rf.flashRpc()
	//投票成功,持久化,不然可能出现一个人在一轮给两个人投票的情况
	rf.persist()
}
func (rf *Raft) reciveVoteReply(reply *RequestVoteReply) bool {
	rf.lock()
	defer rf.unlock()
	if rf.killed()||rf.statu_!=Candidate{
		return false
	}
	if reply.Term>rf.term {
		rf.BeFlower(reply.Term)
		return false
	}
	if reply.Term==rf.term&&reply.VoteGranted ==1{
		rf.votes++
	}
	if rf.votes>rf.peerCount/2{
		rf.BeLeader()
		return true
	}
	return false
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
func (rf *Raft) BeFlower(term int)  {
	rf.statu_=Flower
	rf.voteFor =-1
	rf.term =term
	rf.persist()
	rf.flashRpc()
}
func (rf *Raft) BeCandidate()  {
	rf.statu_=Candidate
	rf.voteFor =rf.me
	rf.votes=1
	rf.term++
	//任期更改,持久化
	rf.persist()
	rf.flashRpc()
}
func (rf *Raft) BeLeader()  {
	//DPrintf("%d become leader",rf.me)
	rf.statu_=Leader
	for i:=0;i<rf.peerCount;i++{
		rf.next_[i]=rf.commit+1
		rf.match_[i]=0
	}
	go rf.heartTiker()
}
func (rf *Raft) heartTiker()  {
	//选举结束立刻发送心跳
	for !rf.killed(){
		rf.lock()
		if rf.statu_!=Leader{
			rf.unlock()
			break
		}
		rf.unlock()
		rf.sendMsg(true)
		time.Sleep(time.Duration(hertbeatInterval))
	}
}
func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.flashRpc()
		timeOut:=rf.getElectionTime()
		time.Sleep(timeOut)
		rf.lock()
		statu:=rf.statu_
		last:=rf.lastReceive
		rf.unlock()
		if statu!=Leader&&time.Now().Sub(last).Milliseconds() >timeOut.Milliseconds(){
			 go rf.election()
		}
	}
}
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh=applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.dead=0
	rf.statu_=Flower
	rf.voteFor =-1
	rf.term =0
	rf.peerCount=len(rf.peers)
	//根据论文指示,把初始下标设置为1,提前增添一个空的log,确实会方便狠多
	rf.logs =make([]Log_,1)
	rf.logs[0]=Log_{0,0}
	rf.commit=0
	rf.logLen =0
	rf.applied=0
	rf.next_=make([]int,rf.peerCount)
	rf.match_=make([]int,rf.peerCount)
	rand.Seed(time.Now().Unix())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
