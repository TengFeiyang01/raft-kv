package shardctrler

import (
	"course/labgob"
	"course/labrpc"
	"course/raft"
	"sync"
	"sync/atomic"
	"time"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	dead           int32 // set by Kill()
	lastApplied    int
	stateMachine   *CtrlerStateMachine
	notifyChans    map[int]chan *OpReply
	duplicateTable map[int64]LastOperationInfo
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	var opReply OpReply
	sc.command(Op{
		OpType:   OpJoin,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		Servers:  args.Servers,
	}, &opReply)

	reply.Err = opReply.Err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	var opReply OpReply
	sc.command(Op{
		OpType:   OpLeave,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		GIDs:     args.GIDs,
	}, &opReply)

	reply.Err = opReply.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	var opReply OpReply
	sc.command(Op{
		OpType:   OpMove,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		Shard:    args.Shard,
		GID:      args.GID,
	}, &opReply)

	reply.Err = opReply.Err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	var opReply OpReply
	sc.command(Op{
		OpType: OpQuery,
		Num:    args.Num,
	}, &opReply)

	reply.Config = opReply.ControllerConfig
	reply.Err = opReply.Err
}

func (sc *ShardCtrler) command(args Op, reply *OpReply) {
	// 判断请求是否重复
	sc.mu.Lock()
	if args.OpType != OpQuery && sc.requestDuplicated(args.ClientId, args.SeqId) {
		// 如果是重复请求 直接返回结果
		opReply := sc.duplicateTable[args.ClientId].Reply
		reply.Err = opReply.Err
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	// 调用 raft，将请求存储到 raft 日志中并进行同步
	index, _, isLeader := sc.rf.Start(args)

	// 如果不是 Leader 的话，直接返回错误
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 等待结果
	sc.mu.Lock()
	notifyCh := sc.getNotifyChannel(index)
	sc.mu.Unlock()

	select {
	case result := <-notifyCh:
		reply.ControllerConfig = result.ControllerConfig
		reply.Err = result.Err
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
	}

	// 删除通知的 channel
	go func() {
		sc.mu.Lock()
		sc.removeNotifyChannel(index)
		sc.mu.Unlock()
	}()
}

// Kill the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// Raft needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// StartServer servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.dead = 0
	sc.lastApplied = 0
	sc.stateMachine = NewCtrlerStateMachine()
	sc.notifyChans = make(map[int]chan *OpReply)
	sc.duplicateTable = make(map[int64]LastOperationInfo)

	go sc.applyTask()
	return sc
}

func (sc *ShardCtrler) requestDuplicated(clientId, seqId int64) bool {
	info, ok := sc.duplicateTable[clientId]
	return ok && seqId <= info.SeqId
}

// 处理 apply 任务
func (sc *ShardCtrler) applyTask() {
	for !sc.killed() {
		select {
		case message := <-sc.applyCh:
			if message.CommandValid {
				sc.mu.Lock()
				// 如果是已经处理过的消息则直接忽略
				if message.CommandIndex <= sc.lastApplied {
					sc.mu.Unlock()
					continue
				}
				sc.lastApplied = message.CommandIndex

				// 取出用户的操作信息
				op := message.Command.(Op)
				var opReply *OpReply
				if op.OpType != OpQuery && sc.requestDuplicated(op.ClientId, op.SeqId) {
					opReply = sc.duplicateTable[op.ClientId].Reply
				} else {
					// 将操作应用状态机中
					opReply = sc.applyLogToStateMachine(op)
					if op.OpType != OpQuery {
						sc.duplicateTable[op.ClientId] = LastOperationInfo{
							SeqId: op.SeqId,
							Reply: opReply,
						}
					}
				}

				// 将结果发送回去
				if _, isLeader := sc.rf.GetState(); isLeader {
					notifyCh := sc.getNotifyChannel(message.CommandIndex)
					notifyCh <- opReply
				}

				sc.mu.Unlock()
			}
		}
	}
}

func (sc *ShardCtrler) applyLogToStateMachine(op Op) *OpReply {
	var err Err
	var config Config
	switch op.OpType {
	case OpJoin:
		err = sc.stateMachine.Join(op.Servers)
	case OpLeave:
		err = sc.stateMachine.Leave(op.GIDs)
	case OpMove:
		err = sc.stateMachine.Move(op.Shard, op.GID)
	case OpQuery:
		config, err = sc.stateMachine.Query(op.Num)
	}
	return &OpReply{ControllerConfig: config, Err: err}
}

func (sc *ShardCtrler) getNotifyChannel(index int) chan *OpReply {
	if _, ok := sc.notifyChans[index]; !ok {
		sc.notifyChans[index] = make(chan *OpReply, 1)
	}
	return sc.notifyChans[index]
}

func (sc *ShardCtrler) removeNotifyChannel(index int) {
	delete(sc.notifyChans, index)
}
