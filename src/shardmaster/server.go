package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import (
  crand "crypto/rand"
  mbig "math/big"
  "sort"
  "time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
}

func nrand() int64 {
  max := mbig.NewInt(int64(1) << 62)
  bigx, _ := crand.Int(crand.Reader, max)
  x := bigx.Int64()
  return x
 }

type ShardMaster struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  configs []Config // indexed by config num
  doneSeq int
  knownConfigNum int
  workLoad map[int64] int // gid -> #shards
}

type Op struct {
  // Your definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  Name string
  GID int64
  Servers []string
  Shard int
  QueryNum int
  OpID int64
}

type GC struct {
    GID int64
    COUNT int
}

func (sm *ShardMaster) CopyConfig() {
  last := sm.configs[sm.knownConfigNum]
  new := Config{Num: last.Num+1}
  // copy the last config's Shards and Groups
  new.Shards = last.Shards
  new.Groups = map[int64][]string{}
  for Gid, servers := range last.Groups {
    new.Groups[Gid] = servers
  }
  sm.configs = append(sm.configs, new)
  sm.knownConfigNum += 1
}

func (sm *ShardMaster) Sort(ascend bool) []GC {
  var tmp []GC
  if len(sm.workLoad) > 0 {
    for gid, count := range sm.workLoad {
        tmp = append(tmp, GC{gid, count})
    }
    if ascend {
      sort.Slice(tmp, func(i, j int) bool {
          return tmp[i].COUNT < tmp[j].COUNT
      })
    } else {
      sort.Slice(tmp, func(i, j int) bool {
          return tmp[i].COUNT > tmp[j].COUNT
      })
    }
  } else {
    DPrintf("No workLoad info...")
  }
  return tmp
}

func (sm *ShardMaster) Rebalance(Gid int64, leave bool) {
  // Load the current config
  config := &sm.configs[sm.knownConfigNum]
  numGroups := len(config.Groups)
  
  if leave {
    //
    sorted := sm.Sort(true)
    index := 0
    for shard, gid := range config.Shards {
      if gid == Gid {
        config.Shards[shard] = sorted[index].GID
        sm.workLoad[sorted[index].GID] += 1
        index = (index + 1) % numGroups
      }
    }
  } else {
    //
    sorted := sm.Sort(false)
    numSorted := len(sorted)
    if numSorted == 0 {
      numSorted = 1
    }
    
    index := 0
    _, existed := sm.workLoad[Gid]
    if !existed {
      sm.workLoad[Gid] = 0
    }
    for sm.workLoad[Gid] < int(NShards / numGroups) {
      for shard, gid := range config.Shards {
        if gid == Gid {
          continue
        } else {
          if gid == 0 || gid == sorted[index].GID {
            DPrintf("Join: Shard(%d) is assigned to Group(%d)...", shard, Gid)
            config.Shards[shard] = Gid
            sm.workLoad[Gid] += 1
            if gid != 0 {
              sm.workLoad[gid] -= 1
            }
            index = (index + 1) % numSorted
            break
          }
        }
      }
    }
  }
}

func (sm *ShardMaster) StartAgreement(op Op) Config {
  sm.mu.Lock()
  defer sm.mu.Unlock()

  var result Config
  opFinish := false
  for !opFinish {
    // check 
    seq := sm.doneSeq + 1
    to := 10 * time.Millisecond
    decided := false
    var currentop Op
    var t interface{}

    DPrintf("sm[%d] start (seq=%d, opID=%d )...\n", sm.me, seq, op.OpID)
    sm.px.Start(seq, op)
    // wait for Paxos instances to complete agreement
    for !decided {
      DPrintf("sm[%d] is waiting for seq=%d to complete...\n", sm.me, seq)
      decided, t = sm.px.Status(seq)
      time.Sleep(to)
      if to < 10 * time.Second {
        to *= time.Duration(rand.Float64()+1)
      }
    }
    currentop = t.(Op)
    // execute the currentOp, it can be either op itself
    // or agreed op from other servers
    switch currentop.Name {
    case "Join":
      DPrintf("sm[%d]: Execute Join(%d, %v)...", sm.me, currentop.GID, currentop.Servers)
      _, existed := sm.configs[sm.knownConfigNum].Groups[currentop.GID]
      if !existed {
        sm.CopyConfig()
        sm.configs[sm.knownConfigNum].Groups[currentop.GID] = currentop.Servers
        sm.Rebalance(currentop.GID, false)
      }
    case "Leave":
      DPrintf("sm[%d]: Execute Leave(%d)...", sm.me, currentop.GID)
      _, existed := sm.configs[sm.knownConfigNum].Groups[currentop.GID]
      if existed {
        sm.CopyConfig()
        delete(sm.configs[sm.knownConfigNum].Groups, currentop.GID)
        delete(sm.workLoad, currentop.GID)
        sm.Rebalance(currentop.GID, true)
      }
    case "Move":
      DPrintf("sm[%d]: Execute Move(%d to %d)...", sm.me, currentop.Shard, currentop.GID)
      victim := sm.configs[sm.knownConfigNum].Shards[currentop.Shard]
      if victim != currentop.GID {
        sm.CopyConfig()
        sm.configs[sm.knownConfigNum].Shards[currentop.Shard] = currentop.GID
        sm.workLoad[currentop.GID] += 1
        if victim != 0 {
          sm.workLoad[victim] -= 1
        }
      }
    case "Query":
      DPrintf("sm[%d]: Execute Query(%d)...", sm.me, currentop.QueryNum)
      if currentop.QueryNum == -1 || currentop.QueryNum >= sm.knownConfigNum {
        result = sm.configs[sm.knownConfigNum]
      } else {
        result = sm.configs[currentop.QueryNum]
      }
    }

    sm.doneSeq = seq
    sm.px.Done(seq)

    opFinish = currentop.OpID == op.OpID
  }
  return result
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
  // Your code here.
  DPrintf("sm[%d]: Receive Join(%d, %v)...", sm.me, args.GID, args.Servers)
  op := Op{Name: "Join", GID: args.GID, Servers: args.Servers, OpID: nrand()}
  sm.StartAgreement(op)

  return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
  // Your code here.
  op := Op{Name: "Leave", GID: args.GID, OpID: nrand()}
  sm.StartAgreement(op)

  return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
  // Your code here.
  op := Op{Name: "Move", GID: args.GID, Shard: args.Shard, OpID: nrand()}
  sm.StartAgreement(op)

  return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
  // Your code here.
  DPrintf("sm[%d]: Receive Query(%d)...", sm.me, args.Num)
  op := Op{Name: "Query", QueryNum: args.Num, OpID: nrand()}
  config := sm.StartAgreement(op)
  DPrintf("sm[%d]: Reply Query(%d) with config(%d)", sm.me, args.Num, config.Num)
  reply.Config = config

  return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
  sm.dead = true
  sm.l.Close()
  sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *ShardMaster {
  gob.Register(Op{})

  sm := new(ShardMaster)
  sm.me = me

  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}

  sm.doneSeq = 0
  sm.knownConfigNum = 0
  sm.workLoad = make(map[int64] int)

  rpcs := rpc.NewServer()
  rpcs.Register(sm)

  sm.px = paxos.Make(servers, me, rpcs)
  
  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  sm.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for sm.dead == false {
      conn, err := sm.l.Accept()
      if err == nil && sm.dead == false {
        if sm.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if sm.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && sm.dead == false {
        fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
        sm.Kill()
      }
    }
  }()

  return sm
}
