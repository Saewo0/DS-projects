package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import "strconv"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
}

type Op struct {
  // Your definitions here.
  Name string
  Key string
  Value string
  DoHash bool
  OpID string
  // Fields for GETSHARD/RECONFIG
  Shard int
  ConfigNum int
  Config shardmaster.Config
  ConfInfo GetShardReply
}


type ShardKV struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  sm *shardmaster.Clerk
  px *paxos.Paxos

  gid int64 // my replica group ID

  // Your definitions here.
  doneSeq int
  config shardmaster.Config
  KVData map[string] string
  dupRPC map[string] RPCResult // record for dup PUT/GET request
}


func (kv *ShardKV) StartAgreement(op Op) (Err, string, GetShardReply) {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  
  ShardInfo := GetShardReply{OK, make(map[string] string), make(map[string] RPCResult)}
  // Check op
  switch op.Name {
  case "GET", "PUT":
    //check shard responsibility
    shard := key2shard(op.Key)
    if kv.gid != kv.config.Shards[shard]{
      return ErrWrongGroup, "", ShardInfo
    }
    if kv.dupRPC[op.OpID].IsExecuted {
      // Duplicate PUT/GET operations detected...
      return OK, kv.dupRPC[op.OpID].LastValue, ShardInfo
    }
  case "GETSHARD":
    if kv.config.Num < op.ConfigNum{
      return ErrNotReady, "", ShardInfo
    }
  case "RECONFIG":
    //check current config
    DPrintf("kv[%d]: curconf(%d) vs. RECONFIG(%d)...\n", kv.me, kv.config.Num, op.Config.Num)
    if  kv.config.Num >= op.Config.Num {
      return OK, "", ShardInfo
    }
  }

  opFinish := false
  for !opFinish {
    // check 
    seq := kv.doneSeq + 1
    DPrintf("kv[%d] starts op(%s) with seq=%d...\n", kv.me, op.Name, seq)
    kv.px.Start(seq, op)

    to := 10 * time.Millisecond
    decided := false
    var currentop Op
    var t interface{}
    // wait for Paxos instances to complete agreement
    for !decided {
      DPrintf("kv[%d] is waiting for seq=%d to complete...\n", kv.me, seq)
      decided, t = kv.px.Status(seq)
      time.Sleep(to)
      if to < 10 * time.Second{
        to *= time.Duration(rand.Float64()+1)
      }
    }
    currentop = t.(Op)
    // execute the currentop, curOP can be op itself or agreed op from other servers
    switch currentop.Name {
    case "GET":
      DPrintf("kv[%d]: Execute GET(%s) with ID(%s)...", kv.me, currentop.Key, currentop.OpID)
      oldValue, exist := kv.KVData[currentop.Key]
      if !exist {
        oldValue = ""
      }
      kv.dupRPC[currentop.OpID] = RPCResult{true, oldValue}
    case "PUT":
      DPrintf("kv[%d]: Execute PUT(%s,%s) with ID(%s)...", kv.me, currentop.Key, currentop.Value, currentop.OpID)
      oldValue, exist := kv.KVData[currentop.Key]
      if !exist {
        oldValue = ""
      }
      if currentop.DoHash {
        h := hash(oldValue + currentop.Value)
        val := strconv.Itoa(int(h))
        kv.KVData[currentop.Key] = val
      } else{
        kv.KVData[currentop.Key] = currentop.Value
      }
      kv.dupRPC[currentop.OpID] = RPCResult{true, oldValue}
    case "GETSHARD":
      DPrintf("kv[%d]: Execute GETSHARD(%d) with ID(%s)...", kv.me, currentop.Shard, currentop.OpID)
      for key := range kv.KVData{
        if key2shard(key) == currentop.Shard {
          ShardInfo.KVData[key] = kv.KVData[key]
        }
      }
      for OpID := range kv.dupRPC {
        ShardInfo.DupRPC[OpID] = kv.dupRPC[OpID]
      }
    case "RECONFIG":
      DPrintf("kv[%d]: Execute RECONFIG(%d)...", kv.me, currentop.Config.Num)
      info := &op.ConfInfo
      for key := range info.KVData {
        kv.KVData[key] = info.KVData[key]
      }
      for OpID := range info.DupRPC {
        kv.dupRPC[OpID] = info.DupRPC[OpID]
      }
      kv.config = currentop.Config
      DPrintf("kv[%d]: update to config(%d) with Shrads (%v) and Groups (%v)",
              kv.me, kv.config.Num, kv.config.Shards, kv.config.Groups)
    }

    kv.doneSeq = seq
    kv.px.Done(seq)

    opFinish = currentop.OpID == op.OpID
  }

  return OK, kv.dupRPC[op.OpID].LastValue, ShardInfo
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  op := Op{Name: "GET", Key: args.Key, OpID: args.OpID}
  reply.Err, reply.Value, _ = kv.StartAgreement(op)

  return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  op := Op{Name: "PUT", Key: args.Key, Value: args.Value, DoHash: args.DoHash, OpID: args.OpID}
  reply.Err, reply.PreviousValue, _ = kv.StartAgreement(op)

  return nil
}

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) error {
  op := Op{Name: "GETSHARD", Shard: args.Shard, ConfigNum: args.ConfigNum, OpID: args.OpID}
  _, _, result := kv.StartAgreement(op)
  reply.KVData = result.KVData
  reply.DupRPC = result.DupRPC
  return nil
}

func (kv *ShardKV) Reconfigure(conf shardmaster.Config) bool {
  ConfInfo := GetShardReply{OK, make(map[string] string), make(map[string] RPCResult)}
  curconf := &kv.config
  i := 0
  for i < shardmaster.NShards {
    gid := curconf.Shards[i]
    if gid != kv.gid && conf.Shards[i] == kv.gid {
      // in current conf, Shards[i] is assigned to Group[gid], not my group
      // in new conf, Shards[i] is assigned to my group
      GetShardID := "GETSGARD_GID_" + strconv.FormatInt(kv.gid, 10) +
                    "_CONFNUM_" + strconv.Itoa(curconf.Num) +
                    "_SHARD_" + strconv.Itoa(i)
      args := &GetShardArgs{Shard:i, ConfigNum: curconf.Num, OpID: GetShardID}
      var reply GetShardReply
      for _, srv := range curconf.Groups[gid]{
        ok := call(srv, "ShardKV.GetShard", args, &reply)
        if ok && reply.Err == OK{
          break
        }
        if ok && reply.Err == ErrNotReady {
          return false
        }
      }
      //process the replys, merge into one results
      ConfInfo.Merge(reply)
    }
    i += 1
  }
  ReconfID := "RECONFIG_GID_" + strconv.FormatInt(kv.gid, 10) +
              "_CONFNUM_" + strconv.Itoa(curconf.Num)
  op := Op{Name: "RECONFIG", Config: conf, OpID: ReconfID, ConfInfo: ConfInfo}
  kv.StartAgreement(op)
  return true
}

func (info *GetShardReply) Merge(newinfo GetShardReply) {
  for key := range newinfo.KVData {
    info.KVData[key] = newinfo.KVData[key]
  }
  for OpID := range newinfo.DupRPC {
    info.DupRPC[OpID] = newinfo.DupRPC[OpID]
  }
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
  //
  kv.mu.Lock()
  i := kv.config.Num + 1
  kv.mu.Unlock()

  newconf := kv.sm.Query(-1)
  DPrintf("kv[%d]: curconf(%d) vs. newconf(%d)...\n", kv.me, i-1, newconf.Num)
  for  i <= newconf.Num {
    conf := kv.sm.Query(i)
    if !kv.Reconfigure(conf) {
      DPrintf("kv[%d]'s Reconfiguration(%d) failed...\n", kv.me, conf.Num)
      return
    }
    i += 1
  }
}


// tell the server to shut itself down.
func (kv *ShardKV) kill() {
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
                 servers []string, me int) *ShardKV {
  gob.Register(Op{})

  kv := new(ShardKV)
  kv.me = me
  kv.gid = gid
  kv.sm = shardmaster.MakeClerk(shardmasters)

  // Your initialization code here.
  kv.doneSeq = 0
  kv.config.Groups = map[int64][]string{}
  kv.KVData = make(map[string] string)
  kv.dupRPC = make(map[string] RPCResult)
  // Don't call Join().

  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)


  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && kv.dead == false {
        fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  go func() {
    for kv.dead == false {
      kv.tick()
      time.Sleep(250 * time.Millisecond)
    }
  }()

  return kv
}
