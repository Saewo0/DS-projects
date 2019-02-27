package kvpaxos

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
import "strconv"
import "time"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
}


type Op struct {
  // Your definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  Name string
  Key string
  Value string
  DoHash bool
  OpID int64
}

type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  // Your definitions here.
  doneSeq int
  KVData map[string] string
  dupRPC map[int64] RPCResult
}

func enquePrintf(op Op, kv *KVPaxos) {
  if op.Name == "GET" {
    DPrintf("kv[%d] enque Op: Get(Key=%s) with OpID=%d\n",
            kv.me, op.Key, op.OpID)
  } else {
    DPrintf("kv[%d] enque Op: Put(Key=%s,Value=%s) with dohash=%t and OpID=%d\n",
            kv.me, op.Key, op.Value, op.DoHash, op.OpID)
  }
}

func dupPrintf(op Op, kv *KVPaxos) {
  if op.Name == "GET" {
    DPrintf("kv[%d]: Get(Key=%s) with OpID=%d request is a DUP\n",
            kv.me, op.Key, op.OpID)
  } else {
    DPrintf("kv[%d]: Put(Key=%s,Value=%s) with OpID=%d request is a DUP\n",
            kv.me, op.Key, op.Value, op.OpID)
  }
}

func processPrintf(op Op, kv *KVPaxos) {
  if op.Name == "GET" {
    DPrintf("kv[%d]: Processing Get(Key=%s) with OpID=%d request\n",
            kv.me, op.Key, op.OpID)
  } else {
    DPrintf("kv[%d]: Processing Put(Key=%s,Value=%s) with OpID=%d request\n",
            kv.me, op.Key, op.Value, op.OpID)
  }
}

func (kv *KVPaxos) Finish(args *FinishArgs, reply *FinishReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()
  
  opid := args.OpID
  delete(kv.dupRPC, opid)
  reply.Err = OK

  return nil
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  op := Op{Name: "GET", Key: args.Key, OpID: args.OpID}

  reply.Value = kv.StartAgreement(op)

  return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  op := Op{Name: "PUT", Key: args.Key, Value: args.Value, DoHash: args.DoHash, OpID: args.OpID}

  reply.PreviousValue = kv.StartAgreement(op)

  return nil
}

func (kv *KVPaxos) StartAgreement(op Op) string {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  enquePrintf(op, kv)
  
  if kv.dupRPC[op.OpID].IsExecuted {
    // Duplicate operations detected...
    dupPrintf(op, kv)
  } else {
    processPrintf(op, kv)
    opFinish := false
    for !opFinish {
      // check 
      seq := kv.doneSeq + 1
      to := 10 * time.Millisecond
      decided := false
      var currentop Op
      var t interface{}

      kv.px.Start(seq, op)
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
      oldValue, exist := kv.KVData[currentop.Key]
      if !exist {
        oldValue = ""
      }
      if currentop.Name == "PUT" {
        if currentop.DoHash {
          h := hash(oldValue + currentop.Value)
          val := strconv.Itoa(int(h))
          kv.KVData[currentop.Key] = val
        } else{
          kv.KVData[currentop.Key] = currentop.Value
        }
      }
      kv.dupRPC[currentop.OpID] = RPCResult{true, oldValue}

      kv.doneSeq = seq
      kv.px.Done(seq)

      opFinish = currentop.OpID == op.OpID
    }
  }

  return kv.dupRPC[op.OpID].LastValue
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
  DPrintf("Kill(%d): die\n", kv.me)
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *KVPaxos {
  // call gob.Register on structures you want
  // Go's RPC library to marshall/unmarshall.
  gob.Register(Op{})

  kv := new(KVPaxos)
  kv.me = me

  // Your initialization code here.
  kv.doneSeq = 0
  kv.KVData = make(map[string] string)
  kv.dupRPC = make(map[int64] RPCResult)

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
        fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  return kv
}

