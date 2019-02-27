package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"
import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    n, err = fmt.Printf(format, a...)
  }
  return
}

type PBServer struct {
  l net.Listener
  dead bool // for testing
  unreliable bool // for testing
  me string
  vs *viewservice.Clerk
  done sync.WaitGroup
  finish chan interface{}

  // Your declarations here.
  mu sync.Mutex
  view *viewservice.View
  isInited bool
  KVData map[string] string
  dupRPC map[int64] RPCResult
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  pb.mu.Lock()
  defer pb.mu.Unlock()

  DPrintf("RPC: Put(Key=%s,Value=%s) with dohash=%t and OpID=%d request to server(%s)\n",
          args.Key, args.Value, args.DoHash, args.OpID, pb.me)
  if pb.dupRPC[args.OpID].IsExecuted {
    DPrintf("Put(Key=%s,Value=%s) with OpID=%d request to server(%s) is a DUP\n",
            args.Key, args.Value, args.OpID, pb.me)
    reply.Err = ErrDupRPC
    reply.PreviousValue = pb.dupRPC[args.OpID].LastValue
  } else {
    DPrintf("Processing Put() with OpID=%d request\n", args.OpID)
    if pb.view == nil || pb.me != pb.view.Primary {
      // I'm not the Primary
      reply.Err = ErrWrongServer
      DPrintf("Put Error: RPC 'Put' to the non-Primary(%s)\n", pb.me)
    } else {
      // Forward the request to Backup if Backup exists and Op is not duplicated
      oldValue, _ := pb.KVData[args.Key]
      if pb.view.Backup != "" {
        DPrintf("Forward Put to backup %s\n", pb.view.Backup)
        var FArgs *ForwardArgs
        var FReply ForwardReply
        if args.DoHash {
          FArgs = &ForwardArgs{"Append", nrand(), args.Key, args.Value}
        } else {
          FArgs = &ForwardArgs{"Put", nrand(), args.Key, args.Value}
        }
        
        ok := false
        DPrintf("Start forwarding Put(Key=%s,Value=%s) to server(%s)\n", args.Key, args.Value, pb.view.Backup)
        for !ok || FReply.Err == ErrUninitBackup {
          ok = call(pb.view.Backup, "PBServer.ForwardRequest", FArgs, &FReply)
          time.Sleep(viewservice.PingInterval)
        }
        DPrintf("Successfully forwarded Put(Key=%s,Value=%s) to server(%s)\n", args.Key, args.Value, pb.view.Backup)
        if FReply.Err == ErrWrongServer {
          // the View in Client is outdated, should reGet() from viewserver
          reply.Err = ErrWrongServer
        } else if (FReply.Err == ErrDupRPC || FReply.Err == "") && FReply.Value == oldValue {
          if args.DoHash {
            h := hash(oldValue + args.Value)
            val := strconv.Itoa(int(h))
            pb.KVData[args.Key] = val
          } else {
            pb.KVData[args.Key] = args.Value
          }
          reply.PreviousValue = oldValue
          pb.dupRPC[args.OpID] = RPCResult{true, oldValue}
          DPrintf("Successfully Put(Key=%s,Value=%s) into server(%s)\n", args.Key, args.Value, pb.me)
        } else {
          return fmt.Errorf("Error: Unknown Forward Error or P/B databases donot match!\n")
        }
      } else {
        // No Backup, just store the K/V
        if args.DoHash {
          h := hash(oldValue + args.Value)
          val := strconv.Itoa(int(h))
          pb.KVData[args.Key] = val
        } else {
          pb.KVData[args.Key] = args.Value
        }
        reply.PreviousValue = oldValue
        pb.dupRPC[args.OpID] = RPCResult{true, oldValue}
      }
    }
  }
  return nil
}

func (pb *PBServer) ForwardRequest(args *ForwardArgs, reply *ForwardReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()

  DPrintf("Backup(%s)(isInited:%t) is processing forwarded %s request\n", pb.me, pb.isInited, args.OpName)
  if pb.dupRPC[args.OpID].IsExecuted {
    // RPC is duplicated, just return the result from last execution
    reply.Err = ErrDupRPC
    reply.Value = pb.dupRPC[args.OpID].LastValue
  } else {
    // RPC is new
    if pb.view == nil || pb.me != pb.view.Backup {
      // reject the frowarded request
      reply.Err = ErrWrongServer
    } else {
      // I'm the current Backup
      if pb.isInited {
        // Backup has been initialized
        oldValue, ok := pb.KVData[args.Key]
        if args.OpName == "Put" {
          pb.KVData[args.Key] = args.Value
        } else if args.OpName == "Append" {
          h := hash(oldValue + args.Value)
          val := strconv.Itoa(int(h))
          pb.KVData[args.Key] = val
        } else if args.OpName == "Get" {
          if !ok {
            reply.Err = ErrNoKey
          } else {
          }
        } else {
          return fmt.Errorf("Unknown operation is forwarded!\n")
        }
        reply.Value = oldValue
        pb.dupRPC[args.OpID] = RPCResult{true, oldValue}
      } else {
        // Backup has not been initialized
        DPrintf("ErrUninitBackup!\n")
        reply.Err = ErrUninitBackup
      }
    }  
  }
  return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  pb.mu.Lock()
  defer pb.mu.Unlock()

  DPrintf("RPC : Get(%s) with OpID=%d request to server(%s)\n", args.Key, args.OpID, pb.me)
  if pb.dupRPC[args.OpID].IsExecuted {
    DPrintf("Get(%s) with OpID=%d request to server(%s) is a DUP\n",
            args.Key, args.OpID, pb.me)
    reply.Err = ErrDupRPC
    reply.Value = pb.dupRPC[args.OpID].LastValue
  } else {
    if pb.view == nil || pb.me != pb.view.Primary {
      // I'm not the Primary
      reply.Err = ErrWrongServer
      DPrintf("Get Error: RPC 'GET' to the non-Primary(%s)\n", pb.me)
    } else {
      // Forward the request to Backup if Backup exists and Op is not duplicated
      primaryvalue, primaryhasKey := pb.KVData[args.Key]
      if pb.view.Backup != "" {
        // STILL HAVE PROBLEMS IF BACKUP IS EMPTY!!!!!!!!!!!!!!!!!!!!!!!!!!
        DPrintf("Forward Get to backup(%s)\n", pb.view.Backup)
        FArgs := &ForwardArgs{"Get", nrand(), args.Key, ""}
        var FReply ForwardReply

        ok := false
        for !ok || FReply.Err == ErrUninitBackup {
          ok = call(pb.view.Backup, "PBServer.ForwardRequest", FArgs, &FReply)
          time.Sleep(viewservice.PingInterval)
        }
        // Backup successfully send back reply
        if FReply.Err == ErrWrongServer {
          // the View in Client is outdated, should reGet() from viewserver
          reply.Err = ErrWrongServer
        } else if FReply.Err == ErrNoKey && !primaryhasKey {
          reply.Err = ErrNoKey
          reply.Value = ""
          pb.dupRPC[args.OpID] = RPCResult{true, ""}
        } else if (FReply.Err == ErrDupRPC || FReply.Err == "") && FReply.Value == primaryvalue {
          reply.Value = primaryvalue
          pb.dupRPC[args.OpID] = RPCResult{true, primaryvalue}
        } else {
          return fmt.Errorf("Error: Unknown Forward Error or P/B databases donot match!\n")
        }
      } else {
        // No Backup, just return
        if !primaryhasKey {
          reply.Err = ErrNoKey
        } else {
        }
        reply.Value = primaryvalue
        pb.dupRPC[args.OpID] = RPCResult{true, primaryvalue}
      }
    }
  }
  return nil
}


func (pb *PBServer) TransferData(args *TransferArgs, reply *TransferReply) error {
  // Copy the complete kv database to the Backup
  pb.mu.Lock()
  defer pb.mu.Unlock()
  pb.KVData = args.KVData
  pb.isInited = true
  reply.Init = true
  DPrintf("Backup(%s) is initialized\n", pb.me)

  return nil
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
  // Your code here.
  viewNum := uint(0)
  if pb.view != nil {
    viewNum = pb.view.Viewnum
  }
  //DPrintf("tick(): Ping(%d) from pbserver(%s)\n", viewNum, pb.me)
  newView, e := pb.vs.Ping(viewNum)

  if e == nil {
    if newView.Viewnum != viewNum {
      // View changed
      if pb.me == newView.Primary && newView.Backup != "" {
        // set to Primary && there's a backup working, transfer the database to it
        Targs := &TransferArgs{pb.KVData}
        var Treply TransferReply
        // Keep trying if the transfer fails
        ok := false
        for !ok || !Treply.Init {
          DPrintf("Primary(%s) start to transfer data to Backup(%s)\n", pb.me, pb.view.Backup)
          ok = call(newView.Backup, "PBServer.TransferData", Targs, &Treply)
        }
      } else if pb.me == newView.Backup {
        // set to Backup, do nothing
      } else {
        // Set to idle server, do nothing
        pb.isInited = false
      }
      pb.view = &newView
      DPrintf("tick(): pbserver(%s) updates to newView(P: %s; B: %s)\n",
              pb.me, pb.view.Primary, pb.view.Backup)
      }
  } else {
    DPrintf("Ping (%d) reply error!\n", viewNum)
  }
}


// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
  pb.dead = true
  pb.l.Close()
}


func StartServer(vshost string, me string) *PBServer {
  pb := new(PBServer)
  pb.me = me
  pb.vs = viewservice.MakeClerk(me, vshost)
  pb.finish = make(chan interface{})
  // Your pb.* initializations here.
  pb.view = nil
  pb.KVData = make(map[string] string)
  pb.dupRPC = make(map[int64] RPCResult)

  rpcs := rpc.NewServer()
  rpcs.Register(pb)

  os.Remove(pb.me)
  l, e := net.Listen("unix", pb.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  pb.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for pb.dead == false {
      conn, err := pb.l.Accept()
      if err == nil && pb.dead == false {
        if pb.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        } else {
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && pb.dead == false {
        fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
        pb.kill()
      }
    }
    DPrintf("%s: wait until all request are done\n", pb.me)
    pb.done.Wait() 
    // If you have an additional thread in your solution, you could
    // have it read to the finish channel to hear when to terminate.
    close(pb.finish)
  }()

  pb.done.Add(1)
  go func() {
    for pb.dead == false {
      pb.tick()
      time.Sleep(viewservice.PingInterval)
    }
    pb.done.Done()
  }()

  return pb
}
