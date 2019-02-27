
package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    n, err = fmt.Printf(format, a...)
  }
  return
}


type ViewServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  me string


  // Your declarations here.
  curview *View
  isACKed bool
  pingTime map[string] time.Time
}


func (vs *ViewServer) SearchIdleServer() string {
  for name, lastping := range vs.pingTime {
    if name == vs.curview.Primary || name == vs.curview.Backup {
      continue
    } else {
      if (time.Now().Sub(lastping) <= DeadPings * PingInterval) {
        return name
      }
    }
  }
  return ""
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
  // Your code here.
  vs.mu.Lock()
  servName := args.Me
  pingViewNo := args.Viewnum

  vs.pingTime[servName] = time.Now()

  if vs.curview == nil {
    DPrintf("ViewService(Master) just started\n")
    if pingViewNo == 0 {
      // The first Ping(0) is sent from server S_n, set as Primary
      vs.curview = &View{1, servName, ""}
      DPrintf("Ping(): ViewServer(Master) create the fisrt View(%d)\n", vs.curview.Viewnum)
      vs.isACKed = false
      DPrintf("First server %s becomes Primary!\n", servName)
    } else {
      return fmt.Errorf("Ping Error: RPC from %s with Viewnum %d before first View\n",
                        servName, pingViewNo)
    }
  } else {
    primaryName := vs.curview.Primary
    backupName := vs.curview.Backup
    viewNo := vs.curview.Viewnum
    DPrintf("ViewService(Master) is proceeding View(%d)\n", viewNo)
    if pingViewNo == viewNo {
      if servName == primaryName {
        // Primary send Ping(i) to ViewService(master), View(i) is ACKed
        vs.isACKed = true
        DPrintf("View(%d) is ACKed\n", viewNo)
        if backupName == "" {
          // There is no backup server, choose one if there is a idle server
          newbackup := vs.SearchIdleServer()
          if newbackup != "" {
            vs.curview = &View{viewNo+1, servName, newbackup}
            DPrintf("Ping() ViewServer(Master) updates View(%d) to View(%d)\n", viewNo, vs.curview.Viewnum)
            vs.isACKed = false
          }
        }
      } else if servName == backupName {
        // Backup send Ping(i) to ViewService(master), simply do nothing
      }
    } else {
      // pingViewNo != viewNo
      DPrintf("Received Ping(%d) @ View(%d)\n", pingViewNo, viewNo)
      if pingViewNo == 0 {
      // Some server volunteer to work or they crushed and restarted
        if servName == primaryName {
          // Current Primary crushed and restarted
          if backupName == "" {
            // There is no Backup, all data are lost
            return fmt.Errorf("P/B Service failed: All data is lost")
          } else {
            // There is a Backup working, promote it as Primary if View(i) is ACKed
            vs.curview = &View{viewNo+1, backupName, primaryName}
            DPrintf("Ping(): ViewServer(Master) updates to View(%d) with {Primary=%s, Backup=%s}\n",
                    vs.curview.Viewnum, primaryName, backupName)
            vs.isACKed = false
          }
        } else if servName == backupName {
          // Current Backup crushed and restarted
        }
      } else if pingViewNo + 1 == viewNo {
        // Ping(i-1) reaches @ View(i)
      } else {
        // Unexpected Ping(j) @ View(i) error
        return fmt.Errorf("Ping error: RPC from %s with Viewnum %d appears at View %d\n",
                          servName, pingViewNo, viewNo)
      }
    }
  }
  reply.View = *vs.curview
  if args.Me == vs.curview.Primary {
    DPrintf("Received Ping(%d) from Primary, reply View(%d) with {P: %s, B: %s}\n",
            pingViewNo, reply.View.Viewnum, reply.View.Primary, reply.View.Backup)
  } else if args.Me == vs.curview.Backup {
    DPrintf("Received Ping(%d) from Backup, reply View(%d) with {P: %s, B: %s}\n",
            pingViewNo, reply.View.Viewnum, reply.View.Primary, reply.View.Backup)
  }
  vs.mu.Unlock()
  return nil
}

// 
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

  // Your code here.
  vs.mu.Lock()
  if vs.curview != nil {
    reply.View = *vs.curview
  } else {
    reply.View = View{}
  }
  vs.mu.Unlock()

  return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
  // Your code here.
  vs.mu.Lock()
  if vs.curview != nil {
    primaryName := vs.curview.Primary
    backupName := vs.curview.Backup
    viewNo := vs.curview.Viewnum
    timeNow := time.Now()
    DPrintf("tick() in ViewServer: vs.curview(P:%s,B:%s)\n", primaryName, backupName)
    // Check if current assigned Primary & Backup servers are dead
    if backupName != "" {
      if timeNow.Sub(vs.pingTime[primaryName]) > DeadPings * PingInterval &&
      timeNow.Sub(vs.pingTime[backupName]) <= DeadPings * PingInterval {
        // Current Primary is cead, Backup is alive
        DPrintf("P(%s) is dead\n", primaryName)
        if vs.isACKed {
          newbackup := vs.SearchIdleServer()
          vs.curview = &View{viewNo+1, backupName, newbackup}
          DPrintf("tick(pdead):ViewServer(Master) updates View(%d) to View(%d)\n", viewNo, vs.curview.Viewnum)
          vs.isACKed = false
        }
      } else if timeNow.Sub(vs.pingTime[primaryName]) <= DeadPings * PingInterval &&
      timeNow.Sub(vs.pingTime[backupName]) > DeadPings * PingInterval {
        // Current Backup is dead, Primary is alive
        DPrintf("Backup %s is dead\n", backupName)
        if vs.isACKed {
          newbackup := vs.SearchIdleServer()
          DPrintf("Find new Backup named %s\n", newbackup)
          vs.curview = &View{viewNo+1, primaryName, newbackup}
          DPrintf("tick(bdead)ViewServer(Master) updates View(%d) to View(%d)\n", viewNo, vs.curview.Viewnum)
          vs.isACKed = false
        }
      } else if timeNow.Sub(vs.pingTime[primaryName]) > DeadPings * PingInterval &&
        timeNow.Sub(vs.pingTime[backupName]) > DeadPings * PingInterval {
        // Both Primary and Backup are dead
        DPrintf("Service crushed\n")
      } else {
        // Both Primary and Backup work well, do nothing
      }
    } else {
      // There's no Backup now
      if timeNow.Sub(vs.pingTime[primaryName]) > DeadPings * PingInterval {
        // Current Primary is cead, CRASHED!
        DPrintf("The only P(%s) is dead, Service crushed!\n", primaryName)
      }
    }
  } else {
  }
  vs.mu.Unlock()
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
  vs.dead = true
  vs.l.Close()
}

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)
  vs.me = me
  // Your vs.* initializations here.
  vs.curview = nil
  vs.isACKed = false
  vs.pingTime = make(map[string] time.Time)

  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.dead == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.dead == false {
        fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.dead == false {
      vs.tick()
      time.Sleep(PingInterval)
    }
  }()

  return vs
}
