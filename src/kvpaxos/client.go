package kvpaxos

import "net/rpc"
import "fmt"
import "time"

type Clerk struct {
  servers []string
  // You will have to modify this struct.
}


func MakeClerk(servers []string) *Clerk {
  ck := new(Clerk)
  ck.servers = servers
  // You'll have to add code here.
  return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
          args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial("unix", srv)
  if errx != nil {
    return false
  }
  defer c.Close()
    
  err := c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

func (ck *Clerk) Finish(OpID int64) {
  FArgs := FinishArgs{OpID}
  var FReply FinishReply

  i := 0
  nserver := len(ck.servers)
  for i < nserver {
    call(ck.servers[i], "KVPaxos.Finish", FArgs, &FReply)
    i += 1
  }
}


//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
  // You will have to modify this function.
  GArgs := GetArgs{key, nrand()}
  var GReply GetReply
  ok := false
  servId := 0
  nserver := len(ck.servers)
  for !ok {
    ok = call(ck.servers[servId], "KVPaxos.Get", GArgs, &GReply)
    if ok {
      DPrintf("ck send Get(key:%s) with OpID=%d to serv(%d) succ\n", key, GArgs.OpID, servId)
      ck.Finish(GArgs.OpID)
    } else {
      DPrintf("ck send Put(key:%s) with OpID=%d to serv(%d) fail\n", key, GArgs.OpID, servId)
      servId = (servId + 1) % nserver
      time.Sleep(PingInterval)
    }
  }
  return GReply.Value
}

//
// set the value for a key.
// keeps trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
  // You will have to modify this function.
  PArgs := PutArgs{key, value, dohash, nrand()}
  var PReply PutReply
  ok := false
  servId := 0
  nserver := len(ck.servers)
  for !ok {
    ok = call(ck.servers[servId], "KVPaxos.Put", PArgs, &PReply)
    if ok {
      DPrintf("ck send Put(key:%s, Value:%s) with dohash=%t and OpID=%d to serv(%d) succ\n",
              key, value, dohash, PArgs.OpID, servId)
      ck.Finish(PArgs.OpID)
    } else {
      DPrintf("ck send Put(key:%s, Value:%s) with dohash=%t and OpID=%d to serv(%d) fail\n",
              key, value, dohash, PArgs.OpID, servId)
      servId = (servId + 1) % nserver
      time.Sleep(PingInterval)
    }
  }
  return PReply.PreviousValue
}

func (ck *Clerk) Put(key string, value string) {
  ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
  v := ck.PutExt(key, value, true)
  return v
}
