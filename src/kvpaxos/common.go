package kvpaxos

import "hash/fnv"
import "crypto/rand"
import "math/big"
import "time"

const PingInterval = time.Millisecond * 100

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
)
type Err string

type PutArgs struct {
  // You'll have to add definitions here.
  Key string
  Value string
  DoHash bool  // For PutHash
  // You'll have to add definitions here.
  OpID int64
  // Field names must start with capital letters,
  // otherwise RPC will break.
}

type PutReply struct {
  Err Err
  PreviousValue string   // For PutHash
}

type GetArgs struct {
  Key string
  // You'll have to add definitions here.
  OpID int64
}

type GetReply struct {
  Err Err
  Value string
}

type FinishArgs struct {
  OpID int64
}

type FinishReply struct {
  Err Err
}

type RPCResult struct {
  IsExecuted bool
  LastValue string
}

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

func nrand() int64 {
  max := big.NewInt(int64(1) << 62)
  bigx, _ := rand.Int(rand.Reader, max)
  x := bigx.Int64()
  return x
 }
