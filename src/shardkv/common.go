package shardkv

import "hash/fnv"
import (
  "math/big"
  "crypto/rand"
)

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongGroup = "ErrWrongGroup"
  ErrNotReady = "ErrNotReady"
)
type Err string

type PutArgs struct {
  Key string
  Value string
  DoHash bool  // For PutHash
  // You'll have to add definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  OpID string

}

type PutReply struct {
  Err Err
  PreviousValue string   // For PutHash
}

type GetArgs struct {
  Key string
  // You'll have to add definitions here.
  OpID string
}

type GetReply struct {
  Err Err
  Value string
}

type GetShardArgs struct {
  Shard int
  ConfigNum int //provider has to under at least this configuration
  OpID string
}

type GetShardReply struct {
  Err Err
  KVData map[string] string
  DupRPC map[string] RPCResult // record for dup PUT/GET request
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
  max := big.NewInt(int64(int64(1) << 62))
  bigx, _ := rand.Int(rand.Reader, max)
  x := bigx.Int64()
  return x
}
