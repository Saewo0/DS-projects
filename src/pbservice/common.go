package pbservice

import "hash/fnv"

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongServer = "ErrWrongServer"
  ErrDupRPC = "ErrDupRPC"
  ErrUninitBackup = "ErrUninitBackup"
)

type Err string

type PutArgs struct {
  Key string
  Value string
  DoHash bool // For PutHash
  // You'll have to add definitions here.
  OpID int64

  // Field names must start with capital letters,
  // otherwise RPC will break.
}

type PutReply struct {
  Err Err
  PreviousValue string // For PutHash
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

// Your RPC definitions here.

type TransferArgs struct {
  KVData map[string] string
}

type TransferReply struct {
  Init bool
}

type ForwardArgs struct {
  OpName string
  OpID int64
  Key string
  Value string
}

type ForwardReply struct {
  Err Err
  Value string
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

