package mapreduce
import "container/list"
import "fmt"

type WorkerInfo struct {
  address string
  // You can add definitions here.
  alive bool // true for healthy worker, false for failed worker
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

func (mr *MapReduce) RunMaster() *list.List {
  // Your code here
  jobChannel := make(chan *DoJobArgs)
  idleWorkerChannel := make(chan string, 20)
  finishChannel := make(chan bool)
  //resultChannel := make(chan string)

  // Add map jobs to the jobChannel
  go func() {
    for i := 0; i < mr.nMap; i++ {
      mapJob := &DoJobArgs {
                  File: mr.file,
                  Operation: Map,
                  JobNumber: i,
                  NumOtherPhase: mr.nReduce}
      //fmt.Printf("Adding job %d to the jobChannel.\n", i)
      jobChannel <- mapJob
    }
  }()

  // Find an idle worker from either newly-registered worker channel
  // or idle worker channel
  FindWorker := func() string {
    var wkaddress string
    select {
      case wkaddress = <- mr.registerChannel:
        mr.mux.Lock()
        mr.Workers[wkaddress] = &WorkerInfo{address:wkaddress, alive:true}
        mr.mux.Unlock()
      case wkaddress = <- idleWorkerChannel:
    }
    return wkaddress
  }

  // Do jobs in jobChannel concurrently
  go func() {
    for {
      jobArgs, ok:= <- jobChannel
      if ok {
        worker := FindWorker()
        // Send a job to an idle worker by a rpc call and check if some errorss happen
        go func() {
          var jobReply DoJobReply
          ok := call(worker, "Worker.DoJob", jobArgs, &jobReply)
          if ok == false {
            //fmt.Printf("sendJob(RunMaster): RPC %s do %s job %d error\n", worker, jobArgs.Operation, jobArgs.JobNumber)
            jobChannel <- jobArgs       // Add the failed job to jobChannel again
            mr.mux.Lock()
            mr.Workers[worker].alive = false
            mr.mux.Unlock()
          } else {
            //fmt.Printf("sendJob(RunMaster): RPC %s finished job %d.\n", worker, jobArgs.JobNumber)
            idleWorkerChannel <- worker // Add the idle worker to idle worker channel
            finishChannel <- true
            // If a map job is done, add all reduce jobs based on this map job to the jobChannel
            
          }
        }()
      }
    }
  }()

  // Wait for finishing all map jobs
  for i := 0; i < mr.nMap; i++ {
    <- finishChannel
    //fmt.Printf("Total %d maps have been done so far.\n", i)
  }

  //fmt.Printf("All map jobs are done. Let's start reduce!\n")

  // Add reduce jobs to the jobChannel
  go func() {
    for i := 0; i < mr.nReduce; i++ {
      reduceJob := &DoJobArgs {
                  File: mr.file,
                  Operation: Reduce,
                  JobNumber: i,
                  NumOtherPhase: mr.nMap}
      jobChannel <- reduceJob
    }
  }()

  // Wait for finishing all reduce jobs
  for i := 0; i < mr.nReduce; i++ {
    <- finishChannel
  }

  defer close(jobChannel)

  return mr.KillWorkers()
}
