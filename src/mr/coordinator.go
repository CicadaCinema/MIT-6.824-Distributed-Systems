package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type MapTaskStatus int64

const (
	MapNotStarted MapTaskStatus = 0
	MapInProgress MapTaskStatus = 1
	MapCompleted  MapTaskStatus = 2
)

type ReduceTaskStatus int64

const (
	ReduceNotStarted ReduceTaskStatus = 0
	ReduceInProgress ReduceTaskStatus = 1
	ReduceCompleted  ReduceTaskStatus = 2
)

type Coordinator struct {
	// READ-ONLY
	nReduce int
	files   []string
	// MODIFIABLE
	mu           sync.Mutex
	mapStatus    []MapTaskStatus
	reduceStatus []ReduceTaskStatus
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	fmt.Println("Coordinator started")

	// note that the default values are 0 - "not started"
	mStatus := make([]MapTaskStatus, len(files))
	rStatus := make([]ReduceTaskStatus, len(files))

	// note that we do not ned to initialise mutexes
	c := Coordinator{
		nReduce:      nReduce,
		files:        files,
		mapStatus:    mStatus,
		reduceStatus: rStatus,
	}

	c.server()
	return &c
}
