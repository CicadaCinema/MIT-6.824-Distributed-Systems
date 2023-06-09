package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
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
	nMap    int
	files   []string
	// MODIFIABLE
	mu              sync.Mutex
	mapStatus       []MapTaskStatus
	mapStartTime    []time.Time
	reduceStatus    []ReduceTaskStatus
	reduceStartTime []time.Time
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) TaskRequest(args *TaskRequestArgs, reply *TaskRequestReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.NReduce = c.nReduce
	reply.NMap = c.nMap

	// see if any map tasks are available/not yet started
	for i, v := range c.mapStatus {
		if v == MapNotStarted {
			reply.TaskToDo = MapTask
			reply.MapFilename = c.files[i]
			reply.MapTaskNumber = i

			// now this map task is in progress
			c.mapStatus[i] = MapInProgress
			c.mapStartTime[i] = time.Now()
			return nil
		}
	}

	// see if any map tasks are in progress but are taking too long (more than 10s)
	for i, v := range c.mapStatus {
		if v == MapInProgress && time.Now().Sub(c.mapStartTime[i]).Seconds() > 10 {
			reply.TaskToDo = MapTask
			reply.MapFilename = c.files[i]
			reply.MapTaskNumber = i

			// now this map task is in progress
			c.mapStatus[i] = MapInProgress
			c.mapStartTime[i] = time.Now()
			return nil
		}
	}

	// if any map tasks are still in progress, then we cannot give out a task
	for _, v := range c.mapStatus {
		if v == MapInProgress {
			reply.TaskToDo = TaskUnavailable
			return nil
		}
	}

	// given that all the map tasks are completed,
	// see if any reduce tasks are available/not yet started
	for i, v := range c.reduceStatus {
		if v == ReduceNotStarted {
			reply.TaskToDo = ReduceTask
			reply.ReduceTaskNumber = i

			// now this reduce task is in progress
			c.reduceStatus[i] = ReduceInProgress
			c.reduceStartTime[i] = time.Now()
			return nil
		}
	}

	// see if any reduce tasks are in progress but are taking too long (more than 10s)
	for i, v := range c.reduceStatus {
		if v == ReduceInProgress && time.Now().Sub(c.reduceStartTime[i]).Seconds() > 10 {
			reply.TaskToDo = ReduceTask
			reply.ReduceTaskNumber = i

			// now this reduce task is in progress
			c.reduceStatus[i] = ReduceInProgress
			c.reduceStartTime[i] = time.Now()
			return nil
		}
	}

	// if we have reached this point, all the map tasks are complete and all the reduce tasks are one of: in progress, or complete
	reply.TaskToDo = TaskUnavailable
	return nil
}
func (c *Coordinator) MarkMapComplete(args *MarkMapCompleteArgs, reply *MarkMapCompleteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mapStatus[args.MapTaskNumber] = MapCompleted
	return nil
}
func (c *Coordinator) MarkReduceComplete(args *MarkReduceCompleteArgs, reply *MarkReduceCompleteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reduceStatus[args.ReduceTaskNumber] = ReduceCompleted
	return nil
}

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
	c.mu.Lock()
	defer c.mu.Unlock()

	// check that all the map tasks have been completed
	for _, v := range c.mapStatus {
		if v != MapCompleted {
			return false
		}
	}

	// check that all the reduce tasks have been completed
	for _, v := range c.reduceStatus {
		if v != ReduceCompleted {
			return false
		}
	}

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// fmt.Println("Coordinator started")

	nMap := len(files)

	// note that the default values are 0 - "not started"
	mStatus := make([]MapTaskStatus, nMap)
	rStatus := make([]ReduceTaskStatus, nReduce)

	mStartTime := make([]time.Time, nMap)
	rStartTime := make([]time.Time, nReduce)

	// note that we do not ned to initialise mutexes
	c := Coordinator{
		nReduce:         nReduce,
		nMap:            nMap,
		files:           files,
		mapStatus:       mStatus,
		mapStartTime:    mStartTime,
		reduceStatus:    rStatus,
		reduceStartTime: rStartTime,
	}

	c.server()
	return &c
}
