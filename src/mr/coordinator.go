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

// a worker calls this to request a task, which can be a map task or a reduce task
func (c *Coordinator) TaskRequest(args *TaskRequestArgs, reply *TaskRequestReply) error {
	// lock shared memory until this function returns
	c.mu.Lock()
	defer c.mu.Unlock()

	// populate metadata fields
	reply.NReduce = c.nReduce
	reply.NMap = c.nMap

	// check if any map tasks are available/not yet started
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

	// check if any map tasks are in progress but are taking too long (more than 10s)
	// it doesn't matter if the old worker is still running, because upon completion, each worker writes out the correct output data to the same file
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

	// if any map tasks are still in progress but the condition above has not been met, then we cannot give out a task
	for _, v := range c.mapStatus {
		if v == MapInProgress {
			reply.TaskToDo = TaskUnavailable
			return nil
		}
	}

	// given that all the map tasks are completed, check if any reduce tasks are available/not yet started
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

	// check if any reduce tasks are in progress but are taking too long (more than 10s)
	// it doesn't matter if the old worker is still running, because upon completion, each worker writes out the correct output data to the same file
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

	// if we have reached this point, it means that all the map tasks are complete and each of the reduce tasks is either in progress (and started recently) or complete
	reply.TaskToDo = TaskUnavailable
	return nil
}

// a worker calls this to notify the coordinator that a given map task has been completed
func (c *Coordinator) MarkMapComplete(args *MarkMapCompleteArgs, reply *MarkMapCompleteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.mapStatus[args.MapTaskNumber] = MapCompleted
	return nil
}

// a worker calls this to notify the coordinator that a given reduce task has been completed
func (c *Coordinator) MarkReduceComplete(args *MarkReduceCompleteArgs, reply *MarkReduceCompleteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.reduceStatus[args.ReduceTaskNumber] = ReduceCompleted
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

	// ensure that all the map tasks have been completed
	for _, v := range c.mapStatus {
		if v != MapCompleted {
			return false
		}
	}

	// ensure that all the reduce tasks have been completed
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

	// note that the default values are 0 - "not yet started"
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
