package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type TaskKind int64

const (
	TaskUnavailable TaskKind = 0
	MapTask         TaskKind = 1
	ReduceTask      TaskKind = 2
)

// request a task
type TaskRequestArgs struct{}
type TaskRequestReply struct {
	NReduce  int
	NMap     int
	TaskToDo TaskKind
	// if requested to perform a map task, perform it on the data in this file which has this task number
	MapFilename   string
	MapTaskNumber int
	// if requested to perform a reduce task, perform it on the data in intermediate files with this reduce task number
	ReduceTaskNumber int
}

// mark a map task with a given task number as complete
type MarkMapCompleteArgs struct {
	MapTaskNumber int
}
type MarkMapCompleteReply struct{}

// mark a reduce task with a given task number as complete
type MarkReduceCompleteArgs struct {
	ReduceTaskNumber int
}
type MarkReduceCompleteReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
