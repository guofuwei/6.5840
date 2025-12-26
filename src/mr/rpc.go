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

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type RequestTaskArgs struct {
	WorkerID int
}

type RequestTaskReply struct {
	TaskType string // "Map" or "Reduce" or "Wait" or "Exit"
	// For Map task
	MapTaskID int
	Filename  string
	NReduce   int
	// For Reduce task
	ReduceTaskID int
	NMap         int // 说明该 Reduce 任务需要处理多少个 Map 任务的输出
}

type ReportMapDoneArgs struct {
	WorkerID int
	TaskID   int
}

type ReportMapDoneReply struct {
	Success bool
}

type ReportReduceDoneArgs struct {
	WorkerID int
	TaskID   int
}

type ReportReduceDoneReply struct {
	Success bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
