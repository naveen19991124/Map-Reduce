package mr

// RPC definitions.

import (
	"os"
	"strconv"
)

type TaskType int

const (
	Map TaskType = iota + 1
	Reduce
)

func (taskType *TaskType) String() string {
	return [...]string{"Map", "Reduce"}[*taskType-1]
}

type GetTaskArgs struct{}

type GetTaskReply struct {
	Task *Task
}

type ReportTaskCompletionArgs struct {
	PrevState TaskState
	NewState  TaskState
	TaskId    int
}

type ReportTaskCompletionReply struct {
	TaskId int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/Users/naveen.kumar3/Downloads/Map-Reduce-MIT-Lab-1/6.5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
