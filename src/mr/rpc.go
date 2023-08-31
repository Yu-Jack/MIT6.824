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

const (
	Task_Map = iota + 1
	Task_Reduce
)

type HealthReply struct {
	Health bool
}

type ACKTask struct {
	ID       string
	TaskType int
}

type Task struct {
	ID        string
	TaskType  int
	FileNames []string
	NReduce   int
	Bucket    int
}

type TaskReply Task
type Empty struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
