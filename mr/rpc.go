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

type EmptyArs struct {
	ToNext bool
}

type EmptyReply struct {
	Phase string
}

// Universal Task structure
type MapTask struct {
	Filename  string // Filename = key
	NumReduce int    // Number of reduce tasks, used to figure out number of buckets
	NumMap    int    // A number to identify which map task it is
	ToReduce  bool
}

type ReduceTask struct {
	NumReduce string   // Number of reduer, Y from Filename
	Filenames []string // intermediate filename (mr-X-Y, X: No.MapTask, Y: No.ReduceTask)
	Done      bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
