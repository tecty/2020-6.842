package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strconv"
)

type WorkType int

const (
	Map WorkType = iota
	Wait
	Reduce
	Exit
)

func GetReduceFileName(n int) string {
	return fmt.Sprintf("mr-out-%d", n)
}

type (
	// WokerArgs worker doesn't need to  supply any information to master
	WokerArgs struct{}
	// InitInfoReply replys nworks only
	InitInfoReply struct {
		NReduce int
	}
	//FetchJob  reply the current jobs information
	FetchJobReply struct {
		Type     WorkType
		FileName string
	}
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

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
