package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Master struct {
	// Your definitions here.
	nWorkers int
	nCommit  int
	nReduce  int
	nExit    int
	jobs     []string
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) InitWorker(_ *WokerArgs, reply *InitInfoReply) error {
	reply.NReduce = m.nReduce
	m.nWorkers++
	return nil
}

func (m *Master) FetchJob(_ *WokerArgs, reply *FetchJobReply) error {
	switch {
	case len(m.jobs) != 0:
		reply.Type = Map
		reply.FileName = m.jobs[len(m.jobs)-1]
		m.jobs = m.jobs[:len(m.jobs)-1]
	case m.nCommit != 0:
		reply.Type = Wait
	case m.nReduce != 0:
		m.nReduce--
		reply.FileName = GetReduceFileName(m.nReduce)
		reply.Type = Reduce
	default:
		reply.Type = Exit
		m.nExit++
	}
	fmt.Printf("master state %+v\n", m)

	return nil
}

func (m *Master) Commit(_ *WokerArgs, _ *WokerArgs) error {
	// commit a map to master
	m.nCommit--
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)

	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.nWorkers != 0 && m.nExit == m.nWorkers
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		jobs:    files,
		nCommit: len(files),
		nReduce: nReduce,
	}

	m.server()
	return &m
}
