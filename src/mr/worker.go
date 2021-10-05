package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// uncomment to send the Example RPC to the master.
	nReduce := initWoker()
	fhArr := []*os.File{}
	for i := 0; i < nReduce; i++ {
		fh, err := os.OpenFile(GetReduceFileName(i), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			log.Fatalf("could not open the reduce output %s", err.Error())
		}
		fhArr = append(fhArr, fh)
	}

	// try to cloase all the reduce writer
	CloseAllReduceHandler := func() {
		if len(fhArr) == 0 {
			return
		}

		for _, v := range fhArr {
			v.Sync()
			v.Close()
		}
		fhArr = []*os.File{}
	}

	WriteKeyValue := func(kv KeyValue) {
		_, err := fmt.Fprintf(fhArr[ihash(kv.Key)%nReduce], "%v %v\n", kv.Key, kv.Value)
		if err != nil {
			panic(err)
		}
	}

	for job := fetchJob(); job.Type != Exit; job = fetchJob() {
		switch job.Type {
		case Wait:
			CloseAllReduceHandler()
			time.Sleep(1 * time.Second)
		case Reduce:
			CloseAllReduceHandler()
			kvArr := []KeyValue{}
			{
				file, err := os.Open(job.FileName)
				if err != nil {
					log.Fatalf("could not open the reduce output %s", err.Error())
				}
				defer file.Close()
				for {
					var kv KeyValue
					n, err := fmt.Fscanf(file, "%v %v\n", &kv.Key, &kv.Value)
					if n != 2 || err != nil {
						break
					}
					kvArr = append(kvArr, kv)
				}
			}
			ofile, err := os.OpenFile(job.FileName, os.O_TRUNC|os.O_WRONLY, 0644)
			if err != nil {
				log.Fatalf("couldn't open the reduce file %v", err)
			}
			defer ofile.Close()
			sort.Sort(ByKey(kvArr))
			i := 0
			for i < len(kvArr) {
				j := i + 1
				for j < len(kvArr) && kvArr[j].Key == kvArr[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kvArr[k].Value)
				}
				output := reducef(kvArr[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kvArr[i].Key, output)

				i = j
			}

		case Map:
			var content []byte
			{
				file, err := os.Open(job.FileName)
				if err != nil {
					log.Fatalf("cloudn't open given file %s %v", job.FileName, err.Error())
				}
				defer file.Close()
				content, err = ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("read all failed %v", err)
				}
			}
			kva := mapf(job.FileName, string(content))
			for _, v := range kva {
				WriteKeyValue(v)
			}
			commit()
		}
	}
}

func initWoker() int {
	args := WokerArgs{}
	reply := InitInfoReply{}
	call("Master.InitWorker", &args, &reply)
	fmt.Printf("nworker %v\n", reply)
	if reply.NReduce == 0 {
		return 1
	}
	return reply.NReduce
}

func fetchJob() FetchJobReply {
	args := WokerArgs{}
	reply := FetchJobReply{}
	call("Master.FetchJob", &args, &reply)
	fmt.Printf("job %v\n", reply)
	return reply
}

func commit() {
	args := WokerArgs{}
	reply := WokerArgs{}
	call("Master.Commit", &args, &reply)
	fmt.Printf("job %v\n", reply)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
