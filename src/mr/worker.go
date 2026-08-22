package mr

import (
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

var (
	ErrGetTaskFailed    = errors.New("[worker]: Get task failed.")
	ErrReportTaskFailed = errors.New("[worker]: Report finished task failed.")
	SuccessAllTaskDone  = errors.New("[worker]: All tasks done.")
)

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(
	sockname string,
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	coordSockName = sockname

	// Your worker implementation here.

	for {
		err := RunOneLoop(mapf)
		if err == SuccessAllTaskDone {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func RunOneLoop(mapf func(string, string) []KeyValue) error {
	reply, err := GetTask()
	if err != nil {
		fmt.Println("error")
		return ErrGetTaskFailed
	}

	if reply.MapDone {
		return SuccessAllTaskDone
	}

	file := reply.FilePath
	taskID := reply.TaskId

	contents, _ := os.ReadFile(file)
	kva := mapf(file, string(contents))

	for i := 0; i < reply.NumReduce; i++ {
		oname := "mr-" + strconv.Itoa(taskID) + "-" + strconv.Itoa(i)
		os.Remove(oname)
	}

	for _, kv := range kva {
		out_id := ihash(kv.Key) % reply.NumReduce
		out_id_str := strconv.Itoa(out_id)
		oname := "mr-" + strconv.Itoa(taskID) + "-" + out_id_str
		ofile, oErr := os.OpenFile(oname,
			os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o666)
		if oErr != nil {
			fmt.Println("[worker]: Open file failed: ", oErr)
			return ErrGetTaskFailed
		}
		fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
		ofile.Close()
	}

	return ReportOneTask(taskID)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func GetTask() (Reply, error) {
	// declare an argument structure.
	args := Request{}
	args.WorkerId = 99

	// declare a reply structure.
	reply := Reply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.FilePath)
		return reply, nil
	} else {
		fmt.Printf("call failed!\n")
		return Reply{}, ErrGetTaskFailed
	}
}

func ReportOneTask(taskID int) error {
	req := Request{FinishedTaskId: taskID}
	reply := Reply{}

	ok := call("Coordinator.ReportOneTask", &req, &reply)
	if ok {
		fmt.Printf("reply.Y %v\n", reply.FilePath)
		return nil
	} else {
		fmt.Printf("call failed!\n")
		return ErrReportTaskFailed
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
