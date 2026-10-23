package mr

import (
	"bufio"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// KeyValue : Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
		reply, err := GetTask()
		if err != nil {
			fmt.Println("get task failed, err: ", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if reply.TaskType == Exit {
			return
		}

		if reply.TaskType == MapTask {
			err = RunOnceMapLoop(reply, mapf)
		} else if reply.TaskType == ReduceTask {
			err = RunOnceReduceLoop(reply, reducef)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func RunOnceMapLoop(reply Reply, mapf func(string, string) []KeyValue) error {
	file := reply.FilePath
	taskID := reply.TaskID

	contents, _ := os.ReadFile(file)
	kva := mapf(file, string(contents))

	for i := 0; i < reply.NumReduce; i++ {
		oname := "mr-" + strconv.Itoa(taskID) + "-" + strconv.Itoa(i)
		os.Create(oname)
	}

	for _, kv := range kva {
		outID := ihash(kv.Key) % reply.NumReduce
		outIDStr := strconv.Itoa(outID)
		oname := "mr-" + strconv.Itoa(taskID) + "-" + outIDStr
		ofile, oErr := os.OpenFile(oname,
			os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o666)
		if oErr != nil {
			fmt.Println("[worker]: Open file failed: ", oErr)
			return ErrGetTaskFailed
		}
		fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
		ofile.Close()
	}

	return ReportMapTask(taskID)
}

func RunOnceReduceLoop(reply Reply, reducef func(string, []string) string) error {
	reduceID := reply.TaskID

	var key string
	var value string

	intermediate := []KeyValue{}

	for i := 0; i < reply.NumMap; i++ {
		fileName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reduceID)
		file, err := os.OpenFile(fileName, os.O_RDONLY, 0o666)
		if err != nil {
			return ErrGetTaskFailed
		}

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			_, err := fmt.Sscanf(line, "%s %s", &key, &value)
			if err != nil {
				return ErrGetTaskFailed
			}

			intermediate = append(intermediate, KeyValue{key, value})
		}

		if err := scanner.Err(); err != nil {
			// 这里说明不是正常 EOF，而是读文件出错
			return ErrGetTaskFailed
		}
	}

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(reply.TaskID)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	return ReportReduceTask(reply.TaskID)
}

// example function to show how to make an RPC call to the coordinator.
// the RPC argument and reply types are defined in rpc.go.
func GetTask() (Reply, error) {
	// declare an argument structure.
	args := Request{}
	args.WorkerID = 99

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

func ReportReduceTask(taskID int) error {
	req := Request{FinishedTaskID: taskID}
	reply := Reply{}

	ok := call("Coordinator.ReportReduceTask", &req, &reply)
	if ok {
		fmt.Printf("reply.Y %v\n", reply.FilePath)
		return nil
	} else {
		fmt.Printf("call failed!\n")
		return ErrReportTaskFailed
	}
}

func ReportMapTask(taskID int) error {
	req := Request{FinishedTaskID: taskID}
	reply := Reply{}

	ok := call("Coordinator.ReportMapTask", &req, &reply)
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
