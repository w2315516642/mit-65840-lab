package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

type TaskState string

const (
	Idle       TaskState = "Idle"
	InProgress TaskState = "InProgress"
	Done       TaskState = "Done"
)

type TaskStatus struct {
	state   TaskState
	task_id int
}

type Coordinator struct {
	// Your definitions here.
	nReduce int
	files   []string
	status  map[string]TaskStatus
	nRemain int

	mu sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) GetTask(req *Request, rep *Reply) error {
	ok, file := c.GetOneIdleTask()
	rep.MapDone = false
	rep.FilePath = file
	rep.NumReduce = c.nReduce

	c.mu.RLock()
	defer c.mu.RUnlock()
	rep.TaskId = c.status[file].task_id

	if ok {
		if file == "" {
			rep.MapDone = true
			fmt.Println("[Coordinator]: All task done.")
			return nil
		}
		fmt.Println("[Coordinator]: Get task success! file: %s, worker: %d", file, req.WorkerId)
		return nil
	} else {
		fmt.Println("[Coordinator]: Get task failed. worker: %d", req.WorkerId)
		return errors.New("get task failed")
	}
}

func (c *Coordinator) ReportOneTask(req *Request, rep *Reply) error {
	task_id := req.FinishedTaskId

	c.mu.Lock()
	defer c.mu.Unlock()

	new_status := TaskStatus{
		state:   Done,
		task_id: task_id,
	}
	c.status[c.files[task_id]] = new_status
	if c.nRemain > 0 {
		c.nRemain -= 1
	}

	return nil
}

func (c *Coordinator) GetOneIdleTask() (bool, string) {
	ok := false

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.nRemain == 0 {
		return true, ""
	}

	for file, status := range c.status {
		if status.state == Idle {
			ok = true
			new_status := TaskStatus{
				state:   InProgress,
				task_id: c.status[file].task_id,
			}
			c.status[file] = new_status
			return ok, file
		}
	}
	return ok, ""
}

func (c *Coordinator) init() error {
	for task_id, file := range c.files {
		state := TaskStatus{
			state:   Idle,
			task_id: task_id,
		}
		c.status[file] = state
	}
	c.nRemain = len(c.files)

	nTask := c.nRemain
	for i := 0; i < nTask; i++ {
		for j := 0; j < c.nReduce; j++ {
			fname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(j)
			os.Create(fname)
		}
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := true

	// Your code here.
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, status := range c.status {
		if status.state != Done {
			ret = false
			break
		}
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:   files,
		nReduce: nReduce,
		status:  make(map[string]TaskStatus),
	}

	// Your code here.
	c.init()

	c.server(sockname)
	return &c
}
