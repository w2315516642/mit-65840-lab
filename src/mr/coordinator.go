package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type TaskState string

const (
	Idle       TaskState = "Idle"
	InProgress TaskState = "InProgress"
	Done       TaskState = "Done"
)

type TaskStatus struct {
	state  TaskState
	taskID int
}

type Coordinator struct {
	// Your definitions here.
	nReduce      int
	files        []string
	MapStatus    map[string]TaskStatus
	ReduceStatus map[string]TaskStatus
	mapRemain    int
	reduceRemain int

	mu sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) GetTask(req *Request, rep *Reply) error {
	ret := c.GetOneIdleTask()

	if ret.TaskType == MapTask || ret.TaskType == ReduceTask {
		go func() {
			c.Retry(ret.FilePath, ret.TaskType)
		}()
	}

	*rep = ret

	return nil
}

func (c *Coordinator) Retry(file string, tType taskType) {
	time.Sleep(12 * time.Second)

	c.mu.Lock()
	defer c.mu.Unlock()

	if tType == MapTask {
		status := c.MapStatus[file]
		if status.state == InProgress {
			status.state = Idle
			c.MapStatus[file] = status
		}
	} else if tType == ReduceTask {
		status := c.ReduceStatus[file]
		if status.state == InProgress {
			status.state = Idle
			c.ReduceStatus[file] = status
		}
	}
}

func (c *Coordinator) ReportReduceTask(req *Request, rep *Reply) error {
	taskID := req.FinishedTaskID

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.ReduceStatus[strconv.Itoa(taskID)].state == Done {
		return nil
	}

	newStatus := TaskStatus{
		state:  Done,
		taskID: taskID,
	}
	c.ReduceStatus[strconv.Itoa(taskID)] = newStatus
	if c.reduceRemain > 0 {
		c.reduceRemain -= 1
	}

	return nil
}

func (c *Coordinator) ReportMapTask(req *Request, rep *Reply) error {
	taskID := req.FinishedTaskID

	c.mu.Lock()
	defer c.mu.Unlock()

	fileName := c.files[taskID]
	if c.MapStatus[fileName].state == Done {
		return nil
	}

	newStatus := TaskStatus{
		state:  Done,
		taskID: taskID,
	}
	c.MapStatus[c.files[taskID]] = newStatus
	if c.mapRemain > 0 {
		c.mapRemain -= 1
	}

	return nil
}

func (c *Coordinator) GetOneIdleTask() Reply {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply := Reply{
		FilePath:  "",
		NumReduce: c.nReduce,
		NumMap:    len(c.files),
		TaskType:  Wait,
	}

	if c.reduceRemain == 0 {
		reply.TaskType = Exit

		return reply
	}

	if c.mapRemain == 0 {
		// 发放 reduce 任务
		for id, status := range c.ReduceStatus {
			if status.state == Idle {
				newStatus := TaskStatus{
					state:  InProgress,
					taskID: c.ReduceStatus[id].taskID,
				}
				c.ReduceStatus[id] = newStatus

				reply.FilePath = id
				reply.TaskID = newStatus.taskID
				reply.TaskType = ReduceTask

				return reply
			}
		}
		return reply
	}

	for file, status := range c.MapStatus {
		if status.state == Idle {
			newStatus := TaskStatus{
				state:  InProgress,
				taskID: c.MapStatus[file].taskID,
			}
			c.MapStatus[file] = newStatus

			reply.FilePath = file
			reply.TaskID = newStatus.taskID
			reply.TaskType = MapTask

			return reply
		}
	}
	return reply
}

func (c *Coordinator) init() {
	for taskID, file := range c.files {
		state := TaskStatus{
			state:  Idle,
			taskID: taskID,
		}
		c.MapStatus[file] = state
	}
	c.mapRemain = len(c.files)
	c.reduceRemain = c.nReduce

	for i := 0; i < c.nReduce; i++ {
		state := TaskStatus{
			state:  Idle,
			taskID: i,
		}
		c.ReduceStatus[strconv.Itoa(i)] = state
	}
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
	// Your code here.
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.reduceRemain == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:        files,
		nReduce:      nReduce,
		MapStatus:    make(map[string]TaskStatus),
		ReduceStatus: make(map[string]TaskStatus),
	}

	// Your code here.
	c.init()

	c.server(sockname)
	return &c
}
