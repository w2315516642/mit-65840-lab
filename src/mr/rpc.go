package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type taskType string

const (
	MapTask    taskType = "MapTask"
	ReduceTask taskType = "ReduceTask"
	Wait       taskType = "Wait"
	Exit       taskType = "Exit"
)

type Request struct {
	WorkerID       int
	FinishedTaskID int
}

type Reply struct {
	TaskType  taskType
	FilePath  string
	NumReduce int
	NumMap    int
	TaskID    int
}

// Add your RPC definitions here.
