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

type Request struct {
	WorkerId       int
	FinishedTaskId int
}

type Reply struct {
	MapDone   bool
	FilePath  string
	NumReduce int
	TaskId    int
}

// Add your RPC definitions here.
