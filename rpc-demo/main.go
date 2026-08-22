package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

// =====================================================================
// 最小 RPC 演示：Coordinator 起一个 RPC 服务器，Worker 远程调用它
//
//   go run main.go coordinator     # 终端 1
//   go run main.go worker 1        # 终端 2
//
// 对照 Lab 1 的真实代码：
//   本文件的 AskTask     <-> src/mr/coordinator.go 里的 Coordinator 方法
//   本文件的 TaskArgs 等  <-> src/mr/rpc.go 里共享的参数/回复类型
//   本文件的 dial + Call <-> src/worker/worker.go 里连接 coordinator 的逻辑
// =====================================================================

// TaskArgs / TaskReply 是两进程间传递的数据结构。
// 真实 Lab 里它们放在 mr/rpc.go，coordinator 和 worker 都 import 同一份。
type TaskArgs struct {
	WorkerID int
}

type TaskReply struct {
	TaskID   int
	Filename string
}

// ---------- Coordinator 端 ----------

type Coordinator struct {
	nextTaskID int
}

// Worker 远程调用的方法：接收参数指针，往回复指针里写结果，返回 error。
// 必须满足这个签名（导出方法 + 两个指针参数 + error 返回）才会被 net/rpc 注册。
func (c *Coordinator) AskTask(args *TaskArgs, reply *TaskReply) error {
	c.nextTaskID++
	reply.TaskID = c.nextTaskID
	reply.Filename = fmt.Sprintf("file-%d.txt", c.nextTaskID)
	fmt.Printf("[coordinator] worker %d 请求任务 -> 分配 %s (task %d)\n",
		args.WorkerID, reply.Filename, reply.TaskID)
	return nil
}

func runCoordinator(sockname string) {
	c := &Coordinator{}
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname) // 删掉上次残留的 socket 文件，否则 listen 会报错
	l, err := net.Listen("unix", sockname)
	if err != nil {
		log.Fatalf("listen error: %v", err)
	}
	fmt.Printf("[coordinator] 正在监听 %s ...\n", sockname)
	go http.Serve(l, nil) // 异步跑，主 goroutine 才能继续干别的
	select {}             // 演示用：让进程活着；真实 Lab 里 main 会做其他事情
}

// ---------- Worker 端 ----------

// 连接 coordinator，连不上就重试（真实 worker 一般也会这样）
func dial(sockname string) *rpc.Client {
	for {
		conn, err := rpc.DialHTTP("unix", sockname)
		if err == nil {
			return conn
		}
		fmt.Printf("[worker] 连不上 coordinator: %v，1 秒后重试...\n", err)
		time.Sleep(time.Second)
	}
}

func runWorker(sockname string, id int) {
	conn := dial(sockname)
	defer conn.Close()

	args := &TaskArgs{WorkerID: id}
	reply := &TaskReply{}
	// 远程调用 Coordinator.AskTask，reply 会被填上结果
	if err := conn.Call("Coordinator.AskTask", args, reply); err != nil {
		log.Fatalf("call error: %v", err)
	}
	fmt.Printf("[worker %d] 拿到任务: task %d, 文件 %s\n",
		id, reply.TaskID, reply.Filename)
}

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("用法: go run main.go coordinator|worker [workerID]")
	}
	sockname := "/tmp/rpc-demo-socket"

	switch os.Args[1] {
	case "coordinator":
		runCoordinator(sockname)
	case "worker":
		id := 1
		if len(os.Args) > 2 {
			fmt.Sscanf(os.Args[2], "%d", &id)
		}
		runWorker(sockname, id)
	default:
		log.Fatalf("未知模式: %q", os.Args[1])
	}
}
