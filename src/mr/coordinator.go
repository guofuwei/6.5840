package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	nMap      int
	nReduce   int
	filenames []string

	// Map 任务状态
	mapTasks []TaskInfo
	nMapDone int

	// Reduce 任务状态
	reduceTasks []TaskInfo
	nReduceDone int

	phase Phase // MapPhase, ReducePhase, AllDone
	mu    sync.Mutex
}

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	AllDone
)

type TaskInfo struct {
	status    TaskStatus
	workerID  int
	startTime time.Time
}

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Map 阶段
	if c.phase == MapPhase {
		for i := range c.mapTasks {
			if c.mapTasks[i].status == Idle {
				c.mapTasks[i].status = InProgress
				c.mapTasks[i].workerID = args.WorkerID
				c.mapTasks[i].startTime = time.Now()

				reply.TaskType = "Map"
				reply.MapTaskID = i
				reply.Filename = c.filenames[i]
				reply.NReduce = c.nReduce

				return nil
			}
		}
		// 所有 Map 任务都在进行中，让 worker 等待
		reply.TaskType = "Wait"
		return nil
	}

	// Reduce 阶段
	if c.phase == ReducePhase {
		for i := range c.reduceTasks {
			if c.reduceTasks[i].status == Idle {
				c.reduceTasks[i].status = InProgress
				c.reduceTasks[i].workerID = args.WorkerID
				c.reduceTasks[i].startTime = time.Now()

				reply.TaskType = "Reduce"
				reply.ReduceTaskID = i
				reply.NMap = c.nMap // 对于该 Reduce（0--10） 有多少个 Map 任务
				return nil
			}
		}
		reply.TaskType = "Wait"
		return nil
	}

	// 所有任务完成
	reply.TaskType = "Exit"
	return nil
}

func (c *Coordinator) ReportMapDone(args *ReportMapDoneArgs, reply *ReportMapDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	taskID := args.TaskID
	// 如果有两个 worker 同时报告同一个任务完成，防止重复计数
	if c.mapTasks[taskID].status != Completed {
		c.mapTasks[taskID].status = Completed
		c.nMapDone++

		// 检查是否所有 Map 任务完成
		if c.nMapDone == c.nMap {
			c.phase = ReducePhase
			// log.Println("All Map tasks completed, entering Reduce phase")
		}
	}

	reply.Success = true
	return nil
}

func (c *Coordinator) ReportReduceDone(args *ReportReduceDoneArgs, reply *ReportReduceDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	taskID := args.TaskID
	// 如果有两个 worker 同时报告同一个任务完成，防止重复计数
	if c.reduceTasks[taskID].status != Completed {
		c.reduceTasks[taskID].status = Completed
		c.nReduceDone++

		// 检查是否所有 Reduce 任务完成

		if c.nReduceDone == c.nReduce {
			c.phase = AllDone
			// log.Println("All Reduce tasks completed, entering AllDone phase")
		}
	}

	reply.Success = true
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) checkTimeouts() {
	timeout := 5 * time.Second // 5秒超时

	for c.phase != AllDone {
		time.Sleep(time.Second)

		c.mu.Lock()
		now := time.Now()

		if c.phase == MapPhase {
			// 检查 Map 任务
			for i := range c.mapTasks {
				if c.mapTasks[i].status == InProgress {
					if now.Sub(c.mapTasks[i].startTime) > timeout {
						// log.Printf("Map task %d timed out, resetting to Idle", i)
						c.mapTasks[i].status = Idle
						c.mapTasks[i].workerID = 0
					}
				}
			}
		} else {
			// 检查 Reduce 任务
			for i := range c.reduceTasks {
				if c.reduceTasks[i].status == InProgress {
					if now.Sub(c.reduceTasks[i].startTime) > timeout {
						// log.Printf("Reduce task %d timed out, resetting to Idle", i)
						c.reduceTasks[i].status = Idle
						c.reduceTasks[i].workerID = 0
					}
				}
			}
		}
		c.mu.Unlock()
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.phase == AllDone {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nMap:        len(files),
		nReduce:     nReduce,
		filenames:   files,
		mapTasks:    make([]TaskInfo, len(files)),
		reduceTasks: make([]TaskInfo, nReduce),
		phase:       MapPhase,
	}

	// Your code here.

	// fmt.Printf("文件的长度是%d\n", len(files))
	go c.checkTimeouts()

	c.server()
	return &c
}
