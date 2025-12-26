package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	workerID := os.Getpid()
	// fmt.Printf("Worker %d starting\n", workerID)

	// 循环请求任务，不再区分类型
	for {
		task, ok := CallRequestTask(workerID)
		if !ok {
			log.Fatalf("Worker %d: Failed to request task", workerID)
			break
		}

		switch task.TaskType {
		case "Map":
			doMapTask(mapf, task, workerID)
		case "Reduce":
			doReduceTask(reducef, task, workerID)
		case "Wait":
			time.Sleep(time.Second) // 等待任务
		case "Exit":
			return
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func doMapTask(mapf func(string, string) []KeyValue, reply RequestTaskReply, workerID int) {
	// 作为Map Worker
	// 读取输入文件内容
	file, err := os.Open(reply.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", reply.Filename)
	}
	temp, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Filename)
	}
	file.Close()
	content := string(temp)

	// 调用 mapf 函数处理任务
	kva := mapf(reply.Filename, content)
	// 处理 kva，按照ihash规则将不同的KeyValue写入不同的中间文件
	// 首先按照 reduce 任务编号分组
	intermediate := make(map[int][]KeyValue)
	for _, kv := range kva {
		reduceTaskNum := ihash(kv.Key) % reply.NReduce
		intermediate[reduceTaskNum] = append(intermediate[reduceTaskNum], kv)
	}

	// 为每个 reduce 任务创建中间文件
	for reduceTaskNum, kvs := range intermediate {
		// 创建临时文件
		tempFileName := fmt.Sprintf("mr-tmp-%d-%d-%d", workerID, reply.MapTaskID, reduceTaskNum)
		f, err := os.OpenFile(tempFileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			log.Fatalf("Worker %d: cannot create temp file %s", workerID, tempFileName)
		}

		// 写入所有数据
		for _, kv := range kvs {
			fmt.Fprintf(f, "%v %v\n", kv.Key, kv.Value)
		}
		f.Close()

		// 重命名为最终文件名
		finalFileName := fmt.Sprintf("mr-%d-%d", reply.MapTaskID, reduceTaskNum)
		err = os.Rename(tempFileName, finalFileName)
		if err != nil {
			log.Fatalf("Worker %d: cannot rename temp file %s to %s", workerID, tempFileName, finalFileName)
		}
	}
	// fmt.Printf("Worker %d: Completed Map task %d\n", workerID, reply.TaskID)

	// 向协调器汇报任务完成
	_, success := CallReportMapDone(workerID, reply.MapTaskID)
	if !success {
		log.Fatalf("Worker %d: Failed to report completion of Map task %d", workerID, reply.MapTaskID)
	}
}

func doReduceTask(reducef func(string, []string) string, task RequestTaskReply, workerID int) {
	reduceID := task.ReduceTaskID

	// 读取所有 Map 任务的中间文件
	intermediate := []KeyValue{}
	for mapID := 0; mapID < task.NMap; mapID++ {
		filename := fmt.Sprintf("mr-%d-%d", mapID, reduceID)
		file, err := os.Open(filename)
		if err != nil {
			continue
		}

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			parts := strings.SplitN(scanner.Text(), " ", 2)
			if len(parts) == 2 {
				intermediate = append(intermediate, KeyValue{
					Key:   parts[0],
					Value: parts[1],
				})
			}
		}
		file.Close()
	}

	// 排序
	sort.Sort(ByKey(intermediate))

	// 写入输出文件
	outFile := fmt.Sprintf("mr-out-%d", reduceID)
	tmpFile := fmt.Sprintf("mr-out-tmp-%d-%d", workerID, reduceID)

	file, _ := os.Create(tmpFile)

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
		fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	file.Close()

	// 原子重命名
	os.Rename(tmpFile, outFile)
	// fmt.Printf("Worker %d: Completed Reduce task %d\n", workerID, reduceID)

	// 汇报完成
	_, success := CallReportReduceDone(workerID, reduceID)
	if !success {
		log.Fatalf("Worker %d: Failed to report completion of Reduce task %d", workerID, reduceID)
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallRequestTask(workerID int) (RequestTaskReply, bool) {
	args := RequestTaskArgs{WorkerID: workerID}
	reply := RequestTaskReply{}
	err := call("Coordinator.RequestTask", &args, &reply)
	if err != true {
		log.Fatalf("Worker %d: RPC call failed", workerID)
	}
	return reply, true
}

func CallReportMapDone(workerID int, mapTaskID int) (ReportMapDoneReply, bool) {
	args := ReportMapDoneArgs{
		WorkerID: workerID,
		TaskID:   mapTaskID,
	}
	reply := ReportMapDoneReply{}
	err := call("Coordinator.ReportMapDone", &args, &reply)
	if err != true {
		log.Fatalf("Worker %d: RPC call failed", workerID)
	}
	if !reply.Success {
		log.Fatalf("Worker %d: Coordinator failed to record completion of Map task %d", workerID, mapTaskID)
		return reply, false
	}
	return reply, true
}

func CallReportReduceDone(workerID int, reduceTaskID int) (ReportReduceDoneReply, bool) {
	args := ReportReduceDoneArgs{
		WorkerID: workerID,
		TaskID:   reduceTaskID,
	}
	reply := ReportReduceDoneReply{}
	err := call("Coordinator.ReportReduceDone", &args, &reply)
	if err != true {
		log.Fatalf("Worker %d: RPC call failed", workerID)
	}
	if !reply.Success {
		log.Fatalf("Worker %d: Coordinator failed to record completion of Reduce task %d", workerID, reduceTaskID)
		return reply, false
	}
	return reply, true
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
