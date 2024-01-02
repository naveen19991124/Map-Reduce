package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strings"
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

// perform a reduced hashing over the intermediate data to get the different keys which get mapped to a particular reduce task
func performReducedHashing(kva []KeyValue, reducedKVA map[int][]KeyValue, nReduce int) {
	for _, kv := range kva {
		hashedKey := ihash(kv.Key) % nReduce
		reducedKVA[hashedKey] = append(reducedKVA[hashedKey], kv)
	}
}

// writing intermediate data from the reduced key value pairs generated for each reduce task
func writeIntermediateDataToJSONFiles(reducedKVA map[int][]KeyValue, taskId, nReduce int) {
	var filenames []string
	for reduce := 0; reduce < nReduce; reduce++ {
		oname := "mr-" + fmt.Sprint(fmt.Sprint(taskId)+"-"+fmt.Sprint(reduce)+".tmp")
		filenames = append(filenames, oname)
		ofile, err := os.OpenFile(oname, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0777)
		if err != nil {
			log.Fatalf("cannot open file %v", ofile)
		}
		enc := json.NewEncoder(ofile)
		for _, kv := range reducedKVA[reduce] {
			encErr := enc.Encode(&kv)
			if encErr != nil {
				log.Fatalf("cannot encode %v", kv)
			}
		}
		ofile.Close()
	}

	// Atomic renaming of files containing intermediate data
	for _, filename := range filenames {
		os.Rename(filename, strings.TrimSuffix(filename, ".tmp"))
	}
}

// perform map
func PerformMapTask(task *Task, mapf func(string, string) []KeyValue) {
	log.Printf("Info: Performing map task for task id %d\n", task.TaskId)
	filename := task.File.Filename
	nReduce := task.NReduce
	taskId := task.TaskId
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	reducedKVA := make(map[int][]KeyValue)
	performReducedHashing(kva, reducedKVA, nReduce)
	writeIntermediateDataToJSONFiles(reducedKVA, taskId, nReduce)
}

// perform reduce
func PerformReduceTask(task *Task, reducef func(string, []string) string) {
	log.Printf("Info: Performing reduce task for task id %d\n", task.TaskId)
	nReduce := task.NReduce

	files, _ := filepath.Glob(fmt.Sprintf("mr-*-" + fmt.Sprint(nReduce)))

	var intermediate []KeyValue
	for _, file := range files {
		curFile, err := os.Open(file)
		if err != nil {
			log.Fatalf("cannot open %v", curFile)
		}
		dec := json.NewDecoder(curFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		curFile.Close()
	}

	sort.Sort(ByKey(intermediate))
	oname := "mr-out-" + fmt.Sprint(nReduce)
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
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		task := CallGetTask()
		if task.TaskId == -1 {
			continue
		}
		if task == nil {
			break
		}
		taskType := task.TaskType
		switch taskType {
		case Map:
			PerformMapTask(task, mapf)
			log.Printf("Info: Making RPC call to report completion of reduce task id %d\n", task.TaskId)
			CallReportTaskCompletion(&ReportTaskCompletionArgs{PrevState: InProgress, NewState: Completed, TaskId: task.TaskId})
		case Reduce:
			PerformReduceTask(task, reducef)
			log.Printf("Info: Making RPC call to report completion of reduce task id %d\n", task.TaskId)
			CallReportTaskCompletion(&ReportTaskCompletionArgs{PrevState: InProgress, NewState: Completed, TaskId: task.TaskId})
		default:
			log.Fatal("Fatal: Unknown task type")
		}
	}
}

// send a RPC call to the coordinator for getting any map/reduce task
func CallGetTask() *Task {
	args := new(GetTaskArgs)
	reply := new(GetTaskReply)

	ok := call("Coordinator.GetTask", args, reply)
	if ok {
		if reply.Task == nil {
			log.Printf("Info: Got no task from the coordinator, possibly the map reduce job has been completed!")
			os.Exit(0)
		} else if reply.Task.TaskId == -1 {
			log.Printf("Info: Did not get any task, possibly all tasks are in progress\n")
		} else {
			log.Printf("Success: Got %s task with task id %d from the coordinator", reply.Task.TaskType.String(), reply.Task.TaskId)
		}
		return reply.Task
	} else {
		log.Printf("Failure: RPC call to coordinator failed!\n")
		return nil
	}
}

// send a RPC call to the coordinator reporting that the assigned task has been completed
func CallReportTaskCompletion(taskCompletionArgs *ReportTaskCompletionArgs) {
	args := taskCompletionArgs
	reply := new(ReportTaskCompletionReply)

	log.Printf("Info: Updating task with task id %d", args.TaskId)
	ok := call("Coordinator.ReportTaskCompletion", args, reply)
	if ok {
		log.Printf("Success: Reported task completion for task id %d", reply.TaskId)
	} else {
		log.Printf("Failure: RPC call to coordinator failed!\n")
	}
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
