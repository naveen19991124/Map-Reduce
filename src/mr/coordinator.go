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

var startTime, endTime time.Time

type File struct {
	Filename string
	Filepath string
}

type TaskState int

const (
	Pending TaskState = iota + 1
	InProgress
	Completed
)

type Task struct {
	TaskId   int
	TaskType TaskType
	State    TaskState
	File     File
	NReduce  int
}

type Coordinator struct {
	MapTasks        int
	ReduceTasks     int
	TaskMap         map[int]*Task
	PendingTasks    map[TaskType][]*Task
	CompletedTasks  map[TaskType][]*Task
	InProgressTasks map[int]*Task
	Mutex           *sync.Mutex
	Channels        map[int]*chan bool
}

func (taskState *TaskState) String() string {
	return [...]string{"Pending", "InProgress", "Completed"}[*taskState-1]
}

// rpc call to get a task from the coordinator and assign it to a worker
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	var assignedTask *Task
	var noTask *Task = &Task{TaskId: -1}
	if len(c.CompletedTasks[Map]) == c.MapTasks {
		if len(c.CompletedTasks[Reduce]) == c.ReduceTasks {
			reply.Task = assignedTask
			return nil
		} else {
			if len(c.PendingTasks[Reduce]) > 0 {
				assignedTask = c.PendingTasks[Reduce][0]
			} else {
				assignedTask = noTask
				reply.Task = assignedTask
				return nil
			}
		}
	} else {
		if len(c.PendingTasks[Map]) > 0 {
			assignedTask = c.PendingTasks[Map][0]
		} else {
			assignedTask = noTask
			reply.Task = assignedTask
			return nil
		}
	}

	reply.Task = assignedTask
	log.Printf("Info: Got a RPC call from a worker, assigning task %d\n", assignedTask.TaskId)
	c.UpdateTaskState(Pending, InProgress, assignedTask)
	go startTimer(c, assignedTask, Pending, c.Channels[assignedTask.TaskId])
	return nil
}

// start a timeout for a task assigned to a worker for it's completion
func startTimer(c *Coordinator, task *Task, initialState TaskState, channel *chan bool) {
	const taskTimeout int = 10
	log.Printf("Info: Starting a timer for task with task id %d\n", task.TaskId)

	select {
	case <-*channel:
		log.Printf("Success: Got a completion confirmation for task id %d\n", task.TaskId)
	case <-time.After(time.Duration(time.Second * time.Duration(taskTimeout))):
		log.Printf("Fatal: Timer has expired for task with task id %d\n", task.TaskId)
		c.Mutex.Lock()
		if task.State != Completed {
			log.Printf("Fatal: The assigned worker was unable to finish the task with task id %d\n", task.TaskId)
			RestoreTaskState(c, task, initialState)
		} else {
			log.Printf("Success: The assigned worker was able to finish the task with task id %d\n", task.TaskId)
		}
		c.Mutex.Unlock()
	}
	return
}

// update the state of the task to COMPLETED as the coordinator gets a RPC call from the worker for task completion
func (c *Coordinator) ReportTaskCompletion(args *ReportTaskCompletionArgs, reply *ReportTaskCompletionReply) error {
	log.Printf("Info: Got a RPC call for task completion, updating task with task id %d\n", args.TaskId)
	select {
	case *c.Channels[args.TaskId] <- true:
		log.Printf("Info: Communicating the task channel for task id %d for task completion\n", args.TaskId)
	default:
		log.Printf("Info: Possibly the task channel for task id %d is not ready for receiving values\n", args.TaskId)
	}
	c.Mutex.Lock()
	task := c.TaskMap[args.TaskId]
	c.UpdateTaskState(args.PrevState, args.NewState, task)
	reply.TaskId = args.TaskId
	c.Mutex.Unlock()
	return nil
}

// update the state of task from the provided prev state to the current state(PENDING, INPROGRESS, COMPLETED)
func (c *Coordinator) UpdateTaskState(prevState TaskState, currentState TaskState, task *Task) {
	log.Printf("Info: Updating task info of task id %d from %d to %d", task.TaskId, prevState, currentState)
	taskId := task.TaskId
	taskType := task.TaskType
	if prevState == Pending && currentState == InProgress {
		for taskIdx := 0; taskIdx < len(c.PendingTasks[taskType]); taskIdx++ {
			curTask := c.PendingTasks[taskType][taskIdx]
			if curTask.TaskId == taskId {
				c.PendingTasks[taskType] = append(c.PendingTasks[taskType][:taskIdx], c.PendingTasks[taskType][taskIdx+1:]...)
				c.InProgressTasks[taskId] = task
			}
		}
	}

	if prevState == InProgress && currentState == Completed {
		curTask := c.InProgressTasks[taskId]
		c.CompletedTasks[taskType] = append(c.CompletedTasks[taskType], curTask)
		delete(c.InProgressTasks, taskId)
	}

	task.State = currentState
}

// restore the state of the task to initial state when something goes wrong with the task
func RestoreTaskState(c *Coordinator, task *Task, initialState TaskState) {
	log.Printf("Info: Restoring task with task id %d to it's initial state %s\n", task.TaskId, initialState.String())
	delete(c.InProgressTasks, task.TaskId)
	task.State = initialState
	c.PendingTasks[task.TaskType] = append(c.PendingTasks[task.TaskType], task)
	log.Printf("Success: Restored the task state with task id %d %v %s", task.TaskId, c.PendingTasks[task.TaskType], task.TaskType.String())
	return
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("Fatal: listen error:", e)
	}
	log.Printf("Success: Spinned up a server on %s\n", l.Addr())
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	ret := len(c.CompletedTasks[Map]) == c.MapTasks && len(c.CompletedTasks[Reduce]) == c.ReduceTasks

	if ret == true {
		endTime = time.Now()
		log.Printf("Success: Completed Map Reduce Job in %v\n", endTime.Sub(startTime))
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	startTime = time.Now()
	log.Printf("Info: Initializing coordinator with the %d map and %d reduce tasks\n", len(files), nReduce)
	c := Coordinator{}
	c.MapTasks = len(files)
	c.ReduceTasks = nReduce
	c.PendingTasks = make(map[TaskType][]*Task)
	c.CompletedTasks = make(map[TaskType][]*Task)
	c.InProgressTasks = make(map[int]*Task)
	c.TaskMap = make(map[int]*Task)
	c.Channels = make(map[int]*chan bool)
	c.Mutex = &sync.Mutex{}

	// creating map tasks
	for index, filename := range files {
		file := File{Filename: filename, Filepath: filename}
		mapTask := Task{TaskId: index, File: file, State: Pending, TaskType: Map, NReduce: nReduce}
		c.TaskMap[index] = &mapTask
		mapChannel := make(chan bool)
		c.Channels[index] = &mapChannel
		c.PendingTasks[Map] = append(c.PendingTasks[Map], &mapTask)
	}

	// creating reduce tasks
	for reduce := 0; reduce < nReduce; reduce++ {
		reduceTask := Task{TaskId: reduce + c.MapTasks, TaskType: Reduce, State: Pending, NReduce: reduce}
		reduceChannel := make(chan bool)
		c.Channels[reduce+c.MapTasks] = &reduceChannel
		c.TaskMap[reduce+c.MapTasks] = &reduceTask
		c.PendingTasks[Reduce] = append(c.PendingTasks[Reduce], &reduceTask)
	}

	c.server()
	return &c
}
