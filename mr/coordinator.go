package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	NumReduce      int             // Number of reduce tasks
	Files          []string        // Files for tasks, len(Files) is number of Map or Reduce tasks
	MapTasks       chan MapTask    // Channel for uncompleted map tasks
	CompletedTasks map[string]bool // Map to check if task is completed
	Lock           sync.Mutex      // Lock for contolling shared variables
	Phase          string          // Check if current phase is Map or Reduce
	ReduceTasks    chan ReduceTask // Channel for uncompleted reduce tasks, len(ReduceTasks) = NumReduce
}

// Starting coordinator logic
func (c *Coordinator) Start() {
	fmt.Println("Starting Coordinator, adding Map Tasks to channel")

	// Prepare initial MapTasks and add them to the queue
	sequence := 0 // give mapTask a number
	for _, file := range c.Files {
		mapTask := MapTask{
			Filename:  file,
			NumReduce: c.NumReduce,
			NumMap:    sequence,
		}
		sequence += 1

		fmt.Println("MapTask", mapTask, "added to channel")

		c.MapTasks <- mapTask
		c.CompletedTasks["map_"+mapTask.Filename] = false
	}

	c.server()
}

// RPC that worker calls when idle (worker requests a map task)
func (c *Coordinator) RequestMapTask(args *EmptyArs, reply *MapTask) error {
	fmt.Println("Map task requested")
	task, _ := <-c.MapTasks // check if there are uncompleted map tasks. Keep in mind, if MapTasks is empty, this will halt

	fmt.Println("Map task found,", task.Filename)
	if task.ToReduce {
		c.MapTasks <- task
	}
	*reply = task

	go c.WaitForWorker(task)

	return nil
}

// Goroutine will wait 10 seconds and check if map task is completed or not
func (c *Coordinator) WaitForWorker(task MapTask) {
	time.Sleep(time.Second * 10)
	c.Lock.Lock()
	if c.CompletedTasks["map_"+task.Filename] == false {
		fmt.Println("Timer expired, task", task.Filename, "is not finished. Putting back in queue.")
		c.MapTasks <- task
	}
	c.Lock.Unlock()
}

// RPC for reporting a completion of a map task
func (c *Coordinator) TaskCompleted(args *MapTask, reply *EmptyReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	c.CompletedTasks["map_"+args.Filename] = true
	fmt.Println("Task", args, "completed")

	// Check if c is currently in Map phase, if so check if all map tasks are completed
	if c.Phase == "mapper" {
		for t, finished := range c.CompletedTasks {
			if !finished {
				fmt.Println("Check complete-status:", t, "is NOT completed.")
				return nil // return immediately if there exists an incompleted file
			}
			fmt.Println("Check complete-status:", t, "is completed.")
		}
	}
	reply.Phase = "reducer"
	c.MapTasks <- MapTask{ToReduce: true}

	fmt.Println("All mapTasks are completed. Go to Reduce phase. \n")

	if !(c.Phase == "reducer") {
		c.Phase = "reducer"
		go c.makeReduceTask() // if we never call makeReduceTask()
	}

	return nil
}

func (c *Coordinator) makeReduceTask() error {
	// Aggregate intermediate files by their corresponding reducer
	// var rFiles = make(map[string][]string)
	c.Lock.Lock()
	defer c.Lock.Unlock()

	for r := 0; r < c.NumReduce; r++ {
		var inames []string // store the names of intermediate files for reducer r
		for i := 0; i < len(c.Files); i++ {
			iname := "mr-worker-" + strconv.Itoa(i) + "-" + strconv.Itoa(r)
			inames = append(inames, iname)
		}

		reduceTask := ReduceTask{
			NumReduce: strconv.Itoa(r),
			Filenames: inames,
		}
		c.ReduceTasks <- reduceTask
		c.CompletedTasks["reduce_"+strconv.Itoa(r)] = false
	}
	return nil
}

func (c *Coordinator) RequestReduceTask(args *EmptyArs, reply *ReduceTask) error {
	fmt.Println("Reduce task requested")
	task, _ := <-c.ReduceTasks // check if there are uncompleted reduce tasks

	fmt.Println("Reduce task found,", task)
	if task.Done {
		c.ReduceTasks <- task
	}
	*reply = task

	go c.WaitForReducer(task)

	return nil

}

// Goroutine will wait 10 seconds and check if reduce task is completed or not
func (c *Coordinator) WaitForReducer(task ReduceTask) {
	time.Sleep(time.Second * 10)
	c.Lock.Lock()
	if c.CompletedTasks["reduce_"+task.NumReduce] == false {
		fmt.Println("Timer expired, reducer", task.NumReduce, "is not finished. Putting back in queue.")
		c.ReduceTasks <- task
	}
	c.Lock.Unlock()
}

// RPC for reporting a completion of reduce tasks
func (c *Coordinator) ReduceCompleted(args *ReduceTask, reply *EmptyReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	c.CompletedTasks["reduce_"+args.NumReduce] = true
	fmt.Println("Reduce task", args, "completed")

	// check if all reduce tasks are completed
	for _, finished := range c.CompletedTasks { // check if all completed
		if !finished {
			return nil // return immediately if there exists an incompleted file
		}
	}

	// If all tasks completed, change the phase of coordinator
	c.Phase = "done"
	c.ReduceTasks <- ReduceTask{Done: true}
	fmt.Println("All tasks completed. Coordinator stops.")

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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	ret := false

	// Your code here.
	if c.Phase == "done" {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NumReduce:      nReduce,
		Files:          files,
		MapTasks:       make(chan MapTask, 100),
		CompletedTasks: make(map[string]bool),
		Phase:          "mapper",
		ReduceTasks:    make(chan ReduceTask, nReduce),
	}

	fmt.Println("Starting coordinator")

	c.Start()

	return &c
}
