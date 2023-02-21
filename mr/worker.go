package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type WorkerSt struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
	// phase   string
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	w := WorkerSt{
		mapf:    mapf,
		reducef: reducef,
		// phase:   "Map",
	}

	w.RequestTask()
}

// Requests map and then reduce task
func (w *WorkerSt) RequestTask() {
	// requests map task, tries to do it, and repeats
	for {
		c := EmptyArs{}
		reply := MapTask{}
		call("Coordinator.RequestMapTask", &c, &reply)

		if reply.ToReduce {
			break
		}

		file, err := os.Open(reply.Filename)
		if err != nil {
			log.Fatalf("cannot open %v", reply.Filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", reply.Filename)
		}
		file.Close()

		// prepare file descriptors in advance for better performance
		fds := make([]*os.File, reply.NumReduce)
		for i := 0; i < reply.NumReduce; i++ {
			iname := "mr-worker-" + strconv.Itoa(reply.NumMap) + "-" + strconv.Itoa(i)
			ifile, err := os.OpenFile(iname, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
			if err != nil {
				log.Fatal("cannot open file", iname, err)
			}
			fds[i] = ifile
		}

		// store kva in multiple files according to rules described in the README
		kva := w.mapf(reply.Filename, string(content))
		for _, kv := range kva {
			ifile := fds[ihash(kv.Key)%reply.NumReduce]

			// Encode kv to JSON and write it to the end of ifile
			encoder := json.NewEncoder(ifile)
			err = encoder.Encode(kv)
			if err != nil {
				log.Fatalf("Map phase: cannot encode kv %v", kv)
			}
			// ifile.Close()
		}
		for _, ifile := range fds {
			ifile.Close()
		}

		fmt.Println("Map task for", reply.Filename, "completed")

		emptyReply := EmptyReply{Phase: "mapper"}
		call("Coordinator.TaskCompleted", &reply, &emptyReply)
		if emptyReply.Phase == "reducer" {
			break // break out of mapper loop to reduce loop
		}
	}

	// time.Sleep(time.Second * 1) // wait for 1 sec until sending reduce request
	fmt.Println("Worker starts to ask for reduce tasks")

	// requests reduce task, tries to do it, and repeats
	for {
		c := EmptyArs{}
		reply := ReduceTask{}
		call("Coordinator.RequestReduceTask", &c, &reply)

		if reply.Done {
			break
		}

		// aggregate all KeyValue pairs from files assigned to current reducer
		var kva4Y []KeyValue

		for _, filename := range reply.Filenames { // get data from each file and add to kva4Y
			// Read the JSON data from a file
			ifile, err := os.Open(filename)
			if err != nil {
				log.Fatalf("Reduce phase - cannot open %v: %v", filename, err)
			}

			// Create a new scanner to read the file line by line
			scanner := bufio.NewScanner(ifile)
			for scanner.Scan() {
				line := scanner.Text()
				var kv KeyValue
				err := json.Unmarshal([]byte(line), &kv)
				if err != nil {
					continue
				}
				kva4Y = append(kva4Y, kv)
			}

			ifile.Close()
		}

		sort.Sort(ByKey(kva4Y))

		// start to generate output file
		ofile, _ := os.Create("mr-out-" + reply.NumReduce)

		i := 0
		for i < len(kva4Y) {
			j := i + 1
			for j < len(kva4Y) && kva4Y[j].Key == kva4Y[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kva4Y[k].Value)
			}
			output := w.reducef(kva4Y[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", kva4Y[i].Key, output)

			i = j
		}
		ofile.Close()

		fmt.Println("Reducer", reply.NumReduce, "completed")

		// Exit when all reduce files are completed
		emptyReply := EmptyReply{Phase: "reducer"}
		call("Coordinator.ReduceCompleted", &reply, &emptyReply)
		if emptyReply.Phase == "done" {
			break
		}
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
