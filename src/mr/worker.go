package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path"
	"sort"
	"time"
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

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	//fmt.Println("Worker started")

	// the current working directory and a temporary directory
	cwd, _ := os.Getwd()
	// usually this would be /tmp, but / and /home are on different btrfs subvolumes on my system which causes the error "invalid cross-device link" when moving files
	tempdir := cwd

	// repeatedly request work
	for {
		task := TaskRequestReply{}
		ok := call("Coordinator.TaskRequest", &TaskRequestArgs{}, &task)
		if ok {
			//fmt.Printf("taskToDo has id %d\n", task.TaskToDo)
		} else {
			// assume the coordinator is dead because all the tasks have been completed
			return
		}

		switch task.TaskToDo {
		case TaskUnavailable:
			// sleep for one second and repeat the loop to ask for another task
			time.Sleep(time.Second)
			continue
		case MapTask:
			// file to map
			filename := task.MapFilename

			// intermediate[k] stores the list of key-value pairs corresponding to reduce task number k
			intermediate := make([][]KeyValue, task.NReduce)
			for i := 0; i < task.NReduce; i++ {
				intermediate[i] = []KeyValue{}
			}

			// read input file and run map on it, storing the output in kva
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))

			// split kva into nReduce intermediate lists
			for _, kv := range kva {
				reduceTaskNum := ihash(kv.Key) % task.NReduce
				intermediate[reduceTaskNum] = append(intermediate[reduceTaskNum], kv)
			}

			// write to intermediate files
			for i := 0; i < task.NReduce; i++ {
				dest_filename := fmt.Sprintf("mr-%d-%d", task.MapTaskNumber, i)
				// create a temporary file that is guaranteed not to share a name with any others
				dest_file, _ := os.CreateTemp(tempdir, "mrtempfile_intermediate_output")

				enc := json.NewEncoder(dest_file)
				for _, kv := range intermediate[i] {
					enc.Encode(&kv)
				}
				// once all the data has beeen written to disk, rename the output file. this operation is atomic on UNIX
				dest_file.Close()
				os.Rename(dest_file.Name(), path.Join(cwd, dest_filename))
			}

			completeReport := MarkMapCompleteArgs{}
			completeReport.MapTaskNumber = task.MapTaskNumber
			// the coordinator is waiting for me to complete my map task, so it will definitely not be dead and this call() will return without an error
			call("Coordinator.MarkMapComplete", &completeReport, &MarkMapCompleteReply{})
		case ReduceTask:
			// populate key-value array read in from intermidiate input files
			intermediate := []KeyValue{}
			for i := 0; i < task.NMap; i++ {
				source_filename := fmt.Sprintf("mr-%d-%d", i, task.ReduceTaskNumber)
				source_file, _ := os.Open(source_filename)
				dec := json.NewDecoder(source_file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				source_file.Close()
			}

			sort.Sort(ByKey(intermediate))

			oname := fmt.Sprintf("mr-out-%d", task.ReduceTaskNumber)
			// create a temporary file that is guaranteed not to share a name with any others
			ofile, _ := os.CreateTemp(tempdir, "mrtempfile_reduce_output")

			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-X, where X is the reduce task number
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
			// once all the data has beeen written to disk, rename the output file. this operation is atomic on UNIX
			ofile.Close()
			os.Rename(ofile.Name(), path.Join(cwd, oname))

			// remove intermediate files
			for i := 0; i < task.NMap; i++ {
				os.Remove(fmt.Sprintf("mr-%d-%d", i, task.ReduceTaskNumber))
			}

			completeReport := MarkReduceCompleteArgs{}
			completeReport.ReduceTaskNumber = task.ReduceTaskNumber
			// the coordinator is waiting for me to complete my reduce task, so it will definitely not be dead and this call() will return without an error
			call("Coordinator.MarkReduceComplete", &completeReport, &MarkReduceCompleteReply{})
		}
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		// the coordinator is no longer running
		// log.Fatal("dialing:", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
