package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	taskCh := fetchTask()
	heartBeatCh := coordinatorHeartBeat()

	for {
		select {
		case task, ok := <-taskCh:
			if !ok {
				return
			}

			switch task.TaskType {
			case Phase_Map:
				doMapTask(task, mapf)
			case Phase_Reduce:
				doReduceTask(task, reducef)
			}

			ack(task)
		case _, ok := <-heartBeatCh:
			if !ok {
				return
			}
		}
	}
}

func ack(task TaskReply) {
	var ok bool

	for !ok {
		// there might be a problem to keep acking, it should have limit.
		ok = call("Coordinator.ACK", &ACKTask{ID: task.ID, TaskType: task.TaskType}, &Empty{})
	}
}

func doReduceTask(task TaskReply, reducef func(string, []string) string) {
	var intermediate []KeyValue
	for _, filename := range task.FileNames {
		content := getFileContent(filename)
		var tempIntermediate []KeyValue
		json.Unmarshal([]byte(content), &tempIntermediate)
		intermediate = append(intermediate, tempIntermediate...)
	}

	sort.Sort(ByKey(intermediate))
	ofile, _ := os.Create(fmt.Sprintf("mr-out-%s", task.ID))

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
}

func doMapTask(task TaskReply, mapf func(string, string) []KeyValue) {
	intermediate := []KeyValue{}

	for _, filename := range task.FileNames {
		content := getFileContent(filename)
		kva := mapf(filename, content)
		intermediate = append(intermediate, kva...)
	}

	buckets := make(map[int][]KeyValue)
	for i := 0; i < len(intermediate); i++ {
		kv := intermediate[i]
		buckets[ihash(kv.Key)%task.NReduce] = append(buckets[ihash(kv.Key)%task.NReduce], kv)
	}

	for i := 0; i < task.NReduce; i++ {
		filename := fmt.Sprintf("mr-%s-%d", task.ID, i)
		ofile, _ := os.Create(filename)
		content, err := json.Marshal(buckets[i])

		if err != nil {
			log.Fatal(err)
		}

		if string(content) == "null" {
			content = []byte("{}")
		}

		ofile.Write(content)
		ofile.Close()

		call("Coordinator.AddReduceTask", &Task{
			ID: task.ID, TaskType: Task_Reduce,
			FileNames: []string{filename},
			Bucket:    i,
		}, &Empty{})
	}
}

func getFileContent(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	return string(content)
}

func fetchTask() <-chan TaskReply {
	task := make(chan TaskReply)

	go func() {
		defer close(task)

		for {
			t := TaskReply{}
			if ok := call("Coordinator.FetchTask", &Empty{}, &t); ok {
				task <- t
			}
		}
	}()

	return task
}

func coordinatorHeartBeat() <-chan struct{} {
	heartbeat := make(chan struct{})

	go func() {
		defer close(heartbeat)

		for {
			time.Sleep(time.Second)
			hr := HealthReply{}

			if ok := call("Coordinator.HeartBeat", &Empty{}, &hr); !ok {
				return
			} else {
				if hr.Health {
					heartbeat <- struct{}{}
				} else {
					return
				}
			}
		}
	}()

	return heartbeat
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

	fmt.Println(fmt.Errorf("rpcname - %s: %w", rpcname, err))
	return false
}
