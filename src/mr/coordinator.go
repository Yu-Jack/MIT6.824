package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	Phase_Map = iota + 1
	Phase_Reduce
	Phase_Finished
)

type Coordinator struct {
	phase      chan int
	wait       chan struct{}
	closing    bool
	inputFiles []string

	nMapper     int
	mapCount    int
	nReduce     int
	reduceCount int

	task        chan Task
	reduceTak   map[int][]Task
	pendingTask map[string]struct{}
	mut         sync.RWMutex
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	<-c.wait
	return true
}

func (c *Coordinator) dispatchMapTask() {
	for i := 0; i < len(c.inputFiles); i++ {
		c.task <- Task{
			TaskType:  Task_Map,
			FileNames: []string{c.inputFiles[i]},
			ID:        fmt.Sprintf("%d", i+1),
			NReduce:   c.nReduce,
		}
	}
}

func (c *Coordinator) AddReduceTask(args *Task, reply *Empty) error {
	c.mut.Lock()
	defer c.mut.Unlock()

	c.reduceTak[args.Bucket] = append(c.reduceTak[args.Bucket], Task{
		FileNames: args.FileNames,
	})

	return nil
}

func (c *Coordinator) dispatchReduceTask() {
	for i := 0; i < c.nReduce; i++ {
		fileNames := make([]string, 0, c.nReduce)

		for _, task := range c.reduceTak[i] {
			fileNames = append(fileNames, task.FileNames...)
		}

		c.task <- Task{
			TaskType:  Task_Reduce,
			FileNames: fileNames,
			ID:        fmt.Sprintf("%d", i+1),
			NReduce:   c.nReduce,
		}
	}
}

func (c *Coordinator) HeartBeat(args *Empty, reply *HealthReply) error {
	c.mut.RLock()
	defer c.mut.RUnlock()

	if c.closing {
		reply.Health = false
	} else {
		reply.Health = true
	}

	return nil
}

func (c *Coordinator) FetchTask(args *Empty, reply *Task) error {
	task := <-c.task
	reply.TaskType = task.TaskType
	reply.FileNames = task.FileNames
	reply.ID = task.ID
	reply.NReduce = task.NReduce
	c.checkTimeout(&task)
	return nil
}

func (c *Coordinator) checkTimeout(task *Task) {
	c.mut.Lock()
	defer c.mut.Unlock()

	c.pendingTask[fmt.Sprintf("%d-%s", task.TaskType, task.ID)] = struct{}{}
	time.AfterFunc(10*time.Second, func() {
		c.mut.RLock()
		defer c.mut.RUnlock()

		if _, ok := c.pendingTask[fmt.Sprintf("%d-%s", task.TaskType, task.ID)]; ok {
			c.task <- *task
		}
	})
}

func (c *Coordinator) ACK(task *ACKTask, reply *Empty) error {
	c.mut.Lock()
	defer c.mut.Unlock()

	if _, ok := c.pendingTask[fmt.Sprintf("%d-%s", task.TaskType, task.ID)]; !ok {
		return fmt.Errorf("wrong task id and type")
	}

	delete(c.pendingTask, fmt.Sprintf("%d-%s", task.TaskType, task.ID))

	if task.TaskType == Phase_Map {
		c.mapCount++
	} else if task.TaskType == Phase_Reduce {
		c.reduceCount++
	}

	if c.mapCount == c.nMapper && c.reduceCount == c.nReduce {
		c.phase <- Phase_Finished
	}

	if c.mapCount == c.nMapper && c.reduceCount == 0 {
		c.phase <- Phase_Reduce
	}

	return nil
}

func (c *Coordinator) end() {
	c.mut.Lock()
	c.closing = true
	c.mut.Unlock()

	time.Sleep(3 * time.Second) // wait for worker to shutdown
	close(c.wait)
}

func (c *Coordinator) start() {
	c.phase <- Phase_Map

	for {
		phase := <-c.phase

		switch phase {
		case Phase_Map:
			c.dispatchMapTask()
		case Phase_Reduce:
			c.dispatchReduceTask()
		case Phase_Finished:
			c.end()
		}
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := &Coordinator{
		phase:       make(chan int, 1),
		wait:        make(chan struct{}),
		closing:     false,
		task:        make(chan Task),
		nMapper:     len(files),
		inputFiles:  files,
		mapCount:    0,
		nReduce:     nReduce,
		reduceCount: 0,
		reduceTak:   make(map[int][]Task),
		pendingTask: make(map[string]struct{}),
		mut:         sync.RWMutex{},
	}

	c.server()
	go c.start()

	return c
}
