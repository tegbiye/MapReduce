package mapreduce

//
// Please do not modify this file.
//

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

// track whether workers executed in parallel.
type Parallelism struct {
	mu  sync.Mutex
	now int32
	max int32
}

// Worker holds the state for a server waiting for DoTask or Shutdown RPCs
type Worker struct {
	sync.Mutex

	name        string
	Map         func(string, string) []KeyValue
	Reduce      func(string, []string) string
	nRPC        int // quit after this many RPCs; protected by mutex
	nTasks      int // total tasks executed; protected by mutex
	concurrent  int // number of parallel DoTasks in this worker; mutex
	l           net.Listener
	parallelism *Parallelism
}

// DoTask is called by the master when a new task is being scheduled on this
// worker.
func (wk *Worker) DoTask(arg *DoTaskArgs, _ *struct{}) error {
	fmt.Printf("%s: given %v task #%d on file %s (nios: %d)\n",
		wk.name, arg.Phase, arg.TaskNumber, arg.File, arg.NumOtherPhase)

	wk.Lock()
	wk.nTasks += 1
	wk.concurrent += 1
	nc := wk.concurrent
	wk.Unlock()

	if nc > 1 {
		// schedule() should never issue more than one RPC at a
		// time to a given worker.
		log.Fatal("Worker.DoTask: more than one DoTask sent concurrently to a single worker\n")
	}

	pause := false
	if wk.parallelism != nil {
		wk.parallelism.mu.Lock()
		wk.parallelism.now += 1
		if wk.parallelism.now > wk.parallelism.max {
			wk.parallelism.max = wk.parallelism.now
		}
		if wk.parallelism.max < 2 {
			pause = true
		}
		wk.parallelism.mu.Unlock()
	}

	if pause {
		// give other workers a chance to prove that
		// they are executing in parallel.
		time.Sleep(time.Second)
	}

	switch arg.Phase {
	case mapPhase:
		doMap(arg.JobName, arg.TaskNumber, arg.File, arg.NumOtherPhase, wk.Map)
	case reducePhase:
		doReduce(arg.JobName, arg.TaskNumber, mergeName(arg.JobName, arg.TaskNumber), arg.NumOtherPhase, wk.Reduce)
	}

	wk.Lock()
	wk.concurrent -= 1
	wk.Unlock()

	if wk.parallelism != nil {
		wk.parallelism.mu.Lock()
		wk.parallelism.now -= 1
		wk.parallelism.mu.Unlock()
	}

	fmt.Printf("%s: %v task #%d done\n", wk.name, arg.Phase, arg.TaskNumber)
	return nil
}

// Shutdown is called by the master when all work has been completed.
// We should respond with the number of tasks we have processed.
func (wk *Worker) Shutdown(_ *struct{}, res *ShutdownReply) error {
	debug("Shutdown %s\n", wk.name)
	wk.Lock()
	defer wk.Unlock()
	res.Ntasks = wk.nTasks
	wk.nRPC = 1
	return nil
}

// Tell the master we exist and ready to work
func (wk *Worker) register(master string) {
	args := new(RegisterArgs)
	args.Worker = wk.name
	ok := call(master, "Master.Register", args, new(struct{}))
	if ok == false {
		fmt.Printf("Register: RPC %s register error\n", master)
	}
}

// RunWorker sets up a connection with the master, registers its address, and
// waits for tasks to be scheduled.
func RunWorker(MasterAddress string, me string,
	MapFunc func(string, string) []KeyValue,
	ReduceFunc func(string, []string) string,
	nRPC int, parallelism *Parallelism,
) {
	debug("RunWorker %s\n", me)
	wk := new(Worker)
	wk.name = me
	wk.Map = MapFunc
	wk.Reduce = ReduceFunc
	wk.nRPC = nRPC
	wk.parallelism = parallelism
	rpcs := rpc.NewServer()
	rpcs.Register(wk)
	os.Remove(me) // only needed for "unix"
	l, e := net.Listen("unix", me)
	if e != nil {
		log.Fatal("RunWorker: worker ", me, " error: ", e)
	}
	wk.l = l
	wk.register(MasterAddress)

	// DON'T MODIFY CODE BELOW
	for {
		wk.Lock()
		if wk.nRPC == 0 {
			wk.Unlock()
			break
		}
		wk.Unlock()
		conn, err := wk.l.Accept()
		if err == nil {
			wk.Lock()
			wk.nRPC--
			wk.Unlock()
			go rpcs.ServeConn(conn)
		} else {
			break
		}
	}
	wk.l.Close()
	debug("RunWorker %s exit\n", me)
}

func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.

	tasks := make(chan int, ntasks)
	for i := 0; i < ntasks; i++ {
		tasks <- i
	}
	for {
		if phase == mapPhase {
			process(jobName, mapFiles, nReduce, phase, registerChan, tasks, len(tasks), n_other)
			//fmt.Println("Length of remaining tasks ", strconv.Itoa(len(tasks)))
		} else {
			process(jobName, mapFiles, len(tasks), phase, registerChan, tasks, len(tasks), n_other)
			//fmt.Println("Length of remaining tasks ", strconv.Itoa(len(tasks)))
		}
		if len(tasks) == 0 {
			break
		}
	}
	fmt.Printf("Schedule: %v done\n", phase)
}
func process(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string, tasks chan int, ntasks int, n_other int) {
	nworkers := make(chan int)
	for len(tasks) > 0 {
		j := <-tasks
		if phase == mapPhase {
			mapTask := DoTaskArgs{jobName, mapFiles[j], mapPhase, j, n_other}
			go func() {
				address := <-registerChan
				status := call(address, "Worker.DoTask", mapTask, nil)
				if status {
					nworkers <- 1           // current worker finished
					registerChan <- address // reuse the idle worker
				} else {
					tasks <- j
					nworkers <- 0 // current worker failed
				}
			}()
		} else {
			reduceTask := DoTaskArgs{jobName, "", reducePhase, j, n_other}
			go func() {
				address := <-registerChan
				status := call(address, "Worker.DoTask", reduceTask, nil)
				if status {
					nworkers <- 1           // current worker finished
					registerChan <- address // reuse the idle worker
				} else {
					tasks <- j
					nworkers <- 0 // current worker failed
				}
			}()
		}
	}
	for p := 0; p < ntasks; p++ {
		<-nworkers
	}
}

// Map and Reduce function implementations
// Debugging enabled?
const debugEnabled = false

// debug() will only print if debugEnabled is true
func debug(format string, a ...interface{}) (n int, err error) {
	if debugEnabled {
		n, err = fmt.Printf(format, a...)
	}
	return
}

// jobPhase indicates whether a task is scheduled as a map or reduce task.
type jobPhase string

const (
	mapPhase    jobPhase = "mapPhase"
	reducePhase          = "reducePhase"
)

// KeyValue is a type used to hold the key/value pairs passed to the map and
// reduce functions.
type KeyValue struct {
	Key   string
	Value string
}

// reduceName constructs the name of the intermediate file which map task
// <mapTask> produces for reduce task <reduceTask>.
func reduceName(jobName string, mapTask int, reduceTask int) string {
	return "mrtmp." + jobName + "-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask)
}

// mergeName constructs the name of the output file of reduce task <reduceTask>
func mergeName(jobName string, reduceTask int) string {
	return "mrtmp." + jobName + "-res-" + strconv.Itoa(reduceTask)
}

// What follows are RPC types and methods.
// Field names must start with capital letters, otherwise RPC will break.

// DoTaskArgs holds the arguments that are passed to a worker when a job is
// scheduled on it.
type DoTaskArgs struct {
	JobName    string
	File       string   // only for map, the input file
	Phase      jobPhase // are we in mapPhase or reducePhase?
	TaskNumber int      // this task's index in the current phase

	// NumOtherPhase is the total number of tasks in other phase; mappers
	// need this to compute the number of output bins, and reducers needs
	// this to know how many input files to collect.
	NumOtherPhase int
}

// ShutdownReply is the response to a WorkerShutdown.
// It holds the number of tasks this worker has processed since it was started.
type ShutdownReply struct {
	Ntasks int
}

// RegisterArgs is the argument passed when a worker registers with the master.
type RegisterArgs struct {
	Worker string // the worker's UNIX-domain socket name, i.e. its RPC address
}

func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func doMap(
	jobName string, // the name of the MapReduce job
	mapTask int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(filename string, contents string) []KeyValue,
) {

	input, _ := ioutil.ReadFile(inFile)
	KeyValue := mapF(inFile, string(input))
	files := make(map[string]*os.File)
	for _, key := range KeyValue {
		hash_value := ihash(key.Key) % nReduce
		filename := reduceName(jobName, mapTask, hash_value)
		if _, ok := files[filename]; !ok {
			file, _ := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
			files[filename] = file
			defer file.Close()
		}
		enc := json.NewEncoder(files[filename])
		err := enc.Encode(&key)
		if err != nil {
			fmt.Println(err)
		}
	}
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {

	keyvals := make(map[string][]string)
	for m := 0; m < nMap; m++ {
		filename := reduceName(jobName, m, reduceTask)
		file, _ := os.Open(filename)
		defer file.Close()
		dec := json.NewDecoder(file)
		for dec.More() {
			var data KeyValue
			dec.Decode(&data)
			keyvals[data.Key] = append(keyvals[data.Key], data.Value)
		}
	}
	var keys []string
	for k := range keyvals {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	ofile, _ := os.OpenFile(outFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
	enc := json.NewEncoder(ofile)
	for _, s := range keys {
		keyval := KeyValue{s, reduceF(s, keyvals[s])}
		err := enc.Encode(&keyval)
		if err != nil {
			fmt.Println(err)
		}
	}
}
