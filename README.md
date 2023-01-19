# MapReduce Implementation

Description:

"In this lab you'll develop a MapReduce system. You'll implement a worker process that calls application Map and Reduce functions and handles reading and writing files, and a coordinator process that hands out tasks to workers and copes with failed workers. You'll be building something similar to the MapReduce paper."

"Your job is to implement a distributed MapReduce, consisting of two programs, the coordinator and the worker. There will be just one coordinator process, and one or more worker processes executing in parallel. In a real system the workers would run on a bunch of different machines, but for this lab you'll run them all on a single machine. The workers will talk to the coordinator via RPC. Each worker process will ask the coordinator for a task, read the task's input from one or more files, execute the task, and write the task's output to one or more files. The coordinator should notice if a worker hasn't completed its task in a reasonable amount of time (for this lab, use ten seconds), and give the same task to a diff erent worker. "

The mapreduce modules:
1. Coordinator programs
   - coordinator.go: coordinates the process or we can say it is a master process
   - coordinator_rpc.go: connects on the distributed mode
   - coordinator_merge.go: merges the files
2. Worker Programs
   - worker.go: 
   - map.go:   the doMap job 
   - reduce.go:  the doReduce job
   - rpc.go: connects on the distributed mode
3. scheduler:
   - scheduler.go: schedules the map_Phase and reduse_Phase
