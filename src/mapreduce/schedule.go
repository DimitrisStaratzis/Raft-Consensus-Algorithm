package mapreduce

import "fmt"

func callWorker(workerName string, args DoTaskArgs) bool {
	var err error
	status := call(workerName, "Worker.DoTask", args, &err)
	if err != nil {
		fmt.Println(err)
	}
	return status
}

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	for task := 0; task < ntasks; task++ {
		args := new(DoTaskArgs)
		args.JobName = mr.jobName
		args.File = mr.files[task]
		args.Phase = phase
		args.TaskNumber = task
		args.NumOtherPhase = nios

		fmt.Println("waiting for worker to connect...")
		workerName := <-mr.registerChannel
		fmt.Printf("Connected to server %s \n", workerName)
		go callWorker(workerName, *args)
		/*if taskStatus == false {
			fmt.Printf("Worker %s failed\n", workerName)
		} else {

		}*/
	}

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO

	//
	fmt.Printf("Schedule: %v phase done\n", phase)
}
