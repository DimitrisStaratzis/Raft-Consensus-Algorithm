package mapreduce

import (
	"fmt"
)

func callWorker(workerName string, args DoTaskArgs, mr *Master) bool {
	status := call(workerName, "Worker.DoTask", args, nil)
	mr.registerChannel <- workerName
	return status
}

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		debug("Map phase")
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
		debug("reduce phase")
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)
	/*wg := sync.WaitGroup{}
	wg.Add(ntasks)*/
	for task := 0; task < ntasks; task++ {
		args := DoTaskArgs{
			JobName:       mr.jobName,
			File:          mr.files[task],
			Phase:         phase,
			TaskNumber:    task,
			NumOtherPhase: nios,
		}

		fmt.Println("waiting for worker to connect...")
		workerName := <-mr.registerChannel
		fmt.Printf("Connected to worker %s \n", workerName)
		go callWorker(workerName, args, mr)
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
	//wg.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}
