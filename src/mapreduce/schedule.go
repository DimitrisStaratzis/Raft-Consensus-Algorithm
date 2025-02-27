package mapreduce

import (
	"fmt"
	"sync"
)

func callWorker(workerName string, args DoTaskArgs, mr *Master, tasksStatus map[string]int, index int, wg *sync.WaitGroup) bool {
	//var err error

	ok := call(workerName, "Worker.DoTask", args, new(struct{}))
	if ok {
		tasksStatus[mr.files[index]] = 2 //file processed correctly
	} else {
		tasksStatus[mr.files[index]] = 0 //file did not process correctly
	}
	wg.Done()
	mr.registerChannel <- workerName //put worker back as soon as it finishes its task
	return ok
}

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
// All ntasks tasks have to be scheduled on workers, and only once all of
// them have been completed successfully should the function return.
// Remember that workers may fail, and that any given worker may finish
// multiple tasks.
//
// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO

//
//wg.Wait()
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
	// 0 means not started
	// 1 means pending
	// 2 means finished
	tasksStatus := make(map[string]int)
	for task := 0; task < ntasks; task++ {
		tasksStatus[mr.files[task]] = 0
	}
	var wg sync.WaitGroup
	task := 0
	for {
		if tasksStatus[mr.files[task]] == 0 {
			args := DoTaskArgs{
				JobName:       mr.jobName,
				File:          mr.files[task],
				Phase:         phase,
				TaskNumber:    task,
				NumOtherPhase: nios,
			}

			workerName := <-mr.registerChannel
			wg.Add(1)
			tasksStatus[mr.files[task]] = 1 //working on this file
			go callWorker(workerName, args, mr, tasksStatus, task, &wg)

		}

		if task == ntasks-1 {
			counter := 0
			for j := 0; j < ntasks; j++ {
				if tasksStatus[mr.files[j]] == 2 {
					counter++
				}
			}
			if counter == ntasks {
				break
			} else {
				task = -1
			}

		}
		task++
	}
	wg.Wait()

	fmt.Printf("Schedule: %v phase done\n", phase)
}
