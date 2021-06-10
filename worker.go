package main

import (
	"context"
	"google.golang.org/grpc"
	"log"
	"map-reduce-grpc/worker_driver"
	"time"
)

//Configs of worker are defined as global variables
var inputDirPath = worker_driver.InputDirPath
var outputDirPath = worker_driver.OutputDirPath
var intermediateDirPath = worker_driver.IntermediateDirPath

var currentMode string
var taskIndex int
var numOfComplementaryTasks int
var targetFiles []string
var driverMode = worker_driver.Available

var client worker_driver.ManageTaskPoolClient
var taskAssigningRequest worker_driver.TaskAssigningRequest
var taskSubmissionRequest worker_driver.TaskSubmissionRequest

func configWorkerBasedOnNewTask(response *worker_driver.TaskAssigningResponse) {
	driverMode = response.Status
	driverMode = response.Status
	currentMode = response.TaskType
	taskIndex = int(response.TaskIdx)
	numOfComplementaryTasks = int(response.NumOfComplementaryTasks)
	targetFiles = response.TargetInputFiles
}


func run() string{
	log.Printf("Switch worker mode to, %s", currentMode)
	switch currentMode {
	case worker_driver.Map:
		mapperMain()
	case worker_driver.Reduce:
		reducerMain()
	}
	taskSubmissionRequest.DonTaskType = currentMode
	taskSubmissionRequest.DoneTaskIdx =int32 (taskIndex)
	response, err := client.AdmitTaskDoneByWorker(context.Background(), &taskSubmissionRequest)
	if err != nil {
		log.Fatalf("Failed to submit task to driver to port 9000, following error occurred \t %v", err)
	}
	return response.Status
}

func main() {
	var conn, err = grpc.Dial(":9000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to port 9000, following error occurred \t %v", err)

	}
	defer conn.Close()
	client = worker_driver.NewManageTaskPoolClient(conn)
	// whenever worker is done, it is ready for new task
	taskAssigningRequest = worker_driver.TaskAssigningRequest{Ready: true}


	for driverMode!=worker_driver.NoTasksAvailable{
		log.Printf("Driver status is: %s", driverMode)

		response, err := client.AssignTaskToWorker(context.Background(), &taskAssigningRequest)
		if err != nil {
			log.Fatalf("Failed to assign task to this worker to port 9000, following error occurred \t %v", err)
		}
		configWorkerBasedOnNewTask(response)
		switch driverMode {
		case worker_driver.Available:
			driverMode = run()
		case worker_driver.MapTasksWaitingChannel:
			log.Println("Waiting for map channel ... Waiting for 10 seconds")
			time.Sleep(10 * time.Second)
		}
	}
}
