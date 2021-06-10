package worker_driver

import (
	"golang.org/x/net/context"
	"io/ioutil"
	"log"
	"strconv"
)

type TaskTracker struct {
	TodoTasks []MapReduceTask
	DoingTask []MapReduceTask
}

type MapReduceTask struct {
	TaskType                string
	Inx                     int
	numOfComplementaryTasks int
	InputPaths              []string
	OutputDirectoryPath     string
}

func (mapReduceTask MapReduceTask) wrapAsResponse(serverStatus string) (*TaskAssigningResponse, error) {
	if serverStatus == MapTasksAvailable || serverStatus == ReduceTasksAvailable {
		serverStatus = Available
	}
	return &TaskAssigningResponse{
		Status:                  serverStatus,
		TaskType:                mapReduceTask.TaskType,
		TaskIdx:                 int32(mapReduceTask.Inx),
		NumOfComplementaryTasks: int32(mapReduceTask.numOfComplementaryTasks),
		TargetInputFiles:        mapReduceTask.InputPaths,
	}, nil
}

type Server struct {
	UnimplementedManageTaskPoolServer
	NumOfMapTasks     int
	NumOfReduceTasks  int
	MapTaskTracker    TaskTracker
	ReduceTaskTracker TaskTracker
}

func (server *Server) InitServer() {
	server.NumOfMapTasks = NumOfMapTasks
	server.NumOfReduceTasks = NumOfReduceTasks

	// reconfigure number of map tasks
	files, err := ioutil.ReadDir(InputDirPath)
	if err != nil {
		panic(err)
	}
	numOfFiles := len(files)
	if numOfFiles < server.NumOfMapTasks {
		log.Printf(strconv.Itoa(numOfFiles) + " files exists here! For simplicity we don't split content of " +
			"files for map task. Therefore we ar going to have " + strconv.Itoa(numOfFiles) + " tasks.")
		server.NumOfMapTasks = numOfFiles
	}
	filesDistribution := make(map[int][]string)
	for index, file := range files {
		reminder := index % server.NumOfMapTasks
		filesDistribution[reminder] = append(filesDistribution[reminder], InputDirPath+"/"+file.Name())
	}

	for mapTaskInx := 0; mapTaskInx < server.NumOfMapTasks; mapTaskInx++ {
		t := MapReduceTask{
			TaskType:                Map,
			Inx:                     mapTaskInx,
			numOfComplementaryTasks: server.NumOfMapTasks,
			InputPaths:              filesDistribution[mapTaskInx],
		}
		server.MapTaskTracker.TodoTasks = append(server.MapTaskTracker.TodoTasks, t)
	}

	for reduceTaskInx := 0; reduceTaskInx < server.NumOfReduceTasks; reduceTaskInx++ {
		targetFiles := []string{}
		for mapTaskInx:=0; mapTaskInx < server.NumOfMapTasks; mapTaskInx ++{
			targetFiles = append(targetFiles, IntermediateDirPath+"/mr-" + strconv.Itoa(mapTaskInx) + "-" + strconv.Itoa(reduceTaskInx) + ".txt")
		}
		t := MapReduceTask{
			TaskType:                Reduce,
			Inx:                     reduceTaskInx,
			numOfComplementaryTasks: server.NumOfMapTasks,
			InputPaths:              targetFiles,
		}
		server.ReduceTaskTracker.TodoTasks = append(server.ReduceTaskTracker.TodoTasks, t)
	}
}
func (s *Server) status() string {
	if len(s.MapTaskTracker.TodoTasks) > 0 {
		return MapTasksAvailable
	} else {
		if len(s.MapTaskTracker.DoingTask) > 0 {
			return MapTasksWaitingChannel
		} else {
			if len(s.ReduceTaskTracker.TodoTasks) > 0 {
				return ReduceTasksAvailable
			} else {
				return NoTasksAvailable
			}
		}
	}
}
func (s *Server) assignMapTask() MapReduceTask {
	var assigningTask MapReduceTask
	assigningTask = s.MapTaskTracker.TodoTasks[len(s.MapTaskTracker.TodoTasks)-1]
	// remove from todolist
	s.MapTaskTracker.TodoTasks = s.MapTaskTracker.TodoTasks[:len(s.MapTaskTracker.TodoTasks)-1]
	// add to doing list
	s.MapTaskTracker.DoingTask = append(s.MapTaskTracker.DoingTask, assigningTask)

	return assigningTask
}
func (s *Server) assignReduceTask() MapReduceTask {
	var assigningTask MapReduceTask
	assigningTask = s.ReduceTaskTracker.TodoTasks[len(s.ReduceTaskTracker.TodoTasks)-1]
	// remove from todolist
	s.ReduceTaskTracker.TodoTasks = s.ReduceTaskTracker.TodoTasks[:len(s.ReduceTaskTracker.TodoTasks)-1]
	// add to doing list
	s.ReduceTaskTracker.DoingTask = append(s.ReduceTaskTracker.DoingTask, assigningTask)

	return assigningTask
}
func (t *TaskTracker) admitTask(taskIdx int) {
	log.Printf("Task %d admitted", taskIdx)
	alreadyRemoved := true
	var removeInx int
	for inx, task := range t.DoingTask {
		if task.Inx == taskIdx {
			removeInx = inx
			alreadyRemoved = false
		}
	}
	if alreadyRemoved {
		log.Fatalf("Task with task id %d already admitted", taskIdx)
	} else {
		t.DoingTask = remove(t.DoingTask, removeInx)
	}

}
func (s *Server) selectTask() (MapReduceTask, string) {
	serverStatus := s.status()
	if serverStatus == MapTasksAvailable {
		return s.assignMapTask(), serverStatus
	}
	if serverStatus == ReduceTasksAvailable {
		return s.assignReduceTask(), serverStatus
	}
	return MapReduceTask{}, serverStatus
}

func (s *Server) AssignTaskToWorker(ctx context.Context, message *TaskAssigningRequest) (*TaskAssigningResponse, error) {
	if message.Ready {
		task, serverStatus := s.selectTask()
		return task.wrapAsResponse(serverStatus)
	}
	return MapReduceTask{}.wrapAsResponse(s.status())
}
func remove(s []MapReduceTask, i int) []MapReduceTask {
	s[len(s)-1], s[i] = s[i], s[len(s)-1]
	return s[:len(s)-1]
}
func (s *Server) AdmitTaskDoneByWorker(ctx context.Context, message *TaskSubmissionRequest) (*TaskAdmissionResponse, error) {
	doneTaskType := message.DonTaskType
	doneTaskIdx := message.DoneTaskIdx
	switch doneTaskType {
	case Map:
		s.MapTaskTracker.admitTask(int(doneTaskIdx))
	case Reduce:
		s.ReduceTaskTracker.admitTask(int(doneTaskIdx))
	}

	return &TaskAdmissionResponse{
		Status: s.status(),
	}, nil
}
