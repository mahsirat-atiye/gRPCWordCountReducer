package worker_driver

const (
	InputDirPath        = "inputs"
	OutputDirPath       = "outputs"
	IntermediateDirPath = "intermediates"
	NumOfMapTasks       = 6
	NumOfReduceTasks    = 4
)
const (
	MapTasksAvailable      = "MapTasksAvailable"
	MapTasksWaitingChannel = "MapTasksWaitingChannel"
	ReduceTasksAvailable   = "ReduceTasksAvailable"
	NoTasksAvailable       = "NoTasksAvailable"
	Available              = "Available"
)

const (
	Map    = "Map"
	Reduce = "Reduce"
)
