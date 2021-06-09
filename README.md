# Word Count Map Reducer based on gRPC
A distributed map reduce program to do word count with gRPC (https://grpc.io) on local machine.

## How to run:
you can start driver program, and then open a few more terminal windows, and start more
worker program, when it finished, you should see output files containing the frequency of each work in the input
files.

## Setup project
```bigquery
go get -u github.com/golang/protobuf/protoc-gen-go
```

## Assumptions
- Both worker and driver access to same file system (they have same path to files).
- Inputs are always in a directory called `inputs`
- Intermediate files, which are results of map tasks are in `intermediates` directory.
- Output files, are in `outputs` directory
- Division of tasks is based on *Count of files*, therefore number of map tasks will be ***minimum of given number of map tasks and given number of files***
- Default number of map tasks is 6
- Default number of reduce tasks is 4

### Note
You can change these in `config.go` file.