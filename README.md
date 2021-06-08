# Word Count Map Reducer based on gRPC
A distributed map reduce program to do word count with gRPC (https://grpc.io) on local machine,
when you finish.

## How to run:
you can start driver program, and then open a few more terminal windows, and start more
worker program, when it finished, you should see output files containing the frequency of each work in the input
files.

## Setup project
```bigquery
go get -u github.com/golang/protobuf/protoc-gen-go
```