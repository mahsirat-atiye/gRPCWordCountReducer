package main

import (
	"google.golang.org/grpc"
	"log"
	"map-reduce-grpc/worker_driver"
	"net"
)

func main() {
	lis, err := net.Listen("tcp", ":9000")

	if err != nil {
		log.Fatalf("Failed to listen on port 9000, following error occurred \t %v", err)

	}
	s := worker_driver.Server{}
	s.InitServer()
	grpcServer := grpc.NewServer()
	worker_driver.RegisterManageTaskPoolServer(grpcServer, &s)
	if err := grpcServer.Serve(lis); err != nil{
		log.Fatalf("Failed to serve grpc Server on port 9000, following error occurred\t %v", err)
	}

}
