package main

import (
	"cs3410/mapreduce/starter/mappack"
	"flag"
	"log"
)

func main() {
	// Steps: 
	// 1. need to set up command line input flags
	// 2. create a client for every worker
	// 3. start the process

	// flags
	isMaster := flag.Bool("master", false, "Run as master")
	isWorker := flag.Bool("worker", false, "Run as worker")
	input := flag.String("input", "", "Path to input file (master only)")
	masterAddr := flag.String("masterAddr", "", "Address of master (for workers)")
	port := flag.Int("port", 0, "Port to listen on (for workers)")
	m := flag.Int("m", 0, "Number of map tasks (master only)")
	r := flag.Int("r", 0, "Number of reduce tasks (master only)")

	flag.Parse()

	// client
	// same as previous assignment, take it from worker.go through mappack
	client := mappack.Client{}

	// begin process
	err := mappack.Start(client, mappack.StartConfig{
		IsMaster:       *isMaster,
		IsWorker:       *isWorker,
		InputFile:      *input,
		MasterAddr:     *masterAddr,
		Port:           *port,
		NumMapTasks:    *m,
		NumReduceTasks: *r,
	})
	if err != nil {
		log.Fatalf("MapReduce failed: %v", err)
	}

}