package main

import (
	"fmt"
	"log"

	"github.com/Rohit544/gologstream/internal/server"
	"github.com/Rohit544/gologstream/internal/storage"
)

func main() {
	fmt.Println("🚀 Initializing GoLogStream Production Engine Node...")

	// 1. Mount the crash-resilient storage WAL layer
	wal, err := storage.NewWAL("commit.log")
	if err != nil {
		log.Fatalf("❌ Critical Storage Initialization Failure: %v", err)
	}
	defer wal.Close()

	// 2. Instantiate the network server daemon on port 8080
	srv := server.NewTCPServer(":8080", wal)

	// 3. Fire up the server daemon
	if err := srv.Start(); err != nil {
		log.Fatalf("❌ Critical Network Server Crash: %v", err)
	}
}
