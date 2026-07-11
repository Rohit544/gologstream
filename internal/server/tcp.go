package server

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/Rohit544/gologstream/internal/storage"
)

// TCPServer handles incoming raw TCP network connections
type TCPServer struct {
	listenAddr string
	wal        *storage.WAL
}

// NewTCPServer initializes our network daemon configuration
func NewTCPServer(listenAddr string, wal *storage.WAL) *TCPServer {
	return &TCPServer{
		listenAddr: listenAddr,
		wal:        wal,
	}
}

// Start boots the TCP socket listener and enters the high-concurrency accept loop
func (s *TCPServer) Start() error {
	listener, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return fmt.Errorf("failed to bind to address %s: %w", s.listenAddr, err)
	}
	defer listener.Close()

	fmt.Printf("📡 TCPServer accepting raw streaming connections on %s...\n", s.listenAddr)

	for {
		// Accept blocks until a new client (producer/consumer) hits the socket port
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("❌ Failed to accept connection: %v", err)
			continue
		}

		// 🧵 Google-Scale Concurrency: Spin up a lightweight goroutine thread instantly
		// to handle this specific client so the main loop can immediately accept the next user.
		go s.handleConnection(conn)
	}
}

func (s *TCPServer) handleConnection(conn net.Conn) {
	defer conn.Close()
	log.Printf("👥 Client connected from remote address: %s", conn.RemoteAddr().String())

	// Use an optimized buffered reader to stream incoming lines over the wire efficiently
	reader := bufio.NewReader(conn)

	for {
		// Read string input up until a newline delimiter (\n)
		line, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("🔌 Client disconnected or connection broken: %s", conn.RemoteAddr().String())
			return
		}

		// Clean up carriage returns (\r\n) from incoming terminal buffers
		messagePayload := strings.TrimSpace(line)
		if messagePayload == "" {
			continue
		}

		// 1. If a client sends "EXIT", terminate their socket stream cleanly
		if messagePayload == "EXIT" {
			_, _ = conn.Write([]byte("Goodbye!\n"))
			return
		}

		// 2. 🔍 NEW CONSUME PROTOCOL LAYER: Handle historical playback streams
		if strings.HasPrefix(messagePayload, "CONSUME ") {
			var startOffset int64
			// Parse the offset number out of the command string (e.g., "CONSUME 76")
			_, err := fmt.Sscanf(messagePayload, "CONSUME %d", &startOffset)
			if err != nil {
				_, _ = conn.Write([]byte("ERROR: Invalid consume syntax. Use 'CONSUME <offset>'\n"))
				continue
			}

			// Read records from the storage layer
			records, err := s.wal.ReadFromOffset(startOffset)
			if err != nil {
				log.Printf("❌ Failed to read log from offset %d: %v", startOffset, err)
				_, _ = conn.Write([]byte("ERROR: Internal read failure\n"))
				continue
			}

			// Stream the recovered data payloads back down the bare-metal socket line
			_, _ = conn.Write([]byte(fmt.Sprintf("--- STREAM START FROM OFFSET %d ---\n", startOffset)))
			for _, record := range records {
				_, _ = conn.Write([]byte(fmt.Sprintf("📖 %s\n", string(record))))
			}
			_, _ = conn.Write([]byte("--- STREAM END ---\n"))
			continue // Jump straight back to top of the loop to wait for next command
		}

		// 3. 💾 COMMIT TO DISK: (Only runs if the message wasn't EXIT or CONSUME)
		offset, err := s.wal.Append([]byte(messagePayload))
		if err != nil {
			log.Printf("❌ Critical WAL append failure: %v", err)
			_, _ = conn.Write([]byte("ERROR: Internal storage failure\n"))
			return
		}

		// Send an acknowledgement payload back to the client confirming data durability
		ackMessage := fmt.Sprintf("ACK: Durable commit locked at disk offset %d\n", offset)
		_, _ = conn.Write([]byte(ackMessage))
	}

}
