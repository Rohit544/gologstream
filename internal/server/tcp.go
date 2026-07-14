package server

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
)

// StorageEngine defines the boundary interface our server needs to talk to the WAL layer safely
type StorageEngine interface {
	Append(data []byte) (int64, error)
	GetSize() (int64, error)
}

// TCPServer manages high-throughput concurrent socket channels
type TCPServer struct {
	listenAddr string
	wal        StorageEngine
}

// NewTCPServer instantiates our bare-metal network engine wrapper
func NewTCPServer(listenAddr string, wal StorageEngine) *TCPServer {
	return &TCPServer{
		listenAddr: listenAddr,
		wal:        wal,
	}
}

// Start opens up the raw TCP port listening loops
func (s *TCPServer) Start() error {
	listener, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return fmt.Errorf("failed to bind to address %s: %w", s.listenAddr, err)
	}
	defer listener.Close()

	fmt.Println("🚀 Initializing GoLogStream Production Engine Node...")
	fmt.Printf("📡 TCPServer accepting raw streaming connections on %s...\n", s.listenAddr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("⚠️ Network handshake drop: %v", err)
			continue
		}

		// Spin up an decoupled concurrency lane (Goroutine thread) for every single client
		go s.handleConnection(conn)
	}
}

func (s *TCPServer) handleConnection(conn net.Conn) {
	defer conn.Close()
	log.Printf("👥 Client connected from remote address: %s", conn.RemoteAddr().String())

	reader := bufio.NewReader(conn)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("🔌 Client disconnected: %s", conn.RemoteAddr().String())
			return
		}

		messagePayload := strings.TrimSpace(line)
		if messagePayload == "" {
			continue
		}

		// 1. If a client sends "EXIT", terminate their socket stream cleanly
		if messagePayload == "EXIT" {
			_, _ = conn.Write([]byte("Goodbye!\n"))
			return
		}

		// 2. 📊 STAT PROTOCOL LAYER: Handle metadata queries
		if messagePayload == "STAT" {
			size, err := s.wal.GetSize()
			if err != nil {
				log.Printf("❌ Failed to fetch log size stats: %v", err)
				_, _ = conn.Write([]byte("ERROR: Internal storage stat failure\n"))
				continue
			}

			responseMessage := fmt.Sprintf("INFO: Current segment active size is %d bytes\n", size)
			_, _ = conn.Write([]byte(responseMessage))
			continue
		}
				// 📊 NEW STAT PROTOCOL LAYER: Handle metadata queries
		if messagePayload == "STAT" {
			// 1. FETCH SIZE FROM STORAGE
			size, err := s.wal.GetSize()
			if err != nil {
				log.Printf("❌ Failed to fetch log size stats: %v", err)
				_, _ = conn.Write([]byte("ERROR: Internal storage stat failure\n"))
				continue
			}

			// 2. FORMAT AND TRANSMIT OVER WIRE
			responseMessage := fmt.Sprintf("INFO: Current log size is %d bytes\n", size)
			_, _ = conn.Write([]byte(responseMessage))
			continue // Jump straight back to the top of the loop to wait for the next command!
		}


		// 3. 🔍 CONSUME LAYER PLACEHOLDER: (Temporarily paused during active rotation testing)
		if strings.HasPrefix(messagePayload, "CONSUME ") {
			_, _ = conn.Write([]byte("NOTICE: Consumer replay is updating for multi-segment files.\n"))
			continue
		}

		// 4. 💾 COMMIT TO DISK: Segment-aware append operation
		offset, err := s.wal.Append([]byte(messagePayload))
		if err != nil {
			log.Printf("❌ Critical WAL append failure: %v", err)
			_, _ = conn.Write([]byte("ERROR: Internal storage failure\n"))
			return
		}

		ackMessage := fmt.Sprintf("ACK: Durable commit locked at segment local offset %d\n", offset)
		_, _ = conn.Write([]byte(ackMessage))
	}
}
