package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

//WAL represents a high-performance , thread-safe Write-Ahead Log

type WAL struct {
	file *os.File
	mu sync.Mutex
}

// NewWAL initializes or recovers an existing binary log file on physical disk
func NewWAL(filePath string) (*WAL, error){


	file,err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil{
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	return &WAL{file: file} , nil
}



// Append writes raw message data to the disk using a secure binary framing protocol
func (w *WAL) Append(data []byte) (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 1. Fetch current file offset (this acts as our Message ID)
	stat, err := w.file.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to check log stats: %w", err)
	}
	offset := stat.Size()

	// 2. Binary Framing: Write the length of the data first as a 4-byte uint32 integer.
	lengthBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBuf, uint32(len(data)))

	// 3. Write length prefix to disk
	if _, err := w.file.Write(lengthBuf); err != nil {
		return 0, fmt.Errorf("failed to write binary frame header: %w", err)
	}

	// 4. Write actual payload data payload directly behind it
	if _, err := w.file.Write(data); err != nil {
		return 0, fmt.Errorf("failed to write payload data: %w", err)
	}

	// 5. System Call Force-Sync (Bypasses OS memory cache buffers straight to physical disk hardware)
	if err := w.file.Sync(); err != nil {
		return 0, fmt.Errorf("failed to commit log to disk hardware (fsync): %w", err)
	}

	return offset, nil
}



func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

// ReadFromOffset opens the log in read-only mode and streams records starting from a specific byte offset
func (w *WAL) ReadFromOffset(startOffset int64) ([][]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Open a separate read-only file descriptor to prevent modifying the write pointer
	file, err := os.Open(w.file.Name())
	if err != nil {
		return nil, fmt.Errorf("failed to open log for reading: %w", err)
	}
	defer file.Close()

	// Seek straight to the requested byte address (bypasses reading early file lines)
	_, err = file.Seek(startOffset, 0)
	if err != nil {
		return nil, fmt.Errorf("invalid offset seek address: %w", err)
	}

	var records [][]byte
	headerBuf := make([]byte, 4)

	for {
		// 1. Read the 4-byte length prefix header
		_, err := file.Read(headerBuf)
		if err != nil {
			break // Reached EOF (End of File), stop reading cleanly
		}

		// 2. Decode the binary big-endian uint32 into a Go integer
		length := binary.BigEndian.Uint32(headerBuf)

		// 3. Allocate a precise memory buffer for the payload size
		payloadBuf := make([]byte, length)
		_, err = file.Read(payloadBuf)
		if err != nil {
			break
		}

		// 4. Append decoded record to slice
		records = append(records, payloadBuf)
	}

	return records, nil
}
