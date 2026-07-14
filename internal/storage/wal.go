package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// WAL manages a directory of segmented binary log files
type WAL struct {
	mu              sync.Mutex
	dirPath         string
	activeFile      *os.File
	currentSegment  int64
	maxSegmentSize  int64
}

// NewWAL initializes the storage directory and mounts the latest active segment file
func NewWAL(dirPath string) (*WAL, error) {
	// Create the storage tracking directory if it doesn't exist yet
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory: %w", err)
	}

	w := &WAL{
		dirPath:        dirPath,
		currentSegment: 0,
		maxSegmentSize: 100, // 💡 Small boundary (100 Bytes) to test rotation easily!
	}

	// Mount the initial segment file (00000000.seg)
	if err := w.openCurrentSegment(); err != nil {
		return nil, err
	}

	return w, nil
}

// openCurrentSegment sets up a specific numbered physical file descriptor
func (w *WAL) openCurrentSegment() error {
	fileName := fmt.Sprintf("%08d.seg", w.currentSegment)
	filePath := filepath.Join(w.dirPath, fileName)

	// Open file in Append, Create, and Read-Write modes
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open segment file %s: %w", fileName, err)
	}

	w.activeFile = file
	return nil
}

// Append writes data to the current active segment, automatically rotating files if full
func (w *WAL) Append(data []byte) (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 1. Check current segment physical size
	stat, err := w.activeFile.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to read active segment stats: %w", err)
	}

	totalMessageSize := int64(4 + len(data))

	// 2. ROTATION TRIGGER: Will this append cross our 100-byte boundary limit?
	if stat.Size()+totalMessageSize > w.maxSegmentSize {
		// Cleanly seal the current file
		w.activeFile.Close()

		// Increment the active storage index partition
		w.currentSegment++
		fmt.Printf("🔄 [ROTATION ACTIVATED] Segment full! Creating new partition: %08d.seg\n", w.currentSegment)

		// Open the next blank partition file
		if err := w.openCurrentSegment(); err != nil {
			return 0, fmt.Errorf("failed to rotate to new segment: %w", err)
		}
	}

	// 3. Re-read stats to get the exact clean landing offset for this file segment
	currentStat, err := w.activeFile.Stat()
	if err != nil {
		return 0, err
	}
	offset := currentStat.Size()

	// 4. Write binary data framing length header (4-byte BigEnd uint32)
	headerBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(headerBuf, uint32(len(data)))
	if _, err := w.activeFile.Write(headerBuf); err != nil {
		return 0, err
	}

	// 5. Write the message string bytes payload
	if _, err := w.activeFile.Write(data); err != nil {
		return 0, err
	}

	// 6. Force immediate kernel hardware sync cache bypass
	if err := w.activeFile.Sync(); err != nil {
		return 0, err
	}

	return offset, nil
}

// GetSize queries the current active file segment metadata size
func (w *WAL) GetSize() (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	stat, err := w.activeFile.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

// Close cleanly seals the active running storage channels
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.activeFile != nil {
		return w.activeFile.Close()
	}
	return nil
}
// GetSize queries the operating system kernel for the exact current byte size of the WAL file
func (w *WAL) GetSize() (int64, error) {
	// 1. ACQUIRE LOCK: Protect file descriptor from race conditions if a write is happening
	w.mu.Lock()
	defer w.mu.Unlock() // 2. DEFER RELEASE LOCK: Will unlock automatically when this function finishes

	// 3. CALL OS HARDWARE STATS: Ask the kernel for file metadata
	stat, err := w.file.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to read file metadata from kernel: %w", err)
	}

	// 4. RETURN PROPERTY: Return the exact byte count cleanly
	return stat.Size(), nil
}


