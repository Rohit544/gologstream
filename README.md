# GoLogStream

A high-performance, crash-resilient Distributed Write-Ahead Log (WAL) Pub/Sub Engine built entirely from scratch in Go. This engine bypasses heavy HTTP/JSON abstractions in favor of raw TCP sockets and low-level binary framing protocols, achieving ultra-low latency data persistence.

## 🏗️ System Architecture & Data Layer

GoLogStream uses a classic append-only Write-Ahead Log topology, utilizing an explicit operating system boundary control loop via the `fsync` system call to guarantee data durability on physical disk storage hardware.

### 💾 Custom Binary Framing Protocol

Every message streamed over the TCP socket is framed with a **4-byte Big-Endian Unsigned Integer Header** indicating the message payload length, preventing data bleeding and memory leaks during high-throughput concurrent processing loops:

```text
+-----------------------+-----------------------------------------------+
|  Length Prefix (4B)   |           Data Payload (Variable Length)      |
|  [uint32 Big-Endian]  |           [Raw Raw Message Bytes]             |
+-----------------------+-----------------------------------------------+

Zero Memory Leak Parsing: The storage engine decodes the binary header first, allocating a precise execution memory buffer to stream the text payload directly.$O(1)$ Disk Operations: Writing to the log is a constant-time append operation. Streaming historical streams utilizes sequential disk pointer seeks (Seek(offset, 0)), avoiding full table/file scans.🚀 Quick Start Guide1. Boot the Server Engine NodeClone the repository and spin up the TCP server daemon (binds to port :8080):Bashgo run main.go
2. Stream Transactions (Producer Interface)Connect to the bare-metal socket using netcat or any TCP client:Bashnc localhost 8080
Type any transaction string to lock it onto the physical disk blocks:PlaintextORDER_PLACED: User_101 bought Item_A for 2500 INR
The engine will instantly respond with a durable commit tracking acknowledgement:PlaintextACK: Durable commit locked at disk offset 76
3. Historical Replay (Consumer Interface)To simulate consumer catch-up or system recovery, pass the custom execution command followed by your targeted byte offset checkpoint address:PlaintextCONSUME 76
🛠️ Production Engineering FeaturesBare-Metal Concurrency: Utilizes structured Goroutine pools to isolate incoming blocking network socket traffic cleanly.Hardware Durability: Leverages raw Go os.File.Sync() tracking blocks to bypass volatile operating system page caches, ensuring records survive sudden power grid failures.Dynamic Slicing: Avoids reading unnecessary historical lines into RAM by executing raw kernel-level disk heads pointer manipulations.
