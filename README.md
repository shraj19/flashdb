# FlashDB: High-Performance Key-Value Store

A lightweight, concurrent, in-memory key-value database built in Go, implementing the RESP (Redis Serialization Protocol).

## Key Features

* **Custom Protocol Parser:** Built a raw TCP handler to parse RESP messages from scratch
* **Concurrency Safe:** Handles concurrent read/writes using `sync.RWMutex` locks for optimal performance
* **TTL Support:** Implemented active key expiration logic with time-based eviction
* **Stream Processing:** Support for Redis-like streams with consumer groups and blocking reads
* **Transaction Support:** MULTI/EXEC/DISCARD transaction handling with rollback capabilities
* **Persistent Monitoring:** Real-time metrics tracking for database health and performance

## Tech Stack

* **Go (Golang)** - Core language for high-performance concurrency
* **TCP Networking** - Low-level socket programming using `net` package
* **Docker** - Containerized deployment for portability

## Supported Commands

### String Operations
- `SET key value [EX seconds] [PX milliseconds]` - Set key with optional TTL
- `GET key` - Retrieve value by key
- `INCR key` - Increment integer value
- `DEL key` - Delete key

### List Operations
- `LPUSH key value [value ...]` - Push to list head
- `RPUSH key value [value ...]` - Push to list tail
- `LPOP key` - Pop from list head
- `RPOP key` - Pop from list tail
- `LRANGE key start stop` - Get range of elements

### Stream Operations
- `XADD stream_key * field value [field value ...]` - Add entry to stream
- `XRANGE stream_key start end` - Query stream entries
- `XREAD [BLOCK milliseconds] STREAMS stream_key [stream_key ...] id [id ...]` - Read from streams

### Transactions
- `MULTI` - Start transaction
- `EXEC` - Execute transaction
- `DISCARD` - Abort transaction

### General
- `PING` - Health check
- `ECHO message` - Echo message back
- `INFO [section]` - Server information
- `CONFIG GET parameter` - Get configuration

## How to Run

### Using Go directly
```bash
go build -o flashdb cmd/server/main.go
./flashdb
```

### Using Docker
```bash
docker build -t flashdb .
docker run -p 6379:6379 flashdb
```

### Testing with redis-cli
```bash
redis-cli -p 6379
> PING
PONG
> SET mykey "Hello FlashDB"
OK
> GET mykey
"Hello FlashDB"
```

## Architecture

```
flashdb/
├── cmd/
│   └── server/
│       └── main.go          # Entry point
├── internal/
│   ├── commands/
│   │   ├── general.go       # PING, ECHO, INFO
│   │   ├── strings.go       # SET, GET, INCR
│   │   ├── lists.go         # LPUSH, RPUSH, LPOP
│   │   ├── streams.go       # XADD, XRANGE, XREAD
│   │   └── transactions.go  # MULTI, EXEC, DISCARD
│   ├── resp/
│   │   └── resp.go          # RESP protocol parser
│   ├── server/
│   │   └── server.go        # TCP server & client handler
│   ├── storage/
│   │   └── storage.go       # In-memory data structures
│   └── utilities/
│       └── utilities.go     # Helper functions
```

## Performance Features

- **Non-blocking I/O:** Goroutine-per-connection model for concurrent client handling
- **Lock Optimization:** Read-write mutex for concurrent read operations
- **Memory Efficient:** Lazy expiration of TTL keys to reduce overhead
- **Metrics Tracking:** Built-in monitoring for keys count and active connections

## Development

```bash
# Run tests
go test ./...

# Format code
go fmt ./...

# Build binary
go build -o flashdb cmd/server/main.go
```

## License

MIT License - Feel free to use this project for learning and development.

## Author

Built with ❤️ as a learning project to understand low-level database internals and network programming in Go.
