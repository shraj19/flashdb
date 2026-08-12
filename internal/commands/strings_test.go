package commands

import (
	"bufio"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shraj19/flashdb/internal/resp"
	"github.com/shraj19/flashdb/internal/storage"
)

func TestSETGETRoundTrip(t *testing.T) {
	storage.Store = make(map[string]string)
	storage.Expiry = make(map[string]time.Time)
	storage.Lists = make(map[string]any)
	storage.Streams = make(map[string]*storage.Stream)
	storage.StreamWaiters = make(map[string][]*storage.StreamWaiter)
	storage.Waiters = make(map[string][]chan [2]string)
	storage.Mu = sync.RWMutex{}

	client := &storage.Client{}
	response := SETvalue([]string{"SET", "name", "flash"}, client)
	if response != "+OK\r\n" {
		t.Fatalf("expected SET response +OK, got %q", response)
	}

	value := GETvalue([]string{"GET", "name"}, client)
	if value != "$5\r\nflash\r\n" {
		t.Fatalf("expected GET response for stored value, got %q", value)
	}
}

func TestGETMissingKeyReturnsNilBulkString(t *testing.T) {
	storage.Store = make(map[string]string)
	storage.Expiry = make(map[string]time.Time)
	storage.Lists = make(map[string]any)
	storage.Streams = make(map[string]*storage.Stream)
	storage.StreamWaiters = make(map[string][]*storage.StreamWaiter)
	storage.Waiters = make(map[string][]chan [2]string)
	storage.Mu = sync.RWMutex{}

	client := &storage.Client{}
	response := GETvalue([]string{"GET", "missing"}, client)
	if response != "$-1\r\n" {
		t.Fatalf("expected nil bulk string response, got %q", response)
	}
}

func TestINCRIncrementsValue(t *testing.T) {
	storage.Store = make(map[string]string)
	storage.Expiry = make(map[string]time.Time)
	storage.Lists = make(map[string]any)
	storage.Streams = make(map[string]*storage.Stream)
	storage.StreamWaiters = make(map[string][]*storage.StreamWaiter)
	storage.Waiters = make(map[string][]chan [2]string)
	storage.Mu = sync.RWMutex{}

	client := &storage.Client{}
	SETvalue([]string{"SET", "counter", "1"}, client)
	response := INCR([]string{"INCR", "counter"}, client)

	if response != ":2\r\n" {
		t.Fatalf("expected INCR response :2, got %q", response)
	}
}

func TestREPLCONFRegistersReplicaAndReturnsOK(t *testing.T) {
	storage.Replication = &storage.ReplicationState{
		Role:    "master",
		ReplID:  "test-repl-id",
		Offset:  0,
		Replicas: make(map[string]*storage.ReplicaConnection),
	}

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	client := &storage.Client{Conn: clientConn}
	response := REPLCONF([]string{"REPLCONF", "listening-port", "6380"}, client)
	if response != "+OK\r\n" {
		t.Fatalf("expected REPLCONF response +OK, got %q", response)
	}

	storage.Replication.Mu.RLock()
	_, ok := storage.Replication.Replicas[clientConn.RemoteAddr().String()]
	storage.Replication.Mu.RUnlock()
	if !ok {
		t.Fatalf("expected replica connection to be registered")
	}
}

func TestPSYNCWritesFullResyncFrame(t *testing.T) {
	storage.Replication = &storage.ReplicationState{
		Role:     "master",
		ReplID:   "test-repl-id",
		Offset:   0,
		Replicas: make(map[string]*storage.ReplicaConnection),
	}

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	lineCh := make(chan string, 1)
	go func() {
		reader := bufio.NewReader(serverConn)
		line, err := reader.ReadString('\n')
		if err == nil {
			lineCh <- line
		}
		_, _ = io.Copy(io.Discard, reader)
	}()

	client := &storage.Client{Conn: clientConn}
	PSYNC([]string{"PSYNC", "?", "-1"}, client)

	line := <-lineCh
	if !strings.HasPrefix(line, "+FULLRESYNC") {
		t.Fatalf("expected FULLRESYNC handshake, got %q", line)
	}
}

func TestWAITReturnsZeroWithoutReplicas(t *testing.T) {
	storage.Replication = &storage.ReplicationState{
		Role:    "master",
		ReplID:  "test-repl-id",
		Offset:  0,
		Replicas: make(map[string]*storage.ReplicaConnection),
	}

	response := WAIT([]string{"WAIT", "0", "100"}, nil)
	if response != ":0\r\n" {
		t.Fatalf("expected WAIT response :0 when no replicas, got %q", response)
	}
}

func TestSETPropagatesToReplicaOverRESPStream(t *testing.T) {
	storage.Store = make(map[string]string)
	storage.Expiry = make(map[string]time.Time)
	storage.Lists = make(map[string]any)
	storage.Streams = make(map[string]*storage.Stream)
	storage.StreamWaiters = make(map[string][]*storage.StreamWaiter)
	storage.Waiters = make(map[string][]chan [2]string)
	storage.Mu = sync.RWMutex{}
	storage.Replication = &storage.ReplicationState{
		Role:     "master",
		ReplID:   "test-repl-id",
		Offset:   0,
		Replicas: make(map[string]*storage.ReplicaConnection),
	}

	masterConn, replicaConn := net.Pipe()
	defer masterConn.Close()
	defer replicaConn.Close()

	resultCh := make(chan []string, 1)
	errCh := make(chan error, 1)
	go func() {
		incoming, err := resp.ParseRESP(bufio.NewReader(replicaConn))
		if err != nil {
			errCh <- err
			return
		}
		resultCh <- incoming
	}()

	storage.Replication.Mu.Lock()
	storage.Replication.Replicas[masterConn.RemoteAddr().String()] = &storage.ReplicaConnection{
		Conn:      masterConn,
		Connected: true,
	}
	storage.Replication.Mu.Unlock()

	response := SETvalue([]string{"SET", "greeting", "hello"}, &storage.Client{})
	if response != "+OK\r\n" {
		t.Fatalf("expected SET response +OK, got %q", response)
	}

	var (
		incoming []string
		err      error
	)
	select {
	case incoming = <-resultCh:
	case err = <-errCh:
		t.Fatalf("expected replica to receive a RESP command: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for replica to receive propagated RESP command")
	}

	if len(incoming) != 3 {
		t.Fatalf("expected 3 RESP elements for SET, got %d: %#v", len(incoming), incoming)
	}
	if incoming[0] != "SET" || incoming[1] != "greeting" || incoming[2] != "hello" {
		t.Fatalf("expected SET greeting hello, got %#v", incoming)
	}

	if storage.Replication.Offset <= 0 {
		t.Fatalf("expected propagation to increase master replication offset, got %d", storage.Replication.Offset)
	}
}
