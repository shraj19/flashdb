package commands

import (
	"sync"
	"testing"
	"time"

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
