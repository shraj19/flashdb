package commands

import (
	"sync"
	"testing"
	"time"

	"github.com/shraj19/flashdb/internal/storage"
)

func resetStorage() {
	storage.Store = make(map[string]string)
	storage.Expiry = make(map[string]time.Time)
	storage.Lists = make(map[string]any)
	storage.Streams = make(map[string]*storage.Stream)
	storage.StreamWaiters = make(map[string][]*storage.StreamWaiter)
	storage.Waiters = make(map[string][]chan [2]string)
	storage.Mu = sync.RWMutex{}
}

func TestRPUSHAndLRANGE(t *testing.T) {
	resetStorage()
	client := &storage.Client{}

	response := RPUSH([]string{"RPUSH", "mylist", "a", "b"}, client)
	if response != ":2\r\n" {
		t.Fatalf("expected rpush response :2, got %q", response)
	}

	response = LRANGE([]string{"LRANGE", "mylist", "0", "-1"}, client)
	if response != "*2\r\n$1\r\na\r\n$1\r\nb\r\n" {
		t.Fatalf("expected lrange response for two items, got %q", response)
	}
}

func TestLLENReturnsZeroForMissingList(t *testing.T) {
	resetStorage()
	client := &storage.Client{}

	response := LLEN([]string{"LLEN", "missing"}, client)
	if response != ":0\r\n" {
		t.Fatalf("expected llen response :0 for missing list, got %q", response)
	}
}
