package commands

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/shraj19/flashdb/internal/resp"
	"github.com/shraj19/flashdb/internal/storage"
)

func SETvalue(args []string, client *storage.Client) string {
	if len(args) < 3 {
		return "-ERR wrong number of arguments for 'set'\r\n"
	}

	key, value := args[1], args[2]
	storage.Mu.Lock()
	storage.Store[key] = value
	if len(args) >= 5 {
		switch strings.ToUpper(args[3]) {
		case "EX":
			seconds, err := strconv.Atoi(args[4])
			if err != nil {
				storage.Mu.Unlock()
				return "-ERR invalid expire time\r\n"
			}
			storage.Expiry[key] = time.Now().Add(time.Duration(seconds) * time.Second)
		case "PX":
			ms, err := strconv.Atoi(args[4])
			if err != nil {
				storage.Mu.Unlock()
				return "-ERR invalid expire time\r\n"
			}
			storage.Expiry[key] = time.Now().Add(time.Duration(ms) * time.Millisecond)
		default:
			storage.Mu.Unlock()
			return "-ERR syntax error\r\n"
		}
	}
	storage.Mu.Unlock()

	if encoded, err := resp.EncodeCommand(args); err == nil {
		replication := storage.EnsureReplication()
		replication.IncreaseOffset(int64(len(encoded)))
		replication.Mu.RLock()
		for _, rep := range replication.Replicas {
			if rep != nil && rep.Connected && rep.Conn != nil {
				_, _ = rep.Conn.Write(encoded)
			}
		}
		replication.Mu.RUnlock()
	}
	return "+OK\r\n"
}

func GETvalue(args []string, client *storage.Client) string {
	if len(args) < 2 {
		return "-ERR wrong number of arguments for 'get'\r\n"
	}

	key := args[1]

	storage.Mu.RLock()
	exp, hasExpiry := storage.Expiry[key]
	value, ok := storage.Store[key]
	storage.Mu.RUnlock()

	if hasExpiry && time.Now().After(exp) {
		storage.Mu.Lock()
		delete(storage.Store, key)
		delete(storage.Expiry, key)
		storage.Mu.Unlock()
		return "$-1\r\n"
	}

	if !ok {
		return "$-1\r\n"
	} else {
		return fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
	}

}

func TYPE(args []string, client *storage.Client) string {
	if len(args)!=2 {
		return "-ERR wrong number of arguments for 'type'\r\n"
	}

	key := args[1]
	storage.Mu.RLock()
	exp, hasExpiry := storage.Expiry[key]
	_, ok := storage.Store[key]
	storage.Mu.RUnlock()

	if hasExpiry && time.Now().After(exp) {
		storage.Mu.Lock()
		delete(storage.Store, key)
		delete(storage.Expiry, key)
		storage.Mu.Unlock()
		return "+none\r\n"
	}

	if ok {
		return "+string\r\n"
	}
	// Checking for stream
	_, ok = storage.Streams[key]
	if ok {
		return "+stream\r\n"
	}
	return "+none\r\n"
}
