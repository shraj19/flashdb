package commands

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/shraj19/flashdb/internal/storage"
)

func SETvalue(args []string, client *storage.Client) string {
	if len(args) < 3 {
		return "-ERR wrong number of arguments for 'set'\r\n"
	}

	key, value := args[1], args[2]
	storage.Mu.Lock()
	defer storage.Mu.Unlock()
	storage.Store[key] = value

	if len(args) >= 5 {
		switch strings.ToUpper(args[3]) {
		case "EX":
			seconds, err := strconv.Atoi(args[4])
			if err != nil {
				return "-ERR invalid expire time\r\n"
			}
			storage.Expiry[key] = time.Now().Add(time.Duration(seconds) * time.Second)
		case "PX":
			ms, err := strconv.Atoi(args[4])
			if err != nil {
				return "-ERR invalid expire time\r\n"
			}
			storage.Expiry[key] = time.Now().Add(time.Duration(ms) * time.Millisecond)
		default:
			return "-ERR syntax error\r\n"
		}

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
