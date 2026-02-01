package commands

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/shraj19/flashdb/internal/storage"
	"github.com/shraj19/flashdb/internal/utilities"
)

func XADD(args []string, client *storage.Client) string {
	// Checking length of args
	if len(args) < 5 || len(args)%2 != 1 {
		return "-ERR wrong number of arguments for 'xadd'\r\n"
	}

	// Checking for key and id
	streamKey := args[1]
	id := args[2]

	// Locking the storage for safe concurrent access
	storage.Mu.Lock()
	defer storage.Mu.Unlock()

	// Retrieving or initializing the stream
	streams := *storage.GetStreams()
	stream, exists := streams[streamKey]
	if !exists {
		stream = &storage.Stream{
			Entries: []storage.StreamEntry{},
			Index:   make(map[string]int),
		}
	}

	// ---- ID Generation ----
	if id == "*" {
		ts := time.Now().UnixMilli()
		// first entry: sequence 0
		id = fmt.Sprintf("%d-0", ts)
	} else if strings.HasSuffix(id, "-*") {
		parts := strings.Split(id, "-")
		ts, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil || ts < 0 {
			return "-ERR invalid stream ID format\r\n"
		}

		if exists && len(stream.Entries) > 0 {
			lastID := stream.Entries[len(stream.Entries)-1].ID
			lastParts := strings.Split(lastID, "-")
			lastTs, _ := strconv.ParseInt(lastParts[0], 10, 64)
			lastSeq, _ := strconv.ParseInt(lastParts[1], 10, 64)

			if ts < lastTs {
				return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
			}
			if ts == lastTs {
				id = fmt.Sprintf("%d-%d", ts, lastSeq+1)
			} else {
				id = fmt.Sprintf("%d-0", ts)
			}
		} else {
			// first entry in stream
			if ts == 0 {
				id = "0-1" // never 0-0
			} else {
				id = fmt.Sprintf("%d-0", ts)
			}
		}
	}

	// ---- Validate explicit ID ----
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return "-ERR invalid stream ID format\r\n"
	}
	ts, err1 := strconv.ParseInt(parts[0], 10, 64)
	seq, err2 := strconv.ParseInt(parts[1], 10, 64)
	if err1 != nil || err2 != nil || ts < 0 || seq < 0 {
		return "-ERR invalid stream ID format\r\n"
	}

	// Special case: 0-0 is invalid
	if ts == 0 && seq == 0 {
		return "-ERR The ID specified in XADD must be greater than 0-0\r\n"
	}

	if exists && len(stream.Entries) > 0 {
		lastID := stream.Entries[len(stream.Entries)-1].ID
		if utilities.CompareIDs(id, lastID) <= 0 {
			return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
		}
	}

	// ---- Add entry ----
	entry := storage.StreamEntry{
		ID:     id,
		Fields: make(map[string]string),
	}
	for i := 3; i < len(args); i += 2 {
		entry.Fields[args[i]] = args[i+1]
	}

	stream.Entries = append(stream.Entries, entry)
	stream.Index[id] = len(stream.Entries) - 1

	if !exists {
		streams[streamKey] = stream
	}

	// ---- Notify waiters ----
	if waiters, ok := storage.StreamWaiters[streamKey]; ok {
		newID := id
		var remaining []*storage.StreamWaiter
		for _, w := range waiters {
			lastID := w.StreamIDs[streamKey]
			if utilities.CompareIDs(newID, lastID) > 0 {
				close(w.Done)
			} else {
				remaining = append(remaining, w)
			}
		}
		if len(remaining) == 0 {
			delete(storage.StreamWaiters, streamKey)
		} else {
			storage.StreamWaiters[streamKey] = remaining
		}
	}


	return fmt.Sprintf("$%d\r\n%s\r\n", len(id), id)
}

func XRANGE(args []string, client *storage.Client) string {
	if len(args) != 4 {
		return "-ERR wrong number of arguments for 'xrange'\r\n"
	}

	streamkey, startID, endID := args[1], args[2], args[3]

	storage.Mu.RLock()
	defer storage.Mu.RUnlock()

	streamsMap := *storage.GetStreams()
	stream, ok := streamsMap[streamkey]
	if !ok || len(stream.Entries) == 0 {
		return "*0\r\n"
	}

	entries := stream.Entries
	if startID == "-" {
		startID = entries[0].ID
	}
	if endID == "+" {
		endID = entries[len(entries)-1].ID
	}

	// Binary search
	lo, hi := 0, len(entries)
	for lo < hi {
		mid := (lo + hi) / 2
		if utilities.CompareIDs(entries[mid].ID, startID) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	startIdx := lo

	lo, hi = 0, len(entries)
	for lo < hi {
		mid := (lo + hi) / 2
		if utilities.CompareIDs(entries[mid].ID, endID) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	endIdx := lo - 1

	if startIdx > endIdx {
		return "*0\r\n"
	}

	count := endIdx - startIdx + 1
	var respBuilder strings.Builder
	respBuilder.WriteString(fmt.Sprintf("*%d\r\n", count))
	for i := startIdx; i <= endIdx; i++ {
		entry := entries[i]
		respBuilder.WriteString(fmt.Sprintf("*2\r\n$%d\r\n%s\r\n*%d\r\n", len(entry.ID), entry.ID, len(entry.Fields)*2))
		for k, v := range entry.Fields {
			respBuilder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n$%d\r\n%s\r\n", len(k), k, len(v), v))
		}
	}
	return respBuilder.String()
}

func XREAD(args []string, client *storage.Client) string {
	//  Checking length of args
	if len(args) < 4 {
		return "-ERR wrong number of arguments for 'xread'\r\n"
	}

	blocktime := -1 // default: no blocking (i.e., return immediately)
	streamsIdx := -1
	for i := 1; i < len(args); i++ {
		if strings.ToUpper(args[i]) == "BLOCK" && i+1 < len(args) {
			bt, err := strconv.Atoi(args[i+1])
			if err != nil || bt < 0 {
				return "-ERR invalid BLOCK time\r\n"
			}
			blocktime = bt
			i++
		} else if strings.ToUpper(args[i]) == "STREAMS" {
			streamsIdx = i
			break
		}
	}
	if streamsIdx == -1 {
		return "-ERR syntax error\r\n"
	}

	// Getting stream keys and IDs
	start := streamsIdx + 1
	numKeys := (len(args) - start) / 2
	streamKeys := args[start : start+numKeys]
	streamIDs := args[start+numKeys:]
	
	// Locking the storage for safe concurrent access
	storage.Mu.Lock()
	streamsMap := *storage.GetStreams()

	// Replace "$" with actual last IDs
	for i, key := range streamKeys {
		if streamIDs[i] == "$" {
			if stream, ok := streamsMap[key]; ok && len(stream.Entries) > 0 {
				streamIDs[i] = stream.Entries[len(stream.Entries)-1].ID
			} else {
				streamIDs[i] = "0-0"
			}
		}
	}

	var responses []string
	hasEntry := false

	for i, streamKey := range streamKeys {
		startID := streamIDs[i]
		stream, ok := streamsMap[streamKey]
		var streamResp strings.Builder

		if !ok || len(stream.Entries) == 0 {
			responses = append(responses, fmt.Sprintf("*2\r\n$%d\r\n%s\r\n*0\r\n", len(streamKey), streamKey))
			continue
		}

		entries := stream.Entries
		lo, hi := 0, len(entries)
		for lo < hi {
			mid := (lo + hi) / 2
			if utilities.CompareIDs(entries[mid].ID, startID) <= 0 {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		startIdx := lo

		if startIdx >= len(entries) {
			responses = append(responses, fmt.Sprintf("*2\r\n$%d\r\n%s\r\n*0\r\n", len(streamKey), streamKey))
			continue
		}

		hasEntry = true
		entryCount := len(entries) - startIdx
		streamResp.WriteString(fmt.Sprintf("*2\r\n$%d\r\n%s\r\n*%d\r\n", len(streamKey), streamKey, entryCount))
		for j := startIdx; j < len(entries); j++ {
			e := entries[j]
			streamResp.WriteString(fmt.Sprintf("*2\r\n$%d\r\n%s\r\n*%d\r\n", len(e.ID), e.ID, len(e.Fields)*2))
			for k, v := range e.Fields {
				streamResp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n$%d\r\n%s\r\n", len(k), k, len(v), v))
			}
		}
		responses = append(responses, streamResp.String())
	}

	if hasEntry || blocktime < 0 {
		var final strings.Builder
		final.WriteString(fmt.Sprintf("*%d\r\n", len(responses)))
		for _, r := range responses {
			final.WriteString(r)
		}
		storage.Mu.Unlock()
		return final.String()
	}

	// No new entries → block
	waiter := &storage.StreamWaiter{
		Conn: client.Conn,
		Done: make(chan struct{}),
		StreamIDs: map[string]string{},
	}
	for i, key := range streamKeys {
		waiter.StreamIDs[key] = streamIDs[i]
		storage.StreamWaiters[key] = append(storage.StreamWaiters[key], waiter)
	}
	storage.Mu.Unlock()

	var timeout <-chan time.Time
	if blocktime > 0 {
		timeout = time.After(time.Duration(blocktime) * time.Millisecond)
	}

	select {
	case <-waiter.Done:
		// cleanup first
		storage.Mu.Lock()
		for _, key := range streamKeys {
			waiters := storage.StreamWaiters[key]
			for i, w := range waiters {
				if w == waiter {
					storage.StreamWaiters[key] = append(waiters[:i], waiters[i+1:]...)
					break
				}
			}
		}
		storage.Mu.Unlock()

		// Resolve '$' to last known IDs before re-invocation
		storage.Mu.RLock()
		for i, key := range streamKeys {
			if streamIDs[i] == "$" {
				if s, ok := (*storage.GetStreams())[key]; ok && len(s.Entries) > 0 {
					streamIDs[i] = s.Entries[len(s.Entries)-1].ID
				} else {
					streamIDs[i] = "0-0"
				}
			}
		}
		storage.Mu.RUnlock()

		// Now re-run with resolved IDs
		newArgs := make([]string, len(args))
		copy(newArgs, args) // This recursive call is tricky.
		client.Conn.Write([]byte(XREAD(newArgs, client)))

	case <-timeout:
		storage.Mu.Lock()
		for _, key := range streamKeys {
			waiters := storage.StreamWaiters[key]
			for i, w := range waiters {
				if w == waiter {
					storage.StreamWaiters[key] = append(waiters[:i], waiters[i+1:]...)
					break
				}
			}
		}
		storage.Mu.Unlock()
		client.Conn.Write([]byte("*-1\r\n"))
	}
	return "" // Response is sent asynchronously
}
