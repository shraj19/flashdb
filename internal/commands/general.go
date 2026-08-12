package commands

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/shraj19/flashdb/internal/resp"
	"github.com/shraj19/flashdb/internal/storage"
)

// PING command
func PING(args []string, client *storage.Client) string {
	return "+PONG\r\n"
}

// ECHO command
func ECHO(args []string, client *storage.Client) string {
	if len(args) < 2 {
		return "-ERR wrong number of arguments for 'echo'\r\n"
	}
	message := args[1]
	return fmt.Sprintf("$%d\r\n%s\r\n", len(message), message)
}

// INFO command
func INFO(args []string, client *storage.Client) string {
	section := "default"
	if len(args) > 1 {
		section = args[1]
	}

	var info string

	switch section {
	case "default", "server", "all":
		info = buildServerInfo()
	case "replication":
		info = buildReplicationInfo()
	default:
		return fmt.Sprintf("-ERR Unknown section '%s'\r\n", section)
	}

	// Return as bulk string
	return fmt.Sprintf("$%d\r\n%s\r\n", len(info), info)
}

func buildServerInfo() string {
	info := "# Server\r\n"
	info += "redis_version:0.0.1\r\n"
	info += "tcp_port:6379\r\n"
	return info
}

func WAIT(args []string, client *storage.Client) string {
	if len(args) < 3 {
		return "-ERR wrong number of arguments for 'wait'\r\n"
	}

	replication := storage.EnsureReplication()

	numReplicas, err := strconv.Atoi(args[1])
	if err != nil {
		return "-ERR invalid number of replicas\r\n"
	}
	timeoutMs, err := strconv.Atoi(args[2])
	if err != nil {
		return "-ERR invalid timeout\r\n"
	}

	replication.Mu.RLock()
	targetOffset := replication.Offset
	replication.Mu.RUnlock()

	replication.Mu.RLock()
	connected := 0
	for _, r := range replication.Replicas {
		if r != nil && r.Connected {
			connected++
		}
	}
	replication.Mu.RUnlock()
	if connected == 0 {
		return ":0\r\n"
	}

	if targetOffset == 0 {
		return fmt.Sprintf(":%d\r\n", connected)
	}

	replication.Mu.RLock()
	lastAsked := replication.LastAskedOffset
	replication.Mu.RUnlock()

	if targetOffset > lastAsked {
		time.Sleep(10 * time.Millisecond)
		if b, err := resp.EncodeCommand([]string{"REPLCONF", "GETACK", "*"}); err == nil {
			replication.Mu.RLock()
			conns := make([]*storage.ReplicaConnection, 0, len(replication.Replicas))
			for _, r := range replication.Replicas {
				if r != nil && r.Connected && r.Conn != nil {
					conns = append(conns, r)
				}
			}
			replication.Mu.RUnlock()
			for _, rc := range conns {
				if rc.Conn != nil {
					_, _ = rc.Conn.Write(b)
				}
			}
		}

		replication.Mu.Lock()
		replication.LastAskedOffset = targetOffset
		replication.Mu.Unlock()
	}

	deadline := time.Now().Add(time.Duration(timeoutMs) * time.Millisecond)
	for {
		acked := replication.CountReplicasAcked(targetOffset)
		if acked >= numReplicas {
			return fmt.Sprintf(":%d\r\n", acked)
		}
		if time.Now().After(deadline) {
			return fmt.Sprintf(":%d\r\n", acked)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func buildReplicationInfo() string {
	replication := storage.EnsureReplication()
	replication.Mu.RLock()
	replid := replication.ReplID
	offset := replication.Offset
	connected := 0
	for _, r := range replication.Replicas {
		if r != nil && r.Connected {
			connected++
		}
	}
	replication.Mu.RUnlock()

	if replid == "" {
		replication.Mu.Lock()
		if replication.ReplID == "" {
			replication.ReplID = fmt.Sprintf("%x", time.Now().UnixNano())
		}
		replid = replication.ReplID
		replication.Mu.Unlock()
	}

	info := "# Replication\r\n"
	info += fmt.Sprintf("role:%s\r\n", replication.Role)
	info += fmt.Sprintf("connected_slaves:%d\r\n", connected)
	info += fmt.Sprintf("master_replid:%s\r\n", replid)
	info += fmt.Sprintf("master_repl_offset:%d\r\n", offset)
	return info
}

func REPLCONF(args []string, client *storage.Client) string {
	replication := storage.EnsureReplication()
	if client != nil && client.Conn != nil {
		addr := client.Conn.RemoteAddr().String()
		replication.Mu.Lock()
		rep, ok := replication.Replicas[addr]
		if !ok {
			rep = &storage.ReplicaConnection{Conn: client.Conn, Connected: true}
			replication.Replicas[addr] = rep
		} else {
			rep.Conn = client.Conn
			rep.Connected = true
		}
		returnOK := true
		if len(args) >= 2 {
			sub := strings.ToLower(args[1])
			switch sub {
			case "listening-port":
				if len(args) >= 3 {
					rep.ListeningPort = args[2]
				}
			case "ack":
				if len(args) >= 3 {
					if v, err := strconv.ParseInt(args[2], 10, 64); err == nil {
						rep.AckOffset = v
					}
				}
				returnOK = false
			}
		}
		replication.Mu.Unlock()
		if returnOK {
			return "+OK\r\n"
		}
	}
	return ""
}

func PSYNC(args []string, client *storage.Client) string {
	replication := storage.EnsureReplication()
	replication.Mu.RLock()
	replid := replication.ReplID
	replication.Mu.RUnlock()
	if replid == "" {
		replication.Mu.Lock()
		if replication.ReplID == "" {
			replication.ReplID = fmt.Sprintf("%x", time.Now().UnixNano())
		}
		replid = replication.ReplID
		replication.Mu.Unlock()
	}

	if client != nil && client.Conn != nil {
		frame := fmt.Sprintf("+FULLRESYNC %s 0\r\n$0\r\n", replid)
		_, _ = client.Conn.Write([]byte(frame))
	}
	return ""
}