package commands

import (
	"fmt"

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