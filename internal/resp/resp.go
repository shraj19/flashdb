package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// ParseRESP parses a RESP (REdis Serialization Protocol) message.
// Supports:
// - Arrays: *<count>\r\n...
// - Bulk strings: $<length>\r\n<data>\r\n
// - Simple strings (inline): command args...
func ParseRESP(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSpace(line)

	if line == "" {
		return nil, fmt.Errorf("empty command")
	}

	// Check for array format (*count)
	if strings.HasPrefix(line, "*") {
		return parseArray(r, line)
	}

	// Inline command format (simple string)
	// Split on whitespace and return as command + args
	return strings.Fields(line), nil
}

// parseArray parses a RESP array format: *<count>\r\n$<len>\r\n<data>\r\n...
func parseArray(r *bufio.Reader, firstLine string) ([]string, error) {
	count, err := strconv.Atoi(firstLine[1:])
	if err != nil {
		return nil, fmt.Errorf("invalid array count: %v", err)
	}

	if count < 0 {
		return nil, fmt.Errorf("negative array count not allowed")
	}

	if count == 0 {
		return []string{}, nil
	}

	args := make([]string, 0, count)

	for i := 0; i < count; i++ {
		arg, err := parseBulkString(r)
		if err != nil {
			return nil, fmt.Errorf("failed to parse element %d: %v", i, err)
		}
		args = append(args, arg)
	}

	return args, nil
}

// parseBulkString parses a RESP bulk string: $<length>\r\n<data>\r\n
func parseBulkString(r *bufio.Reader) (string, error) {
	lenLine, err := r.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("failed to read bulk string length: %v", err)
	}

	lenLine = strings.TrimSpace(lenLine)

	// Must start with $
	if !strings.HasPrefix(lenLine, "$") {
		return "", fmt.Errorf("expected bulk string marker '$', got: %s", lenLine)
	}

	length, err := strconv.Atoi(lenLine[1:])
	if err != nil {
		return "", fmt.Errorf("invalid bulk string length: %v", err)
	}

	// Redis allows -1 for null bulk string
	if length == -1 {
		return "", nil
	}

	if length < 0 {
		return "", fmt.Errorf("invalid bulk string length: %d", length)
	}

	// Read exactly 'length' bytes plus CRLF
	data := make([]byte, length+2)
	n, err := io.ReadFull(r, data)
	if err != nil {
		return "", fmt.Errorf("failed to read bulk string data (expected %d, got %d): %v", length+2, n, err)
	}

	// Verify trailing CRLF
	if data[length] != '\r' || data[length+1] != '\n' {
		return "", fmt.Errorf("bulk string missing CRLF terminator")
	}

	return string(data[:length]), nil
}
