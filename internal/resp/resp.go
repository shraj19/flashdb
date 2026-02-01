package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

func ParseRESP(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSpace(line)

	if !strings.HasPrefix(line, "*") {
		// Not an array, maybe inline command like "PING"
		return []string{strings.TrimSpace(line)}, nil
	}

	n, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, fmt.Errorf("invalid array length: %v", err)
	}

	args := make([]string, 0, n)

	for i := 0; i < n; i++ {
		// Read bulk string length
		lenLine, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		lenLine = strings.TrimSpace(lenLine)
		if !strings.HasPrefix(lenLine, "$") {
			return nil, fmt.Errorf("expected bulk string, got: %s", lenLine)
		}

		size, err := strconv.Atoi(lenLine[1:])
		if err != nil {
			return nil, fmt.Errorf("invalid bulk string length: %v", err)
		}

		data := make([]byte, size+2)
		_, err = io.ReadFull(r, data)
		if err != nil {
			return nil, err
		}

		args = append(args, string(data[:size]))
	}

	return args, nil
}
