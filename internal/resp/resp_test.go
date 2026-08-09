package resp

import (
	"bufio"
	"strings"
	"testing"
)

func TestParseRESPInlineCommand(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("PING\r\n"))

	args, err := ParseRESP(reader)
	if err != nil {
		t.Fatalf("ParseRESP returned error: %v", err)
	}

	if len(args) != 1 || args[0] != "PING" {
		t.Fatalf("expected parsed inline command to be [PING], got %v", args)
	}
}

func TestParseRESPArrayCommand(t *testing.T) {
	payload := "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
	reader := bufio.NewReader(strings.NewReader(payload))

	args, err := ParseRESP(reader)
	if err != nil {
		t.Fatalf("ParseRESP returned error: %v", err)
	}

	expected := []string{"SET", "key", "value"}
	if len(args) != len(expected) {
		t.Fatalf("expected %d arguments, got %d", len(expected), len(args))
	}

	for i, want := range expected {
		if args[i] != want {
			t.Fatalf("expected argument %d to be %q, got %q", i, want, args[i])
		}
	}
}
