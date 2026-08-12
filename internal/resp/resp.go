package resp

import (
	"bufio"
	"fmt"
	"io"
	"reflect"
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

func EncodeCommand(args []string) ([]byte, error) {
	elems := make([]any, len(args))
	for i, s := range args {
		elems[i] = s
	}
	return Encode(elems)
}

func Encode(v any) ([]byte, error) {
	var b strings.Builder
	if err := encodeValue(&b, v); err != nil {
		return nil, err
	}
	return []byte(b.String()), nil
}

func encodeValue(b *strings.Builder, v any) error {
	switch x := v.(type) {
	case nil:
		b.WriteString("$-1\r\n")
		return nil
	case bool:
		if x {
			b.WriteString("#t\r\n")
		} else {
			b.WriteString("#f\r\n")
		}
		return nil
	case string:
		return encodeBulkString(b, x)
	case int:
		return encodeInteger(b, int64(x))
	case int8:
		return encodeInteger(b, int64(x))
	case int16:
		return encodeInteger(b, int64(x))
	case int32:
		return encodeInteger(b, int64(x))
	case int64:
		return encodeInteger(b, x)
	case uint:
		return encodeInteger(b, int64(x))
	case uint8:
		return encodeInteger(b, int64(x))
	case uint16:
		return encodeInteger(b, int64(x))
	case uint32:
		return encodeInteger(b, int64(x))
	case uint64:
		if x > ^uint64(0)>>1 {
			return fmt.Errorf("uint64 out of int64 range")
		}
		return encodeInteger(b, int64(x))
	case []any:
		return encodeArray(b, x)
	default:
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
			elems := make([]any, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				elems[i] = rv.Index(i).Interface()
			}
			return encodeArray(b, elems)
		}
		return fmt.Errorf("unsupported type %T", v)
	}
}

func encodeBulkString(b *strings.Builder, s string) error {
	b.WriteString("$")
	b.WriteString(strconv.Itoa(len(s)))
	b.WriteString("\r\n")
	b.WriteString(s)
	b.WriteString("\r\n")
	return nil
}

func encodeInteger(b *strings.Builder, i int64) error {
	b.WriteString(":")
	b.WriteString(strconv.FormatInt(i, 10))
	b.WriteString("\r\n")
	return nil
}

func encodeArray(b *strings.Builder, arr []any) error {
	b.WriteString("*")
	b.WriteString(strconv.Itoa(len(arr)))
	b.WriteString("\r\n")
	for _, e := range arr {
		if err := encodeValue(b, e); err != nil {
			return err
		}
	}
	return nil
}
