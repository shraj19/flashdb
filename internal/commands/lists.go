package commands

import (
	"fmt"
	"strconv"
	"time"

	"github.com/shraj19/flashdb/internal/storage"
)

func RPUSH(args []string, client *storage.Client) string {
	if len(args) < 3 {
		return "-ERR wrong number of arguments for 'rpush'\r\n"
	}

	storage.Mu.Lock()
	defer storage.Mu.Unlock()

	key := args[1]
	list, _ := storage.Lists[key].([]string)

	// Values to append
	values := args[2:]

	// First, wake up blocked waiters (FIFO order)
	delivered := 0
	for len(storage.Waiters[key]) > 0 && len(values) > 0 {
		ch := storage.Waiters[key][0]
		storage.Waiters[key] = storage.Waiters[key][1:]

		v := values[0]
		values = values[1:]
		delivered++
		ch <- [2]string{key, v}
	}

	// Whatever remains, append to list
	if len(values) > 0 {
		list = append(list, values...)
	}

	storage.Lists[key] = list

	return fmt.Sprintf(":%d\r\n", len(list)+delivered)
}

func LPUSH(args []string, client *storage.Client) string {
	if len(args) < 3 {
		return "-ERR wrong number of arguments for 'lpush'\r\n"
	}

	storage.Mu.Lock()
	defer storage.Mu.Unlock()
	key := args[1]
	list, _ := storage.Lists[key].([]string)

	// Values to prepend
	values := args[2:]
	for i, j := 0, len(values)-1; i < j; i, j = i+1, j-1 {
		values[i], values[j] = values[j], values[i]
	}

	// Wake earliest waiters first
	delivered := 0
	for len(storage.Waiters[key]) > 0 && len(values) > 0 {
		ch := storage.Waiters[key][0]
		storage.Waiters[key] = storage.Waiters[key][1:]

		v := values[0]
		values = values[1:]
		delivered++
		ch <- [2]string{key, v}
	}

	// Prepend remaining values
	if len(values) > 0 {
		list = append(values, list...)
	}

	storage.Lists[key] = list
	return fmt.Sprintf(":%d\r\n", len(list)+delivered)
}

func LRANGE(args []string, client *storage.Client) string {
	if len(args) != 4 {
		return "-ERR wrong number of arguments for 'lrange'\r\n"
	}
	key := args[1]

	start, err1 := strconv.Atoi(args[2])
	stop, err2 := strconv.Atoi(args[3])
	if err1 != nil || err2 != nil {
		return "-ERR start or stop is not an integer\r\n"
	}

	val, ok := storage.Lists[key]
	if !ok {
		return "*0\r\n"
	}

	list, ok := val.([]string)
	if !ok {
		return "*0\r\n"
	}

	if start < 0 {
		start = len(list) + start
	}
	if stop < 0 {
		stop = len(list) + stop
	}

	if start < 0 {
		start = 0
	}
	if stop < 0 {
		stop = 0
	}

	if stop >= len(list) {
		stop = len(list) - 1
	}

	if start > stop || start >= len(list) {
		return "*0\r\n"
	}
	var resp string
	resp += fmt.Sprintf("*%d\r\n", stop-start+1)
	for i := start; i <= stop; i++ {
		resp += fmt.Sprintf("$%d\r\n%s\r\n", len(list[i]), list[i])
	}
	return resp
}

func LLEN(args []string, client *storage.Client) string {
	if len(args) < 2 {
		return "-ERR wrong number of arguments for 'llen'\r\n"
	}

	storage.Mu.RLock()
	defer storage.Mu.RUnlock()

	val, ok := storage.Lists[args[1]]
	if !ok {
		return ":0\r\n"
	}

	list, ok := val.([]string)
	if !ok {
		return "-ERR wrong type for key\r\n"
	}

	return fmt.Sprintf(":%d\r\n", len(list))
}

func LPOP(args []string, client *storage.Client) string {
	if len(args) < 2 {
		return "-ERR wrong number of arguments for 'lpop'\r\n"
	}

	count := 1
	if len(args) >= 3 {
		n, err := strconv.Atoi(args[2])
		if err != nil || n < 0 {
			return "-ERR count is not a valid integer\r\n"
		}
		count = n
	}

	storage.Mu.Lock()
	defer storage.Mu.Unlock()

	val, ok := storage.Lists[args[1]]
	if !ok {
		if count > 1 {
			return "*0\r\n"
		} else {
			return "$-1\r\n"
		}
	}

	list, ok := val.([]string)
	if !ok {
		return "-ERR wrong type for key\r\n"
	}

	if len(list) == 0 {
		delete(storage.Lists, args[1])
		if count > 1 {
			return "*0\r\n"
		} else {
			return "$-1\r\n"
		}
	}
	
	if count > len(list) {
		count = len(list)
	}

	poppedElements := list[:count]

	// Format response
	var resp string
	if count == 1 {
		elem := poppedElements[0]
		resp = fmt.Sprintf("$%d\r\n%s\r\n", len(elem), elem)
	} else {
		resp += fmt.Sprintf("*%d\r\n", count)
		for _, elem := range poppedElements {
			resp += fmt.Sprintf("$%d\r\n%s\r\n", len(elem), elem)
		}
	}

	// Trim list
	list = list[count:]
	if len(list) == 0 {
		delete(storage.Lists, args[1])
	} else {
		storage.Lists[args[1]] = list
	}
	return resp
}

func BLPOP(args []string, client *storage.Client) string {
	if len(args) < 3 {
		return "-ERR wrong number of arguments for 'blpop'\r\n"
	}

	key := args[1]
	timeoutSec, err := strconv.ParseFloat(args[2], 64)
	if err != nil {
		return "-ERR timeout is not a float\r\n"
	}

	storage.Mu.Lock()

	val, ok := storage.Lists[key]
	if ok {
		list, ok := val.([]string)
		if ok && len(list) > 0 {
			elem := list[0]
			list = list[1:]
			storage.Lists[key] = list
			storage.Mu.Unlock()
			return fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
				len(key), key, len(elem), elem)
		}
	}

	// no element available in list, block with timeout
	waitCh := make(chan [2]string, 1)
	storage.Waiters[key] = append(storage.Waiters[key], waitCh)
	storage.Mu.Unlock()

	var timeout <-chan time.Time
	if timeoutSec > 0 {
		timeout = time.After(time.Duration(timeoutSec * float64(time.Second)))
	}

	select {
	case kv := <-waitCh:
		client.Conn.Write([]byte(fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
			len(kv[0]), kv[0], len(kv[1]), kv[1])))
	case <-timeout:
		client.Conn.Write([]byte("*-1\r\n"))
	}

	storage.Mu.Lock()
	ws := storage.Waiters[key]
	for i, w := range ws {
		if w == waitCh {
			storage.Waiters[key] = append(ws[:i], ws[i+1:]...)
			break
		}
	}
	if len(storage.Waiters[key]) == 0 {
		delete(storage.Waiters, key)
	}
	storage.Mu.Unlock()
	return "" // Response is sent asynchronously
}
