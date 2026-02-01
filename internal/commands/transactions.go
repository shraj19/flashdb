package commands
import (
	"fmt"
	"strconv"
	"strings"

	"github.com/shraj19/flashdb/internal/storage"
)

func INCR(args []string, client *storage.Client) string {
	if len(args) != 2 {
		return "-ERR wrong number of arguments for 'incr'\r\n"
	}

	key := args[1]

	// Locking the storage for safe concurrent access
	storage.Mu.Lock()
	defer storage.Mu.Unlock()

	// Retrieving current value
	stringsMap := storage.GetStore()

	currentValue, exists := stringsMap[key]
	if !exists {
		currentValue = "0"
	}

	// Parsing current value as integer
	intValue, err := strconv.Atoi(currentValue)
	if err != nil {
		return "-ERR value is not an integer or out of range\r\n"
	}

	// Incrementing the value
	intValue++
	// Storing the new value
	stringsMap[key] = strconv.Itoa(intValue)
	
	// Responding with the new value
	return fmt.Sprintf(":%d\r\n", intValue)
}

func MULTI(args []string, client *storage.Client) string {
	if len(args) != 1 {
		return "-ERR wrong number of arguments for 'multi'\r\n"
	}

	if client.InTransaction {
		return "-ERR MULTI calls can not be nested\r\n"
	}

	client.InTransaction = true
	client.QueuedCommands = [][]string{}
	return "+OK\r\n"
}

func EXEC(args []string, client *storage.Client) string {
	if !client.InTransaction {
        return "-ERR EXEC without MULTI\r\n"
    }

	// Defer resetting the transaction state.
	// This ensures it's cleaned up whether the transaction succeeds or fails.
	defer func() {
		client.InTransaction = false
		client.QueuedCommands = nil
	}()

	// Phase 1: Pre-check commands for errors without executing them.
	// Redis aborts the transaction if there are queue-time errors (like syntax errors).
	// While we don't have a separate pre-check function, we can check for obvious errors.
	for _, cmdArgs := range client.QueuedCommands {
		cmdName := strings.ToUpper(cmdArgs[0])
		if cmdName == "DISCARD" || cmdName == "EXEC" || cmdName == "MULTI" {
			return "-ERR EXEC aborted on invalid command in transaction\r\n"
		}
		_, ok := storage.CommandMap[cmdName]
		if !ok {
			return "-ERR EXEC aborted on invalid command in transaction\r\n"
		}
	}

	// Phase 2: Execute all commands since they passed the pre-check.
	responses := []string{}
	for _, cmdArgs := range client.QueuedCommands {
		cmdName := strings.ToUpper(cmdArgs[0])
		handler := storage.CommandMap[cmdName]
		// We can assume 'ok' is true because of the pre-check loop above.
		response := handler(cmdArgs, client)
		responses = append(responses, response)
	}

	var respBuilder strings.Builder
	respBuilder.WriteString(fmt.Sprintf("*%d\r\n", len(responses)))
    for _, r := range responses {
        respBuilder.WriteString(r)
    }
	return respBuilder.String()
}

func DISCARD(args []string, client *storage.Client) string {
	if !client.InTransaction {
		return "-ERR DISCARD without MULTI\r\n"
	}

	// Reset transaction state
	client.InTransaction = false
	client.QueuedCommands = nil

	return "+OK\r\n"
}