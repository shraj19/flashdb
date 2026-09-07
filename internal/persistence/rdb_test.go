package persistence

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/shraj19/flashdb/internal/storage"
)

func resetStorage() {
	storage.Store = make(map[string]string)
	storage.Expiry = make(map[string]time.Time)
	storage.Lists = make(map[string]any)
	storage.Streams = make(map[string]*storage.Stream)
	storage.Mu = sync.RWMutex{}
}

func TestSaveAndLoadStrings(t *testing.T) {
	testCases := []struct {
		name     string
		keys     map[string]string
		checkSet []string // keys that should exist after load
		checkNot []string // keys that should NOT exist after load
	}{
		{
			name: "basic key-value pairs",
			keys: map[string]string{
				"name":    "shashank",
				"project": "flashdb",
				"counter": "42",
			},
			checkSet: []string{"name", "project", "counter"},
			checkNot: []string{"random_key", "never_set", "foo"},
		},
		{
			name: "empty values",
			keys: map[string]string{
				"empty": "",
				"space": " ",
			},
			checkSet: []string{"empty", "space"},
			checkNot: []string{"nonempty"},
		},
		{
			name: "special characters in values",
			keys: map[string]string{
				"newline": "hello\nworld",
				"tab":     "col1\tcol2",
				"binary":  "\x00\x01\x02",
			},
			checkSet: []string{"newline", "tab", "binary"},
			checkNot: []string{"normal"},
		},
		{
			name: "unicode keys and values",
			keys: map[string]string{
				"hindi":  "नमस्ते",
				"emoji":  "🚀💾",
				"chinese": "你好",
			},
			checkSet: []string{"hindi", "emoji", "chinese"},
			checkNot: []string{"english"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resetStorage()
			testFile := fmt.Sprintf("test_%s.rdb", tc.name)
			defer os.Remove(testFile)

			// Set keys
			for k, v := range tc.keys {
				storage.Store[k] = v
			}

			// Save
			err := Save(testFile)
			if err != nil {
				t.Fatalf("Save failed: %v", err)
			}

			// Clear and reload
			resetStorage()
			err = Load(testFile)
			if err != nil {
				t.Fatalf("Load failed: %v", err)
			}

			// Verify keys that should exist
			for _, k := range tc.checkSet {
				expected := tc.keys[k]
				if got := storage.Store[k]; got != expected {
					t.Errorf("key %q: expected %q, got %q", k, expected, got)
				}
			}

			// Verify keys that should NOT exist
			for _, k := range tc.checkNot {
				if _, exists := storage.Store[k]; exists {
					t.Errorf("key %q should not exist", k)
				}
			}
		})
	}
}

func TestSaveAndLoadWithTTL(t *testing.T) {
	testCases := []struct {
		name        string
		key         string
		value       string
		ttlOffset   time.Duration // positive = future, negative = past
		shouldExist bool
		shouldHaveTTL bool
	}{
		{
			name:        "future TTL preserved",
			key:         "temp",
			value:       "expires_later",
			ttlOffset:   1 * time.Hour,
			shouldExist: true,
			shouldHaveTTL: true,
		},
		{
			name:        "expired TTL not loaded",
			key:         "old",
			value:       "should_not_load",
			ttlOffset:   -1 * time.Hour,
			shouldExist: false,
			shouldHaveTTL: false,
		},
		{
			name:        "no TTL stays without TTL",
			key:         "permanent",
			value:       "forever",
			ttlOffset:   0, // 0 means no TTL
			shouldExist: true,
			shouldHaveTTL: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resetStorage()
			testFile := fmt.Sprintf("test_ttl_%s.rdb", tc.name)
			defer os.Remove(testFile)

			// Set key
			storage.Store[tc.key] = tc.value
			if tc.ttlOffset != 0 {
				storage.Expiry[tc.key] = time.Now().Add(tc.ttlOffset)
			}

			// Save
			err := Save(testFile)
			if err != nil {
				t.Fatalf("Save failed: %v", err)
			}

			// Clear and reload
			resetStorage()
			err = Load(testFile)
			if err != nil {
				t.Fatalf("Load failed: %v", err)
			}

			// Check existence
			_, exists := storage.Store[tc.key]
			if exists != tc.shouldExist {
				t.Errorf("key existence: expected %v, got %v", tc.shouldExist, exists)
			}

			// Check TTL
			_, hasTTL := storage.Expiry[tc.key]
			if hasTTL != tc.shouldHaveTTL {
				t.Errorf("TTL existence: expected %v, got %v", tc.shouldHaveTTL, hasTTL)
			}
		})
	}
}

func TestLoadNonExistentFile(t *testing.T) {
	resetStorage()

	err := Load("does_not_exist.rdb")
	if err == nil {
		t.Errorf("expected error for non-existent file")
	}

	if len(storage.Store) != 0 {
		t.Errorf("storage should remain empty, got %d keys", len(storage.Store))
	}
}

func TestSaveEmptyDatabase(t *testing.T) {
	resetStorage()
	testFile := "test_empty.rdb"
	defer os.Remove(testFile)

	err := Save(testFile)
	if err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	err = Load(testFile)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if len(storage.Store) != 0 {
		t.Errorf("storage should be empty, got %d keys", len(storage.Store))
	}
}

func TestSaveAtomicWrite(t *testing.T) {
	resetStorage()
	testFile := "test_atomic.rdb"
	defer os.Remove(testFile)

	storage.Store["key"] = "value"

	err := Save(testFile)
	if err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Temp file should be cleaned up
	if _, err := os.Stat(testFile + ".tmp"); !os.IsNotExist(err) {
		t.Errorf("temp file should not exist after save")
	}

	// Final file should exist
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		t.Errorf("final file should exist")
	}
}

func TestKeyCountBulk(t *testing.T) {
	counts := []int{1, 10, 100, 1000}

	for _, count := range counts {
		t.Run(fmt.Sprintf("%d_keys", count), func(t *testing.T) {
			resetStorage()
			testFile := fmt.Sprintf("test_count_%d.rdb", count)
			defer os.Remove(testFile)

			// Add keys
			for i := 0; i < count; i++ {
				storage.Store[fmt.Sprintf("key%d", i)] = fmt.Sprintf("value%d", i)
			}

			err := Save(testFile)
			if err != nil {
				t.Fatalf("Save failed: %v", err)
			}

			resetStorage()

			err = Load(testFile)
			if err != nil {
				t.Fatalf("Load failed: %v", err)
			}

			if len(storage.Store) != count {
				t.Errorf("expected %d keys, got %d", count, len(storage.Store))
			}
		})
	}
}

func TestFileFormatValidation(t *testing.T) {
	resetStorage()

	testCases := []struct {
		name    string
		content []byte
		wantErr bool
	}{
		{
			name:    "empty file",
			content: []byte{},
			wantErr: true,
		},
		{
			name:    "wrong magic",
			content: []byte("WRONG1"),
			wantErr: true,
		},
		{
			name:    "truncated header",
			content: []byte("FLA"),
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testFile := fmt.Sprintf("test_invalid_%s.rdb", tc.name)
			defer os.Remove(testFile)

			err := os.WriteFile(testFile, tc.content, 0644)
			if err != nil {
				t.Fatalf("failed to create test file: %v", err)
			}

			resetStorage()
			err = Load(testFile)

			if tc.wantErr && err == nil {
				t.Errorf("expected error for %s", tc.name)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error for %s: %v", tc.name, err)
			}
		})
	}
}

func TestLargeValues(t *testing.T) {
	resetStorage()
	testFile := "test_large.rdb"
	defer os.Remove(testFile)

	// 1MB value
	largeValue := make([]byte, 1024*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	storage.Store["large"] = string(largeValue)

	err := Save(testFile)
	if err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	resetStorage()

	err = Load(testFile)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if storage.Store["large"] != string(largeValue) {
		t.Errorf("large value corrupted after save/load")
	}
}
