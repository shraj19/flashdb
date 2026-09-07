package persistence

import "errors"

// Save writes the current database state to disk at the given path.
// Uses atomic write: writes to temp file, then renames.
func Save(path string) error {
	// TODO: implement
	return errors.New("not implemented")
}

// Load reads database state from disk and populates storage.
// Skips keys that have already expired.
func Load(path string) error {
	// TODO: implement
	return errors.New("not implemented")
}
