package godb

import (
	"fmt"
	"os"
	"sync"
)

const (
	fileModePerm = 0644
	dbFileSuffix = ".db"
)

type DB struct {
	dirPath string
	tables  map[string]*Table
	mu      sync.RWMutex
}

// Open opens a database at the given path.
// If the database does not exist then it will be created automatically.
// The returned DB instance must be closed after use, by calling the Close method.
// The returned DB instance is safe for concurrent use by multiple goroutines.
// options can be used to set HashFunc and fsync.
func Open(dirPath string, options ...Option) (*DB, error) {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}
	var tables []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var name string
		_, err := fmt.Sscanf(entry.Name(), dbFileSuffix, &name)
		if err != nil {
			continue
		}
		tables = append(tables, name)
	}
	db := &DB{
		dirPath: dirPath,
	}
	for _, tableName := range tables {
		tablePath := fmt.Sprintf("%s/%s%s", dirPath, tableName, dbFileSuffix)
		db.tables[tableName], _ = OpenTable(tablePath)
	}
	// blockNumber := uint32(offset / blockSize)
	// blockOffset := offset % blockSize

	return db, nil
}

func (db *DB) Use(table string) *Table {
	return nil
}

func (db *DB) DeleteTable(table string) *Table {
	return nil
}

func (db *DB) loadOrCreateTable(name string) *Table {
	db.mu.Lock()
	defer db.mu.Unlock()
	if t, ok := db.tables[name]; ok {
		return t
	}
	t := &Table{}
	db.tables[name] = t
	return t
}

// Close releases all db resources.
func (db *DB) Close() error {
	return nil
}

func (db *DB) tablePath(tableName string) string {
	return fmt.Sprintf("%s/%s%s", db.dirPath, tableName, dbFileSuffix)
}
