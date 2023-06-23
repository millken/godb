package godb

import (
	"fmt"
	"math"
	"os"
	"path/filepath"

	art "github.com/WenyXu/sync-adaptive-radix-tree"
	"github.com/pkg/errors"
)

const (
	fileModePerm = 0644
	dbFileSuffix = ".db"
	maxKeySize   = math.MaxUint16
	maxValueSize = 64 * MB
)

var (
	ErrEmptyKey      = errors.New("empty key")
	ErrKeyTooLarge   = errors.New("key size is too large")
	ErrValueTooLarge = errors.New("value size is too large")
)

type DB struct {
	path     string
	opts     *option
	segments []*segment
	idx      *art.Tree[index]
}

// Open opens a database at the given path.
func Open(path string, options ...Option) (*DB, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	var segments []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var name string
		_, err := fmt.Sscanf(entry.Name(), dbFileSuffix, &name)
		if err != nil {
			continue
		}
		segments = append(segments, name)
	}
	db := &DB{
		path: path,
	}

	// blockNumber := uint32(offset / blockSize)
	// blockOffset := offset % blockSize

	return db, nil
}

// activeSegment returns the last segment.
func (db *DB) activeSegment() *segment {
	if len(db.segments) == 0 {
		return nil
	}
	return db.segments[len(db.segments)-1]
}

// Put put the value of the key to the db
func (db *DB) Put(key, value []byte) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if len(value) > int(maxValueSize) {
		return ErrValueTooLarge
	}
	return db.set(key, value)
}

func (db *DB) createSegment() (*segment, error) {

	// Generate a new sequential segment identifier.
	var id uint16
	if len(db.segments) > 0 {
		id = db.segments[len(db.segments)-1].ID() + 1
	}
	filename := fmt.Sprintf("%04x", id)

	// Generate new empty segment.
	segment, err := createSegment(id, filepath.Join(db.path, filename), db.idx)
	if err != nil {
		return nil, err
	}
	db.segments = append(db.segments, segment)

	return segment, nil
}

func (db *DB) Delete(key []byte) error {
	if err := validateKey(key); err != nil {
		return err
	}
	_, ok := db.idx.Search(key)
	if !ok {
		return nil
	}
	return db.set(key, nil)
}

func (db *DB) set(key, value []byte) error {
	var err error
	segment := db.activeSegment()
	if segment == nil || !segment.CanWrite(key, value) {
		if segment, err = db.createSegment(); err != nil {
			return err
		}
	}
	if err = segment.Write(key, value); err != nil {
		return err
	}

	if db.opts.fsync {
		if err := segment.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// Close releases all db resources.
func (db *DB) Close() error {
	return nil
}

func validateKey(key []byte) error {
	ks := len(key)
	if ks == 0 {
		return ErrEmptyKey
	} else if len(key) > maxKeySize {
		return ErrKeyTooLarge
	}
	return nil
}
