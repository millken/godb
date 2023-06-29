package godb

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"

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
	mu       sync.Mutex
}

// Open opens a database at the given path.
func Open(path string, options ...Option) (*DB, error) {
	if err := os.MkdirAll(path, fileModePerm); err != nil {
		return nil, err
	}
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
	opts := defaultOption()
	for _, opt := range options {
		if err := opt(opts); err != nil {
			return nil, errors.Wrap(err, "Invalid option")
		}
	}
	db := &DB{
		path: path,
		opts: opts,
		idx:  &art.Tree[index]{},
	}
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

// Get gets the value of the key from the db
func (db *DB) Get(key []byte) ([]byte, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	idx, found := db.idx.Search(key)
	if !found {
		return nil, ErrKeyNotFound
	}
	return db.get(idx)
}

// get gets the value of the index from the db
func (db *DB) get(idx index) ([]byte, error) {
	var segment *segment
	for _, seg := range db.segments {
		if seg.ID() == idx.segment() {
			segment = seg
			break
		}
	}
	return segment.ReadAt(idx.offset())
}

func (db *DB) createSegment() (*segment, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	// Generate a new sequential segment identifier.
	var id uint16
	if len(db.segments) > 0 {
		id = db.segments[len(db.segments)-1].ID() + 1
	}
	filename := fmt.Sprintf("%04x%s", id, dbFileSuffix)

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
	if segment == nil || !segment.CanWrite() {
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
	var err error
	for _, s := range db.segments {
		if e := s.Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
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
