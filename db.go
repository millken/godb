package godb

import (
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"

	art "github.com/millken/godb/internal/radixtree"
	"github.com/pkg/errors"
	"github.com/valyala/bytebufferpool"
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

	ErrInvalidKey     = errors.New("invalid key")
	ErrInvalidTTL     = errors.New("invalid ttl")
	ErrExpiredKey     = errors.New("key has expired")
	ErrTxClosed       = errors.New("tx closed")
	ErrDatabaseClosed = errors.New("database closed")
	ErrTxNotWritable  = errors.New("tx not writable")
)

type DB struct {
	path     string
	opts     *option
	segments []*segment
	current  *segment
	idx      *art.Tree[index]
	mu       sync.RWMutex
	closed   bool
}

// Open opens a database at the given path.
func Open(dbpath string, options ...Option) (*DB, error) {
	if err := os.MkdirAll(dbpath, 0777); err != nil {
		return nil, err
	}

	opts := defaultOption()
	for _, opt := range options {
		if err := opt(opts); err != nil {
			return nil, errors.Wrap(err, "Invalid option")
		}
	}
	db := &DB{
		path: dbpath,
		opts: opts,
		idx:  art.New[index](),
	}
	entries, err := os.ReadDir(dbpath)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if filepath.Ext(entry.Name()) == dbFileSuffix {
			segment := newSegment(0, path.Join(dbpath, entry.Name()), db.idx)
			if err := segment.Open(); err != nil {
				return nil, err
			}
			db.segments = append(db.segments, segment)
			sort.Slice(db.segments, func(i, j int) bool {
				return db.segments[i].ID() < db.segments[j].ID()
			})
		}
	}
	if len(db.segments) == 0 {
		filename := fmt.Sprintf("%04x%s", 0, dbFileSuffix)
		// Generate new empty segment.
		segment, err := createSegment(0, filepath.Join(db.path, filename), db.idx)
		if err != nil {
			return nil, err
		}
		db.segments = append(db.segments, segment)
	}
	db.current = db.segments[len(db.segments)-1]
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
	idx, found := db.idx.Get(key)
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
	_, ok := db.idx.Get(key)
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
func (db *DB) writeBatch(recs records) error {
	offset := db.current.size
	buffer := bytebufferpool.Get()
	defer bytebufferpool.Put(buffer)
	for _, rec := range recs {
		if err := rec.marshalToBuffer(buffer); err != nil {
			return err
		}
		rec.offset = offset
		rec.seg = db.current.ID()
		offset += rec.size()
	}
	if err := db.current.write(buffer.Bytes()); err != nil {
		return err
	}

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

func (db *DB) Begin(writable bool) (*Tx, error) {
	tx := &Tx{
		db:       db,
		writable: writable,
	}
	tx.lock()
	if db.closed {
		tx.unlock()
		return nil, ErrDatabaseClosed
	}
	if writable {
		tx.committed = make([]*record, 0, 3)

	}
	return tx, nil
}

// View executes a function within a managed read-only transaction.
// When a non-nil error is returned from the function that error will be return
// to the caller of View().
func (db *DB) View(fn func(tx *Tx) error) error {
	return db.managed(false, fn)
}

// Update executes a function within a managed read/write transaction.
// The transaction has been committed when no error is returned.
// In the event that an error is returned, the transaction will be rolled back.
// When a non-nil error is returned from the function, the transaction will be
// rolled back and the that error will be return to the caller of Update().
func (db *DB) Update(fn func(tx *Tx) error) error {
	return db.managed(true, fn)
}

func (tx *Tx) Rollback() error {
	if tx.db == nil {
		return ErrTxClosed
	}
	// The rollback func does the heavy lifting.
	if tx.writable {
		tx.rollback()
	}
	// unlock the database for more transactions.
	tx.unlock()
	// Clear the db field to disable this transaction from future use.
	tx.db = nil
	return nil
}

// managed calls a block of code that is fully contained in a transaction.
// This method is intended to be wrapped by Update and View
func (db *DB) managed(writable bool, fn func(tx *Tx) error) (err error) {
	var tx *Tx
	tx, err = db.Begin(writable)
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			// The caller returned an error. We must rollback.
			_ = tx.Rollback()
			return
		}
		if writable {
			// Everything went well. Lets Commit()
			err = tx.Commit()
		} else {
			// read-only transaction can only roll back.
			err = tx.Rollback()
		}
	}()
	err = fn(tx)
	return
}
