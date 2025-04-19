package godb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/millken/godb/internal/bio"
	art "github.com/millken/godb/internal/radixtree"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/valyala/bytebufferpool"
)

type DB struct {
	path               string
	opts               *option
	io                 bio.Bio
	buckets            *xsync.Map[uint32, *Bucket]
	def                *Bucket
	size               atomic.Int64
	mu                 sync.RWMutex
	closed             bool
	compactionInterval time.Duration
	compacting         atomic.Bool
	compactCh          chan struct{}
	stopCh             chan struct{}
	wg                 sync.WaitGroup
}

// Open opens a database at the given path.
func Open(dbpath string, options ...Option) (*DB, error) {
	opts := defaultOption()
	for _, opt := range options {
		opt(opts)
	}
	io, err := bio.NewBio(bio.Storage(opts.io), dbpath, opts.incrementSize)
	if err != nil {
		return nil, err
	}
	db := &DB{
		path:    dbpath,
		opts:    opts,
		io:      io,
		buckets: xsync.NewMap[uint32, *Bucket](),
		def: &Bucket{
			bucket: 0,
			idx:    art.New[int64](),
		},
		compactionInterval: 1 * time.Hour, // 默认1小时压缩一次
		compactCh:          make(chan struct{}),
		stopCh:             make(chan struct{}),
	}
	if err = db.loadBuckets(); err != nil {
		return nil, err
	}
	return db, nil
}

func (db *DB) loadBuckets() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	for db.size.Load() < db.io.Size() {
		var h Header
		_, err := db.io.ReadAt(h[:], db.size.Load())
		if err != nil {
			return err
		}
		if !h.isValid() {
			break
		}
		n := db.size.Load() + HeaderSize
		if h.IsBucket() {
			if h.IsDeleted() {
				continue
			}
			name := make([]byte, h.EntrySize())
			_, err = db.io.ReadAt(name, n)
			if err != nil {
				return err
			}
			id := bucketID(name)
			bucket := &Bucket{
				bucket: id,
				idx:    art.New[int64](),
			}
			db.buckets.Store(id, bucket)
		} else if h.IsKV() {
			//bucketID + keyLen
			name := make([]byte, 6)
			_, err = db.io.ReadAt(name, n)
			if err != nil {
				return err
			}
			bucketID := binary.LittleEndian.Uint32(name[:4])
			keyLen := binary.LittleEndian.Uint16(name[4:6])
			key := make([]byte, keyLen)
			_, err = db.io.ReadAt(key, n+6)
			if err != nil {
				return err
			}
			if bucketID == 0 {
				db.def.idx.Put(key, db.size.Load())
			} else {
				bucket, found := db.buckets.Load(bucketID)
				if !found {
					return errors.New("bucket not found")
				}
				bucket.idx.Put(key, db.size.Load())
			}
		}
		db.size.Add(int64(HeaderSize + h.EntrySize()))

	}
	return nil
}
func (db *DB) Put(key, value []byte) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if len(value) > MaxValueSize {
		return ErrValueTooLarge
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDatabaseNotOpen
	}
	rr := acquireKVNode()
	defer releaseKVNode(rr)
	rr.Set(0, key, value)
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	if err := rr.MarshalToBuffer(buf); err != nil {
		return err
	}
	if n, err := db.writeAt(buf.Bytes(), db.size.Load()); err != nil {
		return fmt.Errorf("write to mmap failed: %w on size %d", err, db.size.Load())
	} else if n != buf.Len() {
		return errors.New("write to mmap failed")
	}
	// 更新索引
	db.def.idx.Put(key, db.size.Load())
	// 更新大小
	db.size.Add(int64(buf.Len()))

	return db.sync()
}

func (db *DB) Get(key []byte) ([]byte, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrDatabaseNotOpen
	}
	pos, found := db.def.idx.Get(key)
	if !found {
		return nil, ErrKeyNotFound
	}
	r, err := db.ReadAt(pos)
	if err != nil {
		return nil, err
	}
	return r.Value(), nil
}

func (db *DB) Delete(key []byte) error {
	if err := validateKey(key); err != nil {
		return err
	}
	idx, ok := db.def.idx.Get(key)
	if !ok {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDatabaseNotOpen
	}
	// 更新记录
	if err := db.updateStateWithPosition(idx, StateDeleted); err != nil {
		return err
	}
	db.def.idx.Delete(key)

	return db.sync()
}

func (db *DB) sync() error {
	if db.opts.fsync {
		if err := db.io.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) updateStateWithPosition(pos int64, state State) error {
	if db.closed {
		return ErrDatabaseNotOpen
	}
	n, err := db.writeAt([]byte{byte((TypeKV & TypeMask) | (state & StateMask))}, pos)
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("write to mmap failed")
	}
	return nil
}

func (db *DB) writeAt(p []byte, off int64) (int, error) {
	if (off + int64(len(p))) > db.io.Size() {
		size := db.io.Size() + db.opts.incrementSize
		if err := db.io.Truncate(size); err != nil {
			return 0, err
		}
	}
	return db.io.WriteAt(p, off)
}

func (db *DB) Begin(writable bool) (*Tx, error) {
	tx := &Tx{
		db:       db,
		writable: writable,
	}
	tx.lock()
	if db.closed {
		tx.unlock()
		return nil, ErrDatabaseNotOpen
	}
	if writable {
		tx.buckets = art.New[*bucketNode]()
		tx.committed = art.New[*kvNode]()

	}
	return tx, nil
}

func (db *DB) writeRR(rr Node) (int, error) {
	if db.closed {
		return 0, ErrDatabaseNotOpen
	}
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	if err := rr.MarshalToBuffer(buf); err != nil {
		return 0, err
	}
	n, err := db.writeAt(buf.Bytes(), db.size.Load())
	if err != nil {
		return n, err
	}
	if n != buf.Len() {
		return n, errors.New("write to mmap failed")
	}
	db.size.Add(int64(n))
	return n, nil
}

func (db *DB) ReadAt(offset int64) (Node, error) {
	if db.closed {
		return nil, ErrDatabaseNotOpen
	}
	if offset < 0 || offset >= db.size.Load() {
		return nil, errors.New("offset out of range")
	}
	var h Header
	_, err := db.io.ReadAt(h[:], offset)
	if err != nil {
		return nil, err
	}
	if !h.isValid() {
		return nil, ErrInvalidRecord
	}
	n := offset + HeaderSize
	if h.IsBucket() {
		if h.IsDeleted() {
			return nil, nil
		}
		nameLen := h.EntrySize()
		name := make([]byte, nameLen)
		_, err = db.io.ReadAt(name, n)
		if err != nil {
			return nil, err
		}
		id := bucketID(name)
		bucket := &bucketNode{
			node: node{
				Hdr: Header{},
			},
			ID:   id,
			Name: name,
		}
		bucket.Hdr.EncodeState(TypeBucket, StateNormal)
		bucket.Hdr.SetEntrySize(uint32(len(name)))
		return bucket, nil
	}
	if h.IsKV() {
		entryLen := h.EntrySize()
		buf := make([]byte, entryLen)
		_, err = db.io.ReadAt(buf, n)
		if err != nil {
			return nil, err
		}
		bucketID := binary.LittleEndian.Uint32(buf[:4])
		keyLen := binary.LittleEndian.Uint16(buf[4:6])
		key := buf[6 : 6+keyLen]
		valueLen := binary.LittleEndian.Uint32(buf[6+keyLen : 10+keyLen])
		value := buf[10+uint32(keyLen) : 10+uint32(keyLen)+valueLen]
		checkSum := binary.LittleEndian.Uint32(buf[10+uint32(keyLen)+valueLen : 14+uint32(keyLen)+valueLen])
		rr := &kvNode{
			node: node{
				Hdr: Header{},
			},
			bucketID: bucketID,
			key:      key,
			value:    value,
			checkSum: checkSum,
		}
		rr.Hdr.EncodeState(TypeKV, StateNormal)
		rr.Hdr.SetEntrySize(uint32(len(key)+len(value)) + 14)
		return rr, nil
	}
	return nil, ErrInvalidRecord
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

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}
	db.closed = true
	if err := db.io.Close(); err != nil {
		return err
	}
	close(db.stopCh)
	return nil
}

func (db *DB) writeBucketNodeTree(tree *art.Tree[*bucketNode]) (int, error) {
	if db.closed {
		return 0, ErrDatabaseNotOpen
	}
	l := 0
	for _, val := range tree.Iter() {
		if val.Header().IsPutted() {
			n, err := db.writeRR(val)
			l += n
			if err != nil {
				return l, err
			}
		} else if val.Header().IsDeleted() {

		}
	}
	return l, nil
}

func (db *DB) writeKvNodeTree(tree *art.Tree[*kvNode]) (int, error) {
	if db.closed {
		return 0, ErrDatabaseNotOpen
	}
	l := 0
	for _, val := range tree.Iter() {
		if val.Header().IsPutted() {
			n, err := db.writeRR(val)
			l += n
			if err != nil {
				return l, err
			}
		} else {

		}
	}
	return l, nil
}

// TriggerCompaction 手动触发压缩
func (db *DB) TriggerCompaction() {
	select {
	case db.compactCh <- struct{}{}:
	default:
		// 已经有压缩任务在排队
	}
}

// Compact 执行数据库压缩操作，优化锁定策略
func (db *DB) Compact() error {
	// 确保同一时间只有一个压缩任务
	if !db.compacting.CompareAndSwap(false, true) {
		return nil
	}
	defer db.compacting.Store(false)

	return nil
}
