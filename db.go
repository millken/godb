package godb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/millken/godb/internal/bio"
	art "github.com/millken/godb/internal/radixtree"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/valyala/bytebufferpool"
)

type DB struct {
	path       string
	opts       *option
	io         bio.Bio
	buckets    *xsync.Map[uint32, *Bucket]
	bucket     *Bucket
	size       atomic.Int64
	mu         sync.RWMutex
	closed     bool
	compacting atomic.Bool
	compactCh  chan struct{}
	ctx        context.Context
	stopFn     context.CancelFunc
	wg         sync.WaitGroup
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
	ctx, cancel := context.WithCancel(context.Background())
	db := &DB{
		path:    dbpath,
		opts:    opts,
		io:      io,
		buckets: xsync.NewMap[uint32, *Bucket](),
		bucket: &Bucket{
			bucket: 0,
			idx:    art.New[int64](),
		},
		compactCh: make(chan struct{}),
		ctx:       ctx,
		stopFn:    cancel,
	}
	if err = db.loadBuckets(); err != nil {
		return nil, err
	}
	db.wg.Add(1)
	go db.compactionLoop()
	return db, nil
}

func (db *DB) loadBuckets() error {
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
		if h.IsDeleted() {
		} else if h.IsBucket() {

			name := make([]byte, h.EntrySize())
			_, err = db.io.ReadAt(name, n)
			if err != nil {
				return err
			}
			id := bucketID(name)
			bucket := &Bucket{
				bucket: id,
				name:   name,
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
				db.bucket.idx.Put(key, db.size.Load())
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
	// if key exists, update the state to deleted
	if pos, found := db.bucket.idx.Get(key); found {
		if err := db.updateStateWithPosition(pos, StateDeleted); err != nil {
			return err
		}
		// db.def.idx.Delete(key)
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
	db.bucket.idx.Put(key, db.size.Load())
	db.size.Add(int64(buf.Len()))

	return db.sync()
}

func (db *DB) Size() int64 {
	return db.size.Load()
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
	pos, found := db.bucket.idx.Get(key)
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
	idx, ok := db.bucket.idx.Get(key)
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
	db.bucket.idx.Delete(key)

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

func (db *DB) readHeaderAt(off int64) (Header, error) {
	if db.closed {
		return Header{}, ErrDatabaseNotOpen
	}
	if off < 0 || off >= db.size.Load() {
		return Header{}, errors.New("offset out of range")
	}
	var h Header
	_, err := db.io.ReadAt(h[:], off)
	if err != nil {
		return Header{}, err
	}
	if !h.isValid() {
		return Header{}, ErrInvalidRecord
	}
	return h, nil
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
		if h.IsDeleted() {
			return nil, ErrKeyNotFound
		}
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
	db.stopFn()
	db.wg.Wait()
	if db.closed {
		return nil
	}
	db.closed = true
	if err := db.io.Close(); err != nil {
		return err
	}
	return nil
}

func (db *DB) writeBucketNodeTree(tree *art.Tree[*bucketNode]) (int, error) {
	if db.closed {
		return 0, ErrDatabaseNotOpen
	}
	l := 0
	for _, val := range tree.Iter() {
		if val.Header().IsNormal() {
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
		if val.Header().IsNormal() {
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

// Compact Perform database compression operations, delete deleted records, and optimize storage space
func (db *DB) Compact() error {
	if !db.compacting.CompareAndSwap(false, true) {
		return nil
	}
	defer db.compacting.Store(false)

	db.mu.Lock()
	defer db.mu.Unlock()

	tmpPath := db.path + ".compact"
	defer os.Remove(tmpPath)

	var size int64
	tmpIO, err := bio.NewBio(bio.Storage(db.opts.io), tmpPath, db.opts.incrementSize)
	if err != nil {
		return fmt.Errorf("create bio failed: %w", err)
	}
	defer tmpIO.Close()

	var buckets []*Bucket
	db.buckets.Range(func(id uint32, b *Bucket) bool {
		buckets = append(buckets, b)
		return true
	})

	// 写入所有Bucket的元数据到临时文件
	for _, b := range buckets {
		bn := &bucketNode{
			ID:   b.bucket,
			Name: b.name,
		}
		bn.Hdr.EncodeState(TypeBucket, StateNormal)
		bn.Hdr.SetEntrySize(uint32(len(b.name)))

		buf := bytebufferpool.Get()
		if err = bn.MarshalToBuffer(buf); err != nil {
			bytebufferpool.Put(buf)
			return err
		}

		if _, err = tmpIO.WriteAt(buf.Bytes(), size); err != nil {
			bytebufferpool.Put(buf)
			return err
		}
		size += int64(buf.Len())
		bytebufferpool.Put(buf)
	}

	writeValidKV := func(idx *art.Tree[int64]) error {
		for _, pos := range idx.Iter() {
			var (
				n   int
				hdr Header
			)
			hdr, err = db.readHeaderAt(pos)
			if err != nil {
				return err
			}
			buf := make([]byte, hdr.EntrySize()+HeaderSize)
			n, err = db.io.ReadAt(buf, pos)
			if err != nil {
				return err
			}
			if n != int(hdr.EntrySize()+HeaderSize) {
				return fmt.Errorf("read data length mismatch: %d != %d", n, hdr.EntrySize()+HeaderSize)
			}
			if _, err = tmpIO.WriteAt(buf, size); err != nil {
				return err
			}
			size += int64(len(buf))
		}
		return nil
	}

	if err = writeValidKV(db.bucket.idx); err != nil {
		return fmt.Errorf("process default bucket failed: %w", err)
	}

	for _, b := range buckets {
		if err = writeValidKV(b.idx); err != nil {
			return fmt.Errorf("process bucket %d failed: %w", b.bucket, err)
		}
	}

	if err = tmpIO.Sync(); err != nil {
		return fmt.Errorf("sync tmp file failed: %w", err)
	}

	if err = db.io.Close(); err != nil {
		return fmt.Errorf("close original file failed: %w", err)
	}

	if err = os.Rename(tmpPath, db.path); err != nil {
		return fmt.Errorf("rename tmp file failed: %w", err)
	}

	newIO, err := bio.NewBio(bio.Storage(db.opts.io), db.path, db.opts.incrementSize)
	if err != nil {
		return fmt.Errorf("open new file failed: %w", err)
	}
	db.io = newIO

	db.size.Store(0)
	db.buckets = xsync.NewMap[uint32, *Bucket]()
	db.bucket = &Bucket{
		bucket: 0,
		idx:    art.New[int64](),
	}
	if err := db.loadBuckets(); err != nil {
		return fmt.Errorf("load buckets after compaction failed: %w", err)
	}

	return nil
}

func (db *DB) compactionLoop() {
	defer db.wg.Done()
	var ticker *time.Ticker
	var tickerC <-chan time.Time

	// Create a timer only if automatic compression is enabled
	if db.opts.compactionInterval > 0 {
		ticker = time.NewTicker(db.opts.compactionInterval)
		defer ticker.Stop()
		tickerC = ticker.C
	}

	for {
		select {
		case <-tickerC:
			if err := db.Compact(); err != nil {
				log.Printf("Compaction error: %v", err)
			}
		case <-db.compactCh:
			// triggered by user
			if err := db.Compact(); err != nil {
				log.Printf("Manual compaction error: %v", err)
			}
		case <-db.ctx.Done():
			return
		}
	}
}
