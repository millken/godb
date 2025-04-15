package godb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-mmap/mmap"
	art "github.com/millken/godb/internal/radixtree"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/valyala/bytebufferpool"
)

type dB struct {
	path               string
	opts               *option
	mmap               *mmap.File
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
func OpendB(dbpath string, options ...Option) (*dB, error) {
	fi, err := os.OpenFile(dbpath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	ret, err := fi.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	if ret == 0 {
		// 如果文件为空，写入文件头
		if err = fi.Truncate(32 * MB); err != nil {
			return nil, err
		}
	}
	if err = fi.Close(); err != nil {
		return nil, err
	}
	// 重新打开文件
	mmapFile, err := mmap.OpenFile(dbpath, mmap.Read|mmap.Write)
	if err != nil {
		return nil, err
	}
	opts := defaultOption()
	for _, opt := range options {
		opt(opts)
	}
	db := &dB{
		path:    dbpath,
		opts:    opts,
		mmap:    mmapFile,
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

func (db *dB) loadBuckets() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	for db.size.Load() < int64(db.mmap.Len()) {
		var h Header
		_, err := db.mmap.ReadAt(h[:], db.size.Load())
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
			_, err = db.mmap.ReadAt(name, n)
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
			_, err = db.mmap.ReadAt(name, n)
			if err != nil {
				return err
			}
			bucketID := binary.LittleEndian.Uint32(name[:4])
			keyLen := binary.LittleEndian.Uint16(name[4:6])
			key := make([]byte, keyLen)
			_, err = db.mmap.ReadAt(key, n+6)
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
func (db *dB) Put(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDatabaseClosed
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
	if db.opts.fsync {
		if err := db.mmap.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func (db *dB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrDatabaseClosed
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

func (db *dB) Delete(key []byte) error {
	idx, ok := db.def.idx.Get(key)
	if !ok {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	// 更新记录
	if err := db.updateStateWithPosition(idx, StateDeleted); err != nil {
		return err
	}
	db.def.idx.Delete(key)
	return nil
}

func (db *dB) updateStateWithPosition(pos int64, state State) error {
	if db.closed {
		return ErrDatabaseClosed
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

func (db *dB) writeAt(p []byte, off int64) (int, error) {
	if (int(off) + len(p)) > db.mmap.Len() {
		size := db.mmap.Len() + 32*MB
		var err error
		if err = db.mmap.Sync(); err != nil {
			return 0, err
		}
		if err = db.mmap.Close(); err != nil {
			return 0, err
		}
		if err = os.Truncate(db.path, int64(size)); err != nil {
			return 0, err
		}
		if db.mmap, err = mmap.OpenFile(db.path, mmap.Read|mmap.Write); err != nil {
			return 0, err
		}
	}
	return db.mmap.WriteAt(p, off)
}

func (db *dB) Begin(writable bool) (*Transaction, error) {
	tx := &Transaction{
		db:       db,
		writable: writable,
	}
	tx.lock()
	if db.closed {
		tx.unlock()
		return nil, ErrDatabaseClosed
	}
	if writable {
		tx.buckets = art.New[*bucketNode]()
		tx.committed = art.New[*kvNode]()

	}
	return tx, nil
}

func (db *dB) Set(key, value []byte) error {
	return db.setWithBucket(0, key, value)
}

func (db *dB) writeRR(rr Node) (int, error) {
	if db.closed {
		return 0, ErrDatabaseClosed
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

func (db *dB) ReadAt(offset int64) (Node, error) {
	if db.closed {
		return nil, ErrDatabaseClosed
	}
	if offset < 0 || offset >= db.size.Load() {
		return nil, errors.New("offset out of range")
	}
	var h Header
	_, err := db.mmap.ReadAt(h[:], offset)
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
		name := make([]byte, h.EntrySize())
		_, err = db.mmap.ReadAt(name, n)
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
		//bucketID + keyLen
		buf := make([]byte, h.EntrySize())
		_, err = db.mmap.ReadAt(buf, n)
		if err != nil {
			return nil, err
		}
		bucketID := binary.LittleEndian.Uint32(buf[:4])
		keyLen := binary.LittleEndian.Uint16(buf[4:6])
		key := make([]byte, keyLen)
		copy(key, buf[6:6+keyLen])
		valueLen := binary.LittleEndian.Uint32(buf[6+keyLen : 10+keyLen])
		value := make([]byte, valueLen)
		copy(value, buf[10+keyLen:10+uint32(keyLen)+valueLen])
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

func (db *dB) setWithBucket(bucketID uint32, key, value []byte) error {
	rr := acquireKVNode()
	defer releaseKVNode(rr)
	rr.Set(bucketID, key, value)
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	if err := rr.MarshalToBuffer(buf); err != nil {
		return err
	}
	if n, err := db.writeAt(buf.Bytes(), int64(db.mmap.Len())); err != nil {
		return err
	} else if n != buf.Len() {
		return errors.New("write to mmap failed")
	}

	return nil
}

// View executes a function within a managed read-only transaction.
// When a non-nil error is returned from the function that error will be return
// to the caller of View().
func (db *dB) View(fn func(tx *Transaction) error) error {
	return db.managed(false, fn)
}

// Update executes a function within a managed read/write transaction.
// The transaction has been committed when no error is returned.
// In the event that an error is returned, the transaction will be rolled back.
// When a non-nil error is returned from the function, the transaction will be
// rolled back and the that error will be return to the caller of Update().
func (db *dB) Update(fn func(tx *Transaction) error) error {
	return db.managed(true, fn)
}

// managed calls a block of code that is fully contained in a transaction.
// This method is intended to be wrapped by Update and View
func (db *dB) managed(writable bool, fn func(tx *Transaction) error) (err error) {
	var tx *Transaction
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
func (db *dB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}
	db.closed = true
	if err := db.mmap.Close(); err != nil {
		return err
	}
	close(db.stopCh)
	return nil
}
func (db *dB) writeBucketNodeTree(tree *art.Tree[*bucketNode]) (int, error) {
	if db.closed {
		return 0, ErrDatabaseClosed
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
func (db *dB) writeKvNodeTree(tree *art.Tree[*kvNode]) (int, error) {
	if db.closed {
		return 0, ErrDatabaseClosed
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
