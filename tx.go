package godb

import (
	art "github.com/millken/godb/internal/radixtree"
)

type Tx struct {
	db        *DB // the underlying database.
	size      int64
	writable  bool // when false mutable operations fail.
	committed *art.Tree[*kvNode]
	buckets   *art.Tree[*bucketNode]
}

// lock locks the database based on the transaction type.
func (tx *Tx) lock() {
	if tx.writable {
		tx.db.mu.Lock()
	} else {
		tx.db.mu.RLock()
	}
}

// unlock unlocks the database based on the transaction type.
func (tx *Tx) unlock() {
	if tx.writable {
		tx.db.mu.Unlock()
	} else {
		tx.db.mu.RUnlock()
	}
}
func (tx *Tx) CreateBucket(name []byte) (*Bucket, error) {
	_, found := tx.db.buckets.Load(bucketID(name))
	if found {
		return nil, ErrBucketExists
	}
	rr := newBucket(name)
	tx.buckets.Put(name, rr)
	return &Bucket{
		tx:     tx,
		bucket: bucketID(name),
		idx:    art.New[int64](),
	}, nil
}

// Bucket returns a bucket by name. if the bucket does not exist it will be
// created and returned. The bucket is not persisted until the transaction
func (tx *Tx) Bucket(name []byte) *Bucket {
	id := bucketID(name)
	b, found := tx.db.buckets.Load(id)
	if found {
		if tx.writable {
			b.tx = tx
		}
		return b
	}
	if !tx.writable {
		return &Bucket{
			tx:     tx,
			bucket: id,
			idx:    art.New[int64](),
		}
	}
	rr := newBucket(name)
	tx.buckets.Put(name, rr)
	return &Bucket{
		tx:     tx,
		bucket: id,
		idx:    art.New[int64](),
	}
}

func (tx *Tx) OpenBucket(name []byte) (*Bucket, error) {
	b, found := tx.db.buckets.Load(bucketID(name))
	if !found {
		return nil, ErrBucketNotFound
	}
	b.tx = tx
	return b, nil
}

func (tx *Tx) DeleteBucket(name []byte) error {
	if tx.db == nil {
		return ErrTxClosed
	}
	if !tx.writable {
		return ErrTxNotWritable
	}
	id := bucketID(name)
	_, found := tx.db.buckets.Load(id)
	if !found {
		return ErrBucketNotFound
	}
	// Mark bucket as deleted in the transaction's bucket tree
	rr := newBucket(name)
	rr.Hdr.SetRecord(StateDeleted)
	tx.buckets.Put(name, rr)
	return nil
}

func (tx *Tx) Commit() error {
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		// Read-only transactions should use Rollback, not Commit.
		// Unlock and clear the transaction to prevent deadlocks.
		tx.unlock()
		tx.db = nil
		return ErrTxNotWritable
	}
	tx.size = tx.db.size.Load()
	var (
		err error
	)
	if tx.buckets.Len() > 0 {
		_, err = tx.db.writeBucketNodeTree(tx.buckets)
		if err != nil {
			tx.rollback()
			tx.unlock()
			tx.db = nil
			return err
		}
	}
	if tx.committed.Len() > 0 {
		// If this operation fails then the write did failed and we must
		// rollback.
		_, err = tx.db.writeKvNodeTree(tx.committed)
		if err != nil {
			tx.rollback()
			tx.unlock()
			tx.db = nil
			return err
		}
	}

	err = tx.db.sync()
	// Unlock the database and allow for another writable transaction.
	tx.unlock()
	// Clear the db field to disable this transaction from future use.
	tx.db = nil
	return err
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

// rollback handles the underlying rollback logic.
// Intended to be called from Commit() and Rollback().
func (tx *Tx) rollback() {
	tx.buckets = art.New[*bucketNode]()
	tx.committed = art.New[*kvNode]()
	tx.db.size.Swap(tx.size)
}

func (tx *Tx) put(bucketID uint32, key, value []byte) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if len(value) > MaxValueSize {
		return ErrValueTooLarge
	}
	rr := newKVNode()
	rr.Set(bucketID, key, value)
	tx.committed.Put(key, rr)
	return nil
}

func (tx *Tx) get(idx *art.Tree[int64], key []byte) ([]byte, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	if tx.db == nil {
		return nil, ErrTxClosed
	}
	if tx.writable {
		v, found := tx.committed.Get(key)
		if found {
			return v.Value(), nil
		}
	}

	pos, found := idx.Get(key)
	if !found {
		return nil, ErrKeyNotFound
	}
	r, err := tx.db.ReadAt(pos)
	if err != nil {
		return nil, err
	}

	return r.Value(), nil
}

func (tx *Tx) delete(bucketID uint32, idx *art.Tree[int64], key []byte) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if tx.db == nil {
		return ErrTxClosed
	}
	if !tx.writable {
		return ErrTxNotWritable
	}
	ct, found := tx.committed.Get(key)
	if found {
		if ct.Hdr.IsKV() && !ct.Hdr.IsDeleted() {
			ct.Hdr.SetRecord(StateDeleted)
		}
		return nil
	}
	_, found = idx.Get(key)
	if !found {
		return nil
	}

	rr := newKVNode()
	rr.Set(bucketID, key, nil)
	rr.Hdr.SetRecord(StateDeleted)
	tx.committed.Put(key, rr)

	// if idx.Delete(key) {
	// 	if err := tx.db.updateStateWithPosition(pos, StateDeleted); err != nil {
	// 		return err
	// 	}
	// }
	return nil
}
