package godb

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/dnsoa/go/assert"
)

// TestFix_TxCommitIndexUpdate verifies that after committing a transaction
// that puts KV entries into a bucket, the entries are immediately readable
// without requiring a close/reopen of the database.
func TestFix_TxCommitIndexUpdate(t *testing.T) {
	r := assert.New(t)
	f, cleanup := mustTempFile()
	defer cleanup()

	db, err := Open(f, WithStorage(File))
	r.NoError(err)
	defer db.Close()

	// Write via transaction
	err = db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucket([]byte("mybucket"))
		if err != nil {
			return err
		}
		for i := range 10 {
			key := fmt.Appendf(nil, "key%d", i)
			value := fmt.Appendf(nil, "value%d", i)
			if err = b.Put(key, value); err != nil {
				return err
			}
		}
		return nil
	})
	r.NoError(err)

	// Read back WITHOUT closing and reopening
	err = db.View(func(tx *Tx) error {
		b, err := tx.OpenBucket([]byte("mybucket"))
		if err != nil {
			return err
		}
		for i := range 10 {
			key := fmt.Appendf(nil, "key%d", i)
			value, err := b.Get(key)
			if err != nil {
				return fmt.Errorf("get key%d: %w", i, err)
			}
			expected := fmt.Appendf(nil, "value%d", i)
			r.Equal(expected, value)
		}
		return nil
	})
	r.NoError(err)
}

// TestFix_TxCommitErrorHandling verifies that when a commit fails,
// the error is properly propagated and the database remains consistent.
func TestFix_TxCommitErrorHandling(t *testing.T) {
	r := assert.New(t)
	f, cleanup := mustTempFile()
	defer cleanup()

	db, err := Open(f, WithStorage(File))
	r.NoError(err)
	defer db.Close()

	// Verify that Commit on non-writable tx returns error
	tx, err := db.Begin(false)
	r.NoError(err)
	err = tx.Commit()
	r.Equal(ErrTxNotWritable, err)

	// Verify that Commit on already committed tx returns error
	tx, err = db.Begin(true)
	r.NoError(err)
	err = tx.Commit()
	r.NoError(err)
	err = tx.Commit()
	r.Equal(ErrTxClosed, err)
}

// TestFix_DeleteRaceCondition verifies that Delete properly acquires lock
// before reading from the index.
func TestFix_DeleteRaceCondition(t *testing.T) {
	r := assert.New(t)
	f, cleanup := mustTempFile()
	defer cleanup()

	db, err := Open(f, WithStorage(File))
	r.NoError(err)
	defer db.Close()

	// Setup: put some keys
	for i := range 100 {
		key := fmt.Appendf(nil, "key%d", i)
		value := fmt.Appendf(nil, "value%d", i)
		r.NoError(db.Put(key, value))
	}

	// Run concurrent Put and Delete operations
	var wg sync.WaitGroup
	var errCount atomic.Int32

	for i := range 100 {
		wg.Add(2)
		key := fmt.Appendf(nil, "key%d", i)
		go func() {
			defer wg.Done()
			if err := db.Delete(key); err != nil {
				errCount.Add(1)
			}
		}()
		go func() {
			defer wg.Done()
			if err := db.Put(key, []byte("updated")); err != nil {
				errCount.Add(1)
			}
		}()
	}
	wg.Wait()
	r.Equal(int32(0), errCount.Load())

	// Verify database is consistent - each key should either exist with "updated" or not exist
	for i := range 100 {
		key := fmt.Appendf(nil, "key%d", i)
		value, err := db.Get(key)
		if err == nil {
			r.Equal([]byte("updated"), value)
		} else {
			r.Equal(ErrKeyNotFound, err)
		}
	}
}

// TestFix_BucketReadOnlyTx verifies that Bucket() on a read-only transaction
// does not panic when the bucket doesn't exist.
func TestFix_BucketReadOnlyTx(t *testing.T) {
	r := assert.New(t)
	f, cleanup := mustTempFile()
	defer cleanup()

	db, err := Open(f, WithStorage(File))
	r.NoError(err)
	defer db.Close()

	// Calling Bucket on a read-only tx for a non-existent bucket should not panic
	err = db.View(func(tx *Tx) error {
		b := tx.Bucket([]byte("nonexistent"))
		r.NotNil(b)
		// Getting from empty bucket should return key not found
		_, err := b.Get([]byte("somekey"))
		r.Equal(ErrKeyNotFound, err)
		return nil
	})
	r.NoError(err)
}

// TestFix_DeleteBucket verifies that DeleteBucket works correctly.
func TestFix_DeleteBucket(t *testing.T) {
	r := assert.New(t)
	f, cleanup := mustTempFile()
	defer cleanup()

	db, err := Open(f, WithStorage(File))
	r.NoError(err)
	defer db.Close()

	// Create a bucket and put some data
	err = db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucket([]byte("testbucket"))
		if err != nil {
			return err
		}
		return b.Put([]byte("key1"), []byte("value1"))
	})
	r.NoError(err)

	// Verify bucket exists
	err = db.View(func(tx *Tx) error {
		b, err := tx.OpenBucket([]byte("testbucket"))
		r.NoError(err)
		v, err := b.Get([]byte("key1"))
		r.NoError(err)
		r.Equal([]byte("value1"), v)
		return nil
	})
	r.NoError(err)

	// Delete the bucket
	err = db.Update(func(tx *Tx) error {
		return tx.DeleteBucket([]byte("testbucket"))
	})
	r.NoError(err)

	// Verify bucket is deleted
	err = db.View(func(tx *Tx) error {
		_, err := tx.OpenBucket([]byte("testbucket"))
		r.Equal(ErrBucketNotFound, err)
		return nil
	})
	r.NoError(err)

	// Verify deleting non-existent bucket returns error
	err = db.Update(func(tx *Tx) error {
		return tx.DeleteBucket([]byte("nonexistent"))
	})
	r.Equal(ErrBucketNotFound, err)
}

// TestFix_TxDefaultBucketPutGet verifies that Put/Get on the default bucket
// (bucketID 0) works correctly within transactions without close/reopen.
func TestFix_TxDefaultBucketPutGet(t *testing.T) {
	r := assert.New(t)
	f, cleanup := mustTempFile()
	defer cleanup()

	db, err := Open(f, WithStorage(File))
	r.NoError(err)
	defer db.Close()

	// Write via transaction to default bucket
	err = db.Update(func(tx *Tx) error {
		b := tx.Bucket([]byte("default"))
		return b.Put([]byte("hello"), []byte("world"))
	})
	r.NoError(err)

	// Read back without close/reopen
	err = db.View(func(tx *Tx) error {
		b, err := tx.OpenBucket([]byte("default"))
		if err != nil {
			return err
		}
		v, err := b.Get([]byte("hello"))
		if err != nil {
			return err
		}
		r.Equal([]byte("world"), v)
		return nil
	})
	r.NoError(err)
}

// TestFix_TxDeleteInBucket verifies that deleting keys within a bucket
// via a transaction works correctly when the bucket has a default bucket ID of 0.
func TestFix_TxDeleteInBucket(t *testing.T) {
	r := assert.New(t)
	f, cleanup := mustTempFile()
	defer cleanup()

	db, err := Open(f, WithStorage(File))
	r.NoError(err)
	defer db.Close()

	// Create bucket with data
	err = db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucket([]byte("testbucket"))
		if err != nil {
			return err
		}
		for i := range 5 {
			key := fmt.Appendf(nil, "key%d", i)
			value := fmt.Appendf(nil, "value%d", i)
			if err = b.Put(key, value); err != nil {
				return err
			}
		}
		return nil
	})
	r.NoError(err)

	// Delete some keys without close/reopen
	err = db.Update(func(tx *Tx) error {
		b, err := tx.OpenBucket([]byte("testbucket"))
		if err != nil {
			return err
		}
		return b.Delete([]byte("key2"))
	})
	r.NoError(err)

	// Verify deletion
	err = db.View(func(tx *Tx) error {
		b, err := tx.OpenBucket([]byte("testbucket"))
		if err != nil {
			return err
		}
		// key2 should be gone
		_, err = b.Get([]byte("key2"))
		r.Equal(ErrKeyNotFound, err)
		// other keys should still exist
		v, err := b.Get([]byte("key0"))
		r.NoError(err)
		r.Equal([]byte("value0"), v)
		return nil
	})
	r.NoError(err)
}
