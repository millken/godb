package godb

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/dnsoa/go/assert"
)

// TestCompact_FileShrinks verifies that compaction physically shrinks the
// database file after deleting records.
func TestCompact_FileShrinks(t *testing.T) {
	r := assert.New(t)
	f, cleanup := mustTempFile()
	defer cleanup()

	db, err := Open(f, WithStorage(File), WithIncrementSize(1024), WithCompactionDisabled())
	r.NoError(err)

	// Write 100 keys to grow the file well beyond incrementSize
	for i := range 100 {
		key := fmt.Appendf(nil, "key%04d", i)
		value := fmt.Appendf(nil, "value%04d-data-padding-for-size", i)
		r.NoError(db.Put(key, value))
	}

	sizeBeforeDelete := db.Size()
	fileSizeBeforeDelete := db.io.Size()
	t.Logf("After writes: dataSize=%d, fileSize=%d", sizeBeforeDelete, fileSizeBeforeDelete)

	// Delete half the keys
	for i := range 50 {
		key := fmt.Appendf(nil, "key%04d", i)
		r.NoError(db.Delete(key))
	}

	// File size should not change after delete (just marks as deleted)
	r.Equal(fileSizeBeforeDelete, db.io.Size())

	// Compact
	r.NoError(db.Compact())

	sizeAfterCompact := db.Size()
	fileSizeAfterCompact := db.io.Size()
	t.Logf("After compaction: dataSize=%d, fileSize=%d", sizeAfterCompact, fileSizeAfterCompact)

	// Data size should decrease
	r.True(sizeAfterCompact < sizeBeforeDelete,
		"data size should decrease: %d >= %d", sizeAfterCompact, sizeBeforeDelete)

	// File size should also decrease
	r.True(fileSizeAfterCompact < fileSizeBeforeDelete,
		"file size should decrease: %d >= %d", fileSizeAfterCompact, fileSizeBeforeDelete)

	// Physical file on disk should also be smaller
	fi, err := os.Stat(f)
	r.NoError(err)
	r.True(fi.Size() < fileSizeBeforeDelete,
		"physical file should shrink: %d >= %d", fi.Size(), fileSizeBeforeDelete)

	// Verify remaining keys
	for i := 50; i < 100; i++ {
		key := fmt.Appendf(nil, "key%04d", i)
		value, err := db.Get(key)
		r.NoError(err)
		expected := fmt.Appendf(nil, "value%04d-data-padding-for-size", i)
		r.Equal(expected, value)
	}

	// Verify deleted keys stay deleted
	for i := range 50 {
		key := fmt.Appendf(nil, "key%04d", i)
		_, err := db.Get(key)
		r.Equal(ErrKeyNotFound, err)
	}

	r.NoError(db.Close())
}

// TestCompact_DataExceedsIncrementSize verifies that compaction works correctly
// when the valid data exceeds the incrementSize.
func TestCompact_DataExceedsIncrementSize(t *testing.T) {
	r := assert.New(t)
	f, cleanup := mustTempFile()
	defer cleanup()

	// Use very small increment (512 bytes) to force multiple expansions
	db, err := Open(f, WithStorage(File), WithIncrementSize(512), WithCompactionDisabled())
	r.NoError(err)

	// Write enough data to exceed incrementSize several times over
	for i := range 50 {
		key := fmt.Appendf(nil, "key%04d", i)
		value := []byte(strings.Repeat("x", 100))
		r.NoError(db.Put(key, value))
	}

	t.Logf("Before compaction: dataSize=%d, fileSize=%d", db.Size(), db.io.Size())

	// Compact with no deletes — should still work even though data > incrementSize
	r.NoError(db.Compact())

	t.Logf("After compaction: dataSize=%d, fileSize=%d", db.Size(), db.io.Size())

	// Verify all keys are still accessible
	for i := range 50 {
		key := fmt.Appendf(nil, "key%04d", i)
		value, err := db.Get(key)
		r.NoError(err)
		r.Equal([]byte(strings.Repeat("x", 100)), value)
	}

	r.NoError(db.Close())
}

// TestCompact_PersistenceAfterCompaction verifies that the database can be
// reopened after compaction and all data remains intact.
func TestCompact_PersistenceAfterCompaction(t *testing.T) {
	r := assert.New(t)
	f, cleanup := mustTempFile()
	defer cleanup()

	db, err := Open(f, WithStorage(File), WithIncrementSize(1024), WithCompactionDisabled())
	r.NoError(err)

	// Write, delete, and update some keys
	for i := range 20 {
		key := fmt.Appendf(nil, "key%04d", i)
		value := fmt.Appendf(nil, "value%04d", i)
		r.NoError(db.Put(key, value))
	}
	for i := range 10 {
		key := fmt.Appendf(nil, "key%04d", i)
		r.NoError(db.Delete(key))
	}
	// Update some remaining keys
	for i := 15; i < 20; i++ {
		key := fmt.Appendf(nil, "key%04d", i)
		value := fmt.Appendf(nil, "updated%04d", i)
		r.NoError(db.Put(key, value))
	}

	r.NoError(db.Compact())
	r.NoError(db.Close())

	// Reopen and verify
	db, err = Open(f, WithStorage(File), WithIncrementSize(1024), WithCompactionDisabled())
	r.NoError(err)

	for i := range 10 {
		key := fmt.Appendf(nil, "key%04d", i)
		_, err := db.Get(key)
		r.Equal(ErrKeyNotFound, err, "key%04d should be deleted", i)
	}
	for i := 10; i < 15; i++ {
		key := fmt.Appendf(nil, "key%04d", i)
		value, err := db.Get(key)
		r.NoError(err)
		r.Equal(fmt.Appendf(nil, "value%04d", i), value)
	}
	for i := 15; i < 20; i++ {
		key := fmt.Appendf(nil, "key%04d", i)
		value, err := db.Get(key)
		r.NoError(err)
		r.Equal(fmt.Appendf(nil, "updated%04d", i), value)
	}

	r.NoError(db.Close())
}

// TestCompact_WithBuckets verifies that compaction works correctly with named
// buckets and their KV data.
func TestCompact_WithBuckets(t *testing.T) {
	r := assert.New(t)
	f, cleanup := mustTempFile()
	defer cleanup()

	db, err := Open(f, WithStorage(File), WithIncrementSize(1024), WithCompactionDisabled())
	r.NoError(err)

	// Create bucket and write data via transaction
	err = db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucket([]byte("mybucket"))
		if err != nil {
			return err
		}
		for i := range 20 {
			key := fmt.Appendf(nil, "bkey%04d", i)
			value := fmt.Appendf(nil, "bvalue%04d", i)
			if err = b.Put(key, value); err != nil {
				return err
			}
		}
		return nil
	})
	r.NoError(err)

	// Also write to default bucket
	for i := range 10 {
		key := fmt.Appendf(nil, "dkey%04d", i)
		value := fmt.Appendf(nil, "dvalue%04d", i)
		r.NoError(db.Put(key, value))
	}

	// Delete some from both buckets
	err = db.Update(func(tx *Tx) error {
		b, err := tx.OpenBucket([]byte("mybucket"))
		if err != nil {
			return err
		}
		for i := range 10 {
			key := fmt.Appendf(nil, "bkey%04d", i)
			if err = b.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
	r.NoError(err)

	for i := range 5 {
		key := fmt.Appendf(nil, "dkey%04d", i)
		r.NoError(db.Delete(key))
	}

	sizeBeforeCompact := db.Size()

	r.NoError(db.Compact())

	sizeAfterCompact := db.Size()
	t.Logf("Size: before=%d, after=%d", sizeBeforeCompact, sizeAfterCompact)

	r.True(sizeAfterCompact < sizeBeforeCompact,
		"data size should decrease after compaction")

	// Verify remaining data in named bucket
	err = db.View(func(tx *Tx) error {
		b, err := tx.OpenBucket([]byte("mybucket"))
		if err != nil {
			return err
		}
		for i := range 10 {
			key := fmt.Appendf(nil, "bkey%04d", i)
			_, err := b.Get(key)
			r.Equal(ErrKeyNotFound, err)
		}
		for i := 10; i < 20; i++ {
			key := fmt.Appendf(nil, "bkey%04d", i)
			value, err := b.Get(key)
			r.NoError(err)
			r.Equal(fmt.Appendf(nil, "bvalue%04d", i), value)
		}
		return nil
	})
	r.NoError(err)

	// Verify remaining data in default bucket
	for i := range 5 {
		key := fmt.Appendf(nil, "dkey%04d", i)
		_, err := db.Get(key)
		r.Equal(ErrKeyNotFound, err)
	}
	for i := 5; i < 10; i++ {
		key := fmt.Appendf(nil, "dkey%04d", i)
		value, err := db.Get(key)
		r.NoError(err)
		r.Equal(fmt.Appendf(nil, "dvalue%04d", i), value)
	}

	r.NoError(db.Close())

	// Verify persistence after reopen
	db, err = Open(f, WithStorage(File), WithIncrementSize(1024), WithCompactionDisabled())
	r.NoError(err)
	err = db.View(func(tx *Tx) error {
		b, err := tx.OpenBucket([]byte("mybucket"))
		if err != nil {
			return err
		}
		for i := 10; i < 20; i++ {
			key := fmt.Appendf(nil, "bkey%04d", i)
			value, err := b.Get(key)
			r.NoError(err)
			r.Equal(fmt.Appendf(nil, "bvalue%04d", i), value)
		}
		return nil
	})
	r.NoError(err)
	r.NoError(db.Close())
}

// TestCompact_EmptyDatabase verifies that compaction on an empty database
// works without errors.
func TestCompact_EmptyDatabase(t *testing.T) {
	r := assert.New(t)
	f, cleanup := mustTempFile()
	defer cleanup()

	db, err := Open(f, WithStorage(File), WithCompactionDisabled())
	r.NoError(err)

	r.NoError(db.Compact())
	r.Equal(int64(0), db.Size())

	r.NoError(db.Close())
}

// TestCompact_AllDeleted verifies that compaction works when all records have
// been deleted, resulting in an empty database file.
func TestCompact_AllDeleted(t *testing.T) {
	r := assert.New(t)
	f, cleanup := mustTempFile()
	defer cleanup()

	db, err := Open(f, WithStorage(File), WithIncrementSize(1024), WithCompactionDisabled())
	r.NoError(err)

	// Write then delete all keys
	for i := range 20 {
		key := fmt.Appendf(nil, "key%04d", i)
		value := fmt.Appendf(nil, "value%04d", i)
		r.NoError(db.Put(key, value))
	}

	sizeBeforeDelete := db.Size()
	r.True(sizeBeforeDelete > 0)

	for i := range 20 {
		key := fmt.Appendf(nil, "key%04d", i)
		r.NoError(db.Delete(key))
	}

	r.NoError(db.Compact())

	// After compaction with all deleted, data size should be 0
	r.Equal(int64(0), db.Size())

	// Should be able to write new data after full compaction
	r.NoError(db.Put([]byte("newkey"), []byte("newvalue")))
	value, err := db.Get([]byte("newkey"))
	r.NoError(err)
	r.Equal([]byte("newvalue"), value)

	r.NoError(db.Close())
}

// TestCompact_WriteAfterCompaction verifies that we can continue writing
// to the database after compaction completes.
func TestCompact_WriteAfterCompaction(t *testing.T) {
	r := assert.New(t)
	f, cleanup := mustTempFile()
	defer cleanup()

	db, err := Open(f, WithStorage(File), WithIncrementSize(1024), WithCompactionDisabled())
	r.NoError(err)

	for i := range 30 {
		key := fmt.Appendf(nil, "key%04d", i)
		r.NoError(db.Put(key, []byte("value")))
	}
	for i := range 15 {
		key := fmt.Appendf(nil, "key%04d", i)
		r.NoError(db.Delete(key))
	}

	r.NoError(db.Compact())

	// Write new keys after compaction
	for i := 30; i < 60; i++ {
		key := fmt.Appendf(nil, "key%04d", i)
		r.NoError(db.Put(key, []byte("post-compact")))
	}

	// Verify old + new keys
	for i := 15; i < 30; i++ {
		key := fmt.Appendf(nil, "key%04d", i)
		value, err := db.Get(key)
		r.NoError(err)
		r.Equal([]byte("value"), value)
	}
	for i := 30; i < 60; i++ {
		key := fmt.Appendf(nil, "key%04d", i)
		value, err := db.Get(key)
		r.NoError(err)
		r.Equal([]byte("post-compact"), value)
	}

	r.NoError(db.Close())
}

// TestCompact_MmapBackend verifies that compaction works with the Mmap backend.
func TestCompact_MmapBackend(t *testing.T) {
	r := assert.New(t)
	f, cleanup := mustTempFile()
	defer cleanup()

	db, err := Open(f, WithStorage(Mmap), WithIncrementSize(1024), WithCompactionDisabled())
	r.NoError(err)

	for i := range 100 {
		key := fmt.Appendf(nil, "key%04d", i)
		value := fmt.Appendf(nil, "value%04d-padding-data", i)
		r.NoError(db.Put(key, value))
	}
	for i := range 50 {
		key := fmt.Appendf(nil, "key%04d", i)
		r.NoError(db.Delete(key))
	}

	sizeBeforeCompact := db.Size()
	r.NoError(db.Compact())

	r.True(db.Size() < sizeBeforeCompact,
		"data size should decrease after compaction")

	// Verify remaining data
	for i := 50; i < 100; i++ {
		key := fmt.Appendf(nil, "key%04d", i)
		value, err := db.Get(key)
		r.NoError(err)
		r.Equal(fmt.Appendf(nil, "value%04d-padding-data", i), value)
	}

	r.NoError(db.Close())
}
