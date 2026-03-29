package godb

import (
"fmt"
"os"
"testing"

"github.com/dnsoa/go/assert"
)

// Quick debug test to demonstrate compaction bugs
func TestCompactDebug(t *testing.T) {
r := assert.New(t)
f, cleanup := mustTempFile()
defer cleanup()

// Use small increment size (1KB) to easily test expansion
db, err := Open(f, WithStorage(File), WithIncrementSize(1024), WithCompactionDisabled())
r.NoError(err)

// Write 100 keys, ~50 bytes each = ~5000 bytes total (more than incrementSize=1024)
for i := range 100 {
key := fmt.Appendf(nil, "key%04d", i)
value := fmt.Appendf(nil, "value%04d-data-padding-for-size", i)
r.NoError(db.Put(key, value))
}
t.Logf("After writing 100 keys: size=%d, fileSize=%d", db.Size(), db.io.Size())

// Delete half the keys
for i := range 50 {
key := fmt.Appendf(nil, "key%04d", i)
r.NoError(db.Delete(key))
}
t.Logf("After deleting 50 keys: size=%d, fileSize=%d", db.Size(), db.io.Size())

sizeBeforeCompact := db.Size()
fileSizeBeforeCompact := db.io.Size()

// Compact should shrink the file
err = db.Compact()
r.NoError(err)

sizeAfterCompact := db.Size()
fileSizeAfterCompact := db.io.Size()
t.Logf("After compaction: size=%d (was %d), fileSize=%d (was %d)",
sizeAfterCompact, sizeBeforeCompact,
fileSizeAfterCompact, fileSizeBeforeCompact)

// Data size should be smaller after compaction
r.True(sizeAfterCompact < sizeBeforeCompact,
"data size should decrease after compaction: %d >= %d", sizeAfterCompact, sizeBeforeCompact)

// File size should also shrink (or at least not be bigger than before)
r.True(fileSizeAfterCompact <= fileSizeBeforeCompact,
"file size should not grow after compaction: %d > %d", fileSizeAfterCompact, fileSizeBeforeCompact)

// Verify remaining keys are still accessible
for i := 50; i < 100; i++ {
key := fmt.Appendf(nil, "key%04d", i)
value, err := db.Get(key)
r.NoError(err, "key%04d should exist", i)
expected := fmt.Appendf(nil, "value%04d-data-padding-for-size", i)
r.Equal(expected, value)
}

// Verify deleted keys are still gone
for i := range 50 {
key := fmt.Appendf(nil, "key%04d", i)
_, err := db.Get(key)
r.Equal(ErrKeyNotFound, err, "key%04d should be deleted", i)
}

// Verify file on disk is actually smaller
fi, err := os.Stat(f)
r.NoError(err)
t.Logf("Actual file on disk: %d bytes", fi.Size())

r.NoError(db.Close())

// Reopen and verify data integrity after compaction
db, err = Open(f, WithStorage(File), WithIncrementSize(1024), WithCompactionDisabled())
r.NoError(err)

for i := 50; i < 100; i++ {
key := fmt.Appendf(nil, "key%04d", i)
value, err := db.Get(key)
r.NoError(err, "key%04d should exist after reopen", i)
expected := fmt.Appendf(nil, "value%04d-data-padding-for-size", i)
r.Equal(expected, value)
}

r.NoError(db.Close())
}
