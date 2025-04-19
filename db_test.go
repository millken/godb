package godb

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var (
	testEngines = map[string]Option{
		// "File":   WithStorage(File),
		"Mmap": WithStorage(Mmap),
		// "Memory": WithStorage(Memory),
	}
)

type benchmarkTestCase struct {
	name string
	size int
}

func TestDB_Basic(t *testing.T) {
	r := require.New(t)
	dbpath, cleaner := mustTempFile()
	defer cleaner()
	db, err := Open(dbpath, WithStorage(File))
	r.NoError(err)
	r.NotNil(db)
	t.Run("Put", func(t *testing.T) {
		key := []byte("foo")
		value := []byte("bar")
		err = db.Put(key, value)
		r.NoError(err)
		t.Log(db.Size())
	})
	t.Run("Get", func(t *testing.T) {
		key := []byte("foo")
		value, err := db.Get(key)
		r.NoError(err)
		r.Equal([]byte("bar"), value)
		t.Log(db.Size())
	})
	t.Run("PutAgain", func(t *testing.T) {
		key := []byte("foo")
		value := []byte("baz")
		err = db.Put(key, value)
		r.NoError(err)
		t.Log(db.Size())
	})
	t.Run("GetAgain", func(t *testing.T) {
		key := []byte("foo")
		value, err := db.Get(key)
		r.NoError(err)
		r.Equal([]byte("baz"), value)
		t.Log(db.Size())
	})
	t.Run("Delete", func(t *testing.T) {
		key := []byte("foo")
		err = db.Delete(key)
		r.NoError(err)
		t.Log(db.Size())
	})
	t.Run("GetAfterDelete", func(t *testing.T) {
		key := []byte("foo")
		value, err := db.Get(key)
		r.ErrorIs(err, ErrKeyNotFound)
		r.Nil(value)
		t.Log(db.Size())
	})
	t.Run("PutAfterDelete", func(t *testing.T) {
		key := []byte("foo")
		value := []byte("bar")
		err = db.Put(key, value)
		r.NoError(err)
		t.Log(db.Size())
	})
	t.Run("GetAfterPut", func(t *testing.T) {
		key := []byte("foo")
		value, err := db.Get(key)
		r.NoError(err)
		r.Equal([]byte("bar"), value)
		t.Log(db.Size())
	})
	t.Run("ReOpenDB", func(t *testing.T) {
		r.NoError(db.Close())
		db, err = Open(dbpath)
		r.NoError(err)
		r.NotNil(db)
		t.Log(db.Size())
	})
	t.Run("GetAfterReOpen", func(t *testing.T) {
		key := []byte("foo")
		value, err := db.Get(key)
		r.NoError(err)
		r.Equal([]byte("bar"), value)
		t.Log(db.Size())
	})
	t.Run("DeleteAfterReOpen", func(t *testing.T) {
		key := []byte("foo")
		err = db.Delete(key)
		r.NoError(err)
		value, err := db.Get(key)
		r.ErrorIs(err, ErrKeyNotFound)
		r.Nil(value)
		t.Log(db.Size())
	})
	t.Run("GetAfterDeleteReOpen", func(t *testing.T) {
		key := []byte("foo")
		r.NoError(db.Close())
		db, err = Open(dbpath)
		r.NoError(err)
		value, err := db.Get(key)
		r.ErrorIs(err, ErrKeyNotFound)
		r.Nil(value)
		t.Log(db.Size())
	})
	t.Run("PutAgain", func(t *testing.T) {
		key := []byte("foo")
		value := []byte("bar")
		err = db.Put(key, value)
		r.NoError(err)
		t.Log(db.Size())
	})
	t.Run("Compact", func(t *testing.T) {
		err = db.Compact()
		r.NoError(err)
		t.Log(db.Size())
		value, err := db.Get([]byte("foo"))
		r.NoError(err)
		r.Equal([]byte("bar"), value)
	})
}

func TestDB(t *testing.T) {
	r := require.New(t)
	dbpath := path.Join(".", "_godb")
	db, err := Open(dbpath)
	r.NoError(err)
	r.NotNil(db)
	defer func() {
		os.Remove(dbpath)
	}()

	err = db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucket([]byte("test"))
		if err != nil {
			return err
		}
		for i := range 100 {
			key := fmt.Appendf(nil, "%016d", i)
			value := fmt.Appendf(nil, "%d", i)
			if err = b.Put(key, value); err != nil {
				return err
			}
		}
		return nil
	})
	r.NoError(err)
	r.NoError(db.Close())
	db, err = Open(dbpath)
	r.NoError(err)
	r.NotNil(db)
	db.View(func(tx *Tx) error {
		for i := range 100 {
			b, err := tx.OpenBucket([]byte("test"))
			if err != nil {
				return err
			}
			key := fmt.Appendf(nil, "%016d", i)
			value, err := b.Get(key)
			r.NoError(err)
			r.Equal([]byte(fmt.Sprintf("%d", i)), value)
		}
		return nil
	})

}

func TestDB_Concurrent(t *testing.T) {
	require := require.New(t)
	f, cleanup := mustTempFile()

	db, err := Open(f)
	require.NoError(err)
	require.NotNil(db)
	defer func() {
		require.NoError(db.Close())
		cleanup()
	}()

	const n, secs, maxkey = 4, 6, 1000

	runtime.GOMAXPROCS(runtime.NumCPU())

	var (
		closeWg sync.WaitGroup
		stop    uint32
		cnt     [n]uint32
	)

	for i := range n {
		closeWg.Add(1)
		go func(i int) {
			var put, get, found uint
			defer func() {
				t.Logf("goroutine %d stopped after %d ops, put=%d get=%d found=%d missing=%d",
					i, cnt[i], put, get, found, get-found)
				closeWg.Done()
			}()

			for atomic.LoadUint32(&stop) == 0 {
				x := cnt[i]

				k := rand.IntN(maxkey)
				kstr := fmt.Sprintf("%016d", k)

				if (rand.Int() % 2) > 0 {
					put++
					err := db.Put([]byte(kstr), []byte(fmt.Sprintf("%d.%d.%-1000d", k, i, x)))
					if err != nil {
						t.Error("Put: got error: ", err, kstr)
						return
					}
				} else {
					get++
					v, err := db.Get([]byte(kstr))
					if err == nil {
						found++
						rk, ri, rx := 0, -1, uint32(0)
						fmt.Sscanf(string(v), "%d.%d.%d", &rk, &ri, &rx)
						if rk != k {
							t.Errorf("invalid key want=%d got=%d", k, rk)
						}
						if ri < 0 || ri >= n {
							t.Error("invalid goroutine number: ", ri)
						} else {
							tx := atomic.LoadUint32(&(cnt[ri]))
							if rx > tx {
								t.Errorf("invalid seq number, %d > %d ", rx, tx)
							}
						}
					} else if err != ErrKeyNotFound {
						t.Error("Get: got error: ", err)
						return
					}
				}
				atomic.AddUint32(&cnt[i], 1)
			}
		}(i)
	}

	time.Sleep(secs * time.Second)
	atomic.StoreUint32(&stop, 1)
	closeWg.Wait()
}

func TestRandomWrites(t *testing.T) {
	require := require.New(t)
	f, cleanup := mustTempFile()
	defer cleanup()
	db, err := Open(f)
	require.NoError(err)

	keys := [64][]byte{}
	wants := [64]int{}
	for k := range keys {
		keys[k] = []byte(strconv.Itoa(k))
		wants[k] = -1
	}
	xxx := bytes.Repeat([]byte("x"), 512)

	const N = 1000
	for i := 0; i < N; i++ {
		k := rand.IntN(len(keys))
		if rand.IntN(20) != 0 {
			wants[k] = rand.IntN(len(xxx) + 1)
			if err := db.Put(keys[k], xxx[:wants[k]]); err != nil {
				t.Fatalf("i=%d: Put: %v", i, err)
			}
		} else {
			wants[k] = -1
			if err := db.Delete(keys[k]); err != nil {
				t.Fatalf("i=%d: Delete: %v", i, err)
			}
		}

		if i != N-1 || rand.IntN(50) != 0 {
			continue
		}
		for k := range keys {
			got := -1
			if v, err := db.Get(keys[k]); err != nil {
				if err != ErrKeyNotFound {
					t.Fatalf("Get: %v", err)
				}
			} else {
				got = len(v)
			}
			if got != wants[k] {
				t.Errorf("i=%d, k=%d: got %d, want %d", i, k, got, wants[k])
			}
		}
	}

	require.NoError(db.Close())
}
func BenchmarkDB_Put(b *testing.B) {

	tests := []benchmarkTestCase{
		// {"128B", 128},
		// {"256B", 256},
		{"1K", 1024},
		{"2K", 2048},
		{"4K", 4096},
		// {"8K", 8192},
		// {"16K", 16384},
		// {"32K", 32768},
	}

	variants := map[string][]Option{
		"NoSync": {
			WithFsync(false),
		},
		"Sync": {
			WithFsync(true),
		},
	}
	for engName, eng := range testEngines {
		for name, options := range variants {
			f, cleanup := mustTempFile()
			defer cleanup()
			options = append(options, eng)
			db, err := Open(f, options...)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			for _, tt := range tests {
				b.Run(tt.name+engName+name, func(b *testing.B) {
					b.SetBytes(int64(tt.size))

					key := []byte("foo")
					value := []byte(strings.Repeat(" ", tt.size))
					b.ResetTimer()
					for b.Loop() {
						err := db.Put(key, value)
						if err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}

func BenchmarkDB_Get(b *testing.B) {
	tests := []benchmarkTestCase{
		{"128B", 128},
		{"256B", 256},
		{"512B", 512},
		{"1K", 1024},
		{"2K", 2048},
		{"4K", 4096},
		{"8K", 8192},
		{"16K", 16384},
		{"32K", 32768},
	}
	for engName, eng := range testEngines {
		for _, tt := range tests {
			b.Run(tt.name+engName, func(b *testing.B) {
				f, cleanup := mustTempFile()
				defer cleanup()

				db, err := Open(f, eng)
				if err != nil {
					b.Fatal(err)
				}
				defer db.Close()
				b.SetBytes(int64(tt.size))

				key := []byte("foo")
				value := []byte(strings.Repeat(" ", tt.size))

				err = db.Put(key, value)
				if err != nil {
					b.Fatal(err)
				}

				b.ResetTimer()
				for b.Loop() {
					val, err := db.Get(key)
					if err != nil {
						b.Fatal(err)
					}
					if !bytes.Equal(val, value) {
						b.Errorf("unexpected value")
					}
				}
				b.StopTimer()
			})
		}
	}
}

func BenchmarkDB_Delete(b *testing.B) {
	f, cleanup := mustTempFile()
	defer cleanup()

	db, err := Open(f)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	const keyCount = 10000
	var keys [keyCount][]byte
	for i := range keyCount {
		keys[i] = []byte(strconv.Itoa(rand.Int()))
	}
	val := bytes.Repeat([]byte("x"), 10)
	for _, key := range keys {
		if err = db.Put(key, val); err != nil {
			b.Fatal(err)
		}
	}

	for i := 0; b.Loop(); i++ {
		if err = db.Delete(keys[i%keyCount]); err != nil {
			b.Fatal(err)
		}

	}
	b.StopTimer()

}

func BenchmarkTxn_Put(b *testing.B) {

	tests := []benchmarkTestCase{
		{"128B", 128},
		{"256B", 256},
		// {"1K", 1024},
		// {"2K", 2048},
		// {"4K", 4096},
		// {"8K", 8192},
		// {"16K", 16384},
		// {"32K", 32768},
	}

	variants := map[string][]Option{
		"NoSync": {
			WithFsync(false),
		},
		"Sync": {
			WithFsync(true),
		},
	}
	for engName, eng := range testEngines {
		for name, options := range variants {
			f, cleanup := mustTempFile()
			defer cleanup()
			options = append(options, eng)
			db, err := Open(f, options...)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			for _, tt := range tests {
				b.Run(tt.name+engName+name, func(b *testing.B) {
					b.SetBytes(int64(tt.size))

					key := []byte("foo")
					value := []byte(strings.Repeat(" ", tt.size))
					b.ResetTimer()
					for b.Loop() {
						db.Update(func(tx *Tx) error {
							bc := tx.Bucket([]byte("test"))
							if err := bc.Put(key, value); err != nil {
								b.Fatal(err)
							}
							return nil
						})
					}
				})
			}
		}
	}
}

func BenchmarkTxn_BatchPut(b *testing.B) {

	tests := []benchmarkTestCase{
		{"128B", 128},
		{"256B", 256},
		// {"1K", 1024},
		// {"2K", 2048},
		// {"4K", 4096},
		// {"8K", 8192},
		// {"16K", 16384},
		// {"32K", 32768},
	}

	variants := map[string][]Option{
		"NoSync": {
			WithFsync(false),
			WithStorage(File),
		},
		"Sync": {
			WithFsync(true),
			WithStorage(File),
		},
	}

	for name, options := range variants {
		f, cleanup := mustTempFile()
		defer cleanup()

		db, err := Open(f, options...)
		if err != nil {
			b.Fatal(err)
		}
		defer db.Close()

		for _, tt := range tests {
			b.Run(tt.name+name, func(b *testing.B) {
				b.SetBytes(int64(tt.size))

				key := []byte("foo")
				value := []byte(strings.Repeat(" ", tt.size))
				b.ResetTimer()
				db.Update(func(tx *Tx) error {
					for b.Loop() {
						bc := tx.Bucket([]byte("test"))
						if err := bc.Put(key, value); err != nil {
							b.Fatal(err)
						}
					}
					return nil
				})
			})
		}
	}
}
func mustTempDir() (string, func()) {
	dir, err := os.MkdirTemp("", "db-test")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp dir: %v", err))
	}
	return dir, func() { os.RemoveAll(dir) }
}

func mustTempFile() (string, func()) {
	file, err := os.CreateTemp("", "db-test")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp file: %v", err))
	}
	defer file.Close()
	return file.Name(), func() { os.Remove(file.Name()) }
}
