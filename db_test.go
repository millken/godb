package godb

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDB_Concurrent(t *testing.T) {
	require := require.New(t)
	dir, cleanup := mustTempDir()

	db, err := Open(dir)
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

	for i := 0; i < n; i++ {
		closeWg.Add(1)
		go func(i int) {
			var put, get, found uint
			defer func() {
				t.Logf("goroutine %d stopped after %d ops, put=%d get=%d found=%d missing=%d",
					i, cnt[i], put, get, found, get-found)
				closeWg.Done()
			}()

			rnd := rand.New(rand.NewSource(int64(1000 + i)))
			for atomic.LoadUint32(&stop) == 0 {
				x := cnt[i]

				k := rnd.Intn(maxkey)
				kstr := fmt.Sprintf("%016d", k)

				if (rnd.Int() % 2) > 0 {
					put++
					err := db.Put([]byte(kstr), []byte(fmt.Sprintf("%d.%d.%-1000d", k, i, x)))
					if err != nil {
						t.Error("Put: got error: ", err)
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
	dir, cleanup := mustTempDir()
	defer cleanup()
	db, err := Open(dir)
	require.NoError(err)

	keys := [64][]byte{}
	wants := [64]int{}
	for k := range keys {
		keys[k] = []byte(strconv.Itoa(k))
		wants[k] = -1
	}
	xxx := bytes.Repeat([]byte("x"), 512)

	rng := rand.New(rand.NewSource(123))
	const N = 1000
	for i := 0; i < N; i++ {
		k := rng.Intn(len(keys))
		if rng.Intn(20) != 0 {
			wants[k] = rng.Intn(len(xxx) + 1)
			if err := db.Put(keys[k], xxx[:wants[k]]); err != nil {
				t.Fatalf("i=%d: Put: %v", i, err)
			}
		} else {
			wants[k] = -1
			if err := db.Delete(keys[k]); err != nil {
				t.Fatalf("i=%d: Delete: %v", i, err)
			}
		}

		if i != N-1 || rng.Intn(50) != 0 {
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
		{"128B", 128},
		{"256B", 256},
		{"1K", 1024},
		{"2K", 2048},
		{"4K", 4096},
		{"8K", 8192},
		{"16K", 16384},
		{"32K", 32768},
	}

	variants := map[string][]Option{
		"NoSync": {
			FsyncOption(false),
		},
		"Sync": {
			FsyncOption(true),
		},
	}

	for name, options := range variants {
		dir, cleanup := mustTempDir()
		defer cleanup()

		db, err := Open(dir, options...)
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
				for i := 0; i < b.N; i++ {
					err := db.Put(key, value)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
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

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			dir, cleanup := mustTempDir()
			defer cleanup()

			db, err := Open(dir)
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
			for i := 0; i < b.N; i++ {
				val, err := db.Get(key)
				if err != nil {
					b.Fatal(err)
				}
				if !bytes.Equal(val, value) {
					b.Errorf("unexpected value")
				}
			}
			b.StopTimer()
			db.Close()
		})
	}
}

func BenchmarkDB_Delete(b *testing.B) {
	dir, cleanup := mustTempDir()
	defer cleanup()

	db, err := Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	const keyCount = 10000
	var keys [keyCount][]byte
	for i := 0; i < keyCount; i++ {
		keys[i] = []byte(strconv.Itoa(rng.Int()))
	}
	val := bytes.Repeat([]byte("x"), 10)
	for _, key := range keys {
		if err = db.Put(key, val); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err = db.Delete(keys[i%keyCount]); err != nil {
			b.Fatal(err)
		}

	}
	b.StopTimer()

}

func mustTempDir() (string, func()) {
	dir, err := ioutil.TempDir("", "db-test")
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
