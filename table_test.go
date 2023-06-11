package godb

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
)

type benchmarkTestCase struct {
	name string
	size int
}

func TestTable(t *testing.T) {
	name, clean := mustTempFile()
	defer clean()
	tbl, err := OpenTable(name)
	if err != nil {
		t.Fatalf("failed to open table: %v", err)
	}
	defer tbl.Close()
	tests := []struct {
		name string
		size int
	}{
		{"128B", 128},
		{"256B", 256},
		{"1K", 1024},
		{"2K", 2048},
		{"4K", 4096},
		{"8K", 8192},
		{"16K", 16384},
		{"32K", 32768},
		{"64K", 65536},
		{"128K", 131072},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			key := []byte(test.name + strconv.Itoa(rand.Int()))
			val := bytes.Repeat([]byte{' '}, test.size)
			if err := tbl.Put(key, val); err != nil {
				t.Fatalf("failed to put: %v", err)
			}
			result, err := tbl.Get(key)
			if err != nil {
				t.Fatalf("failed to get: %s %v", key, err)
			}
			if !bytes.Equal(val, result) {
				t.Fatalf("expected %x, got %x", result, val)
			}
		})
	}
}

func BenchmarkTablePut(b *testing.B) {

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

	name, clean := mustTempFile()
	defer clean()

	db, err := OpenTable(name)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
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

func BenchmarkTableGet(b *testing.B) {
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
	name, clean := mustTempFile()
	defer clean()

	db, err := OpenTable(name)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
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
		})
	}
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
