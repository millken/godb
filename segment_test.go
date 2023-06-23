package godb

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	art "github.com/WenyXu/sync-adaptive-radix-tree"
	"github.com/stretchr/testify/require"
)

type benchmarkTestCase struct {
	name string
	size int
}

func TestSegmentMeta(t *testing.T) {
	require := require.New(t)
	var meta segmentMeta
	meta.setID(2)
	buf := meta.encode()
	require.Equal(int(segmentMetaSize), len(buf))
	require.True(meta.isValid())
	require.Equal(segmentVersion, meta.Version())
	require.Equal(uint16(2), meta.ID())

}

func TestSegment(t *testing.T) {
	require := require.New(t)
	dir, cleanup := mustTempDir()
	defer cleanup()

	file := filepath.Join(dir, "segment001.test")
	idx := &art.Tree[index]{}
	segment, err := createSegment(0, file, idx)
	require.NoError(err)

	tests := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("foo"), []byte("bar")},
		{[]byte("foo1"), []byte("bar1")},
		{[]byte("foo2"), bytes.Repeat([]byte("m"), 3*(1<<20))},
	}
	for _, tt := range tests {
		err = segment.Write(tt.key, tt.value)
		require.NoError(err)

		value, err := segment.Read(tt.key)
		require.NoError(err)
		require.Equal(tt.value, value)
	}

	// Write another entry that is too large for the remaining segment space.
	// if err := segment.WriteEntry(createEntry(flagEntryPut, []byte("foo2"), bytes.Repeat([]byte("n"), (1<<30)))); err != ErrSegmentNotWritable {
	// 	t.Fatalf("unexpected error: %v", err)
	// }

	err = segment.Close()
	require.NoError(err)
	idx = &art.Tree[index]{}
	segment = newSegment(0, file, idx)
	err = segment.Open()
	require.NoError(err)
	for _, tt := range tests {
		value, err := segment.Read(tt.key)
		require.NoError(err)
		require.Equal(tt.value, value)
	}
	err = segment.Close()
	require.NoError(err)
}

func BenchmarkTablePut(b *testing.B) {

	tests := []benchmarkTestCase{
		{"128B", 128},
		// {"256B", 256},
		// {"1K", 1024},
		// {"2K", 2048},
		// {"4K", 4096},
		// {"8K", 8192},
		// {"16K", 16384},
		{"32K", 32768},
	}

	name, clean := mustTempFile()
	defer clean()
	idx := &art.Tree[index]{}
	seg, err := createSegment(0, name, idx)
	if err != nil {
		b.Fatal(err)
	}
	defer seg.Close()

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			b.SetBytes(int64(tt.size))

			key := []byte("foo")
			value := []byte(strings.Repeat(" ", tt.size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := seg.Write(key, value)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
