package godb

import (
	"bytes"
	"path/filepath"
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

	}

	err = segment.Close()
	require.NoError(err)
	idx = &art.Tree[index]{}
	segment = newSegment(0, file, idx)
	err = segment.Open()
	require.NoError(err)

	err = segment.Close()
	require.NoError(err)
}
