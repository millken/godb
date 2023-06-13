package godb

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMemoryStorage(t *testing.T) {
	require := require.New(t)
	storage := newMemoryStorage(nil)
	n, err := storage.Size()
	require.NoError(err)
	require.Equal(int64(0), n)
	n1, err := storage.Write([]byte("hello"))
	require.NoError(err)
	require.Equal(5, n1)
	n1, err = storage.Write([]byte(" world"))
	require.NoError(err)
	require.Equal(6, n1)
	buf := make([]byte, 5)
	n1, err = storage.ReadAt(buf, 0)
	require.NoError(err)
	require.Equal(5, n1)
	require.Equal("hello", string(buf))
	n1, err = storage.ReadAt(buf, 5)
	require.NoError(err)
	require.Equal(5, n1)
	require.Equal(" worl", string(buf))
	_, err = storage.ReadAt(buf, 10)
	require.ErrorIs(err, io.EOF)
	n, err = storage.Size()
	require.NoError(err)
	require.Equal(int64(11), n)
	n1, err = storage.Write([]byte("123"))
	require.NoError(err)
	require.Equal(3, n1)
	n, err = storage.Size()
	require.NoError(err)
	require.Equal(int64(14), n)
	buf = make([]byte, 14)
	n1, err = storage.ReadAt(buf, 0)
	require.NoError(err)
	require.Equal(14, n1)
	require.Equal("hello world123", string(buf))
	require.NoError(storage.Sync())
	require.NoError(storage.Close())
	_, err = storage.ReadAt(buf, 0)
	require.ErrorIs(err, os.ErrClosed)
}