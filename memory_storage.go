package godb

import (
	"bytes"
	"errors"
	"io"
	"os"
)

var _ Storage = (*memoryStorage)(nil)

type memoryStorage struct {
	data     *bytes.Buffer
	isClosed bool
}

func (s *memoryStorage) ReadAt(p []byte, off int64) (int, error) {
	if s.isClosed {
		return 0, os.ErrClosed
	}
	if off < 0 {
		return 0, errors.New("ReadAt: negative offset")
	}
	reqLen := len(p)
	buffLen := int64(s.data.Len())
	if off >= buffLen {
		return 0, io.EOF
	}

	n := copy(p, s.data.Bytes()[off:])
	if n < reqLen {
		return n, io.EOF
	}
	return n, nil
}

// Write implements io.Writer https://golang.org/pkg/io/#Writer
// by appending the passed bytes to the buffer unless the buffer is closed or index negative.
func (s *memoryStorage) Write(p []byte) (n int, err error) {
	if s.isClosed {
		return 0, os.ErrClosed
	}
	n, err = s.data.Write(p)

	return n, err
}

func (s *memoryStorage) Sync() error {
	return nil
}

func (s *memoryStorage) Size() (int64, error) {
	if s.isClosed {
		return 0, os.ErrClosed
	}

	return int64(s.data.Len()), nil
}

func (s *memoryStorage) Close() error {
	s.isClosed = true
	return nil
}

func newMemoryStorage(data []byte) Storage {
	return &memoryStorage{
		data: bytes.NewBuffer(data),
	}
}
