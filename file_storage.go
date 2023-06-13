package godb

import (
	"bufio"
	"io"
	"os"
)

var _ Storage = (*fileStorage)(nil)

const DataFilePerm = 0644

type fileStorage struct {
	fd *os.File
	wr *bufio.ReadWriter
}

func (b *fileStorage) ReadAt(p []byte, off int64) (int, error) {
	err := b.wr.Flush()
	if err != nil {
		return 0, err
	}
	_, err = b.fd.Seek(off, 0)
	if err != nil {
		return 0, err
	}
	return b.wr.Read(p)
}

func (b *fileStorage) Write(p []byte) (int, error) {
	return b.wr.Write(p)
}

func (b *fileStorage) Sync() error {
	err := b.wr.Flush()
	if err != nil {
		return err
	}
	return b.fd.Sync()
}

func (b *fileStorage) Close() error {
	err := b.wr.Flush()
	if err != nil {
		return err
	}
	return b.fd.Close()
}

func (b *fileStorage) Size() (int64, error) {
	err := b.wr.Flush()
	if err != nil {
		return 0, err
	}
	return b.fd.Seek(0, io.SeekEnd)
}

func newFileStorage(path string) (Storage, error) {
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, DataFilePerm)
	if err != nil {
		return nil, err
	}
	return &fileStorage{
		fd: fd,
		wr: bufio.NewReadWriter(bufio.NewReader(fd), bufio.NewWriterSize(fd, blockSize)),
	}, nil
}
