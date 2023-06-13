package godb

import "io"

type Storage interface {
	io.ReaderAt
	io.Writer
	io.Closer
	Sync() error
	Size() (int64, error)
}
