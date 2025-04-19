package bio

import "errors"

type Storage int

const (
	FileStorage Storage = iota
	MemoryStorage
	MmapStorage
)

var (
	ErrInvalidOffset = errors.New("invalid offset")
)

type Bio interface {
	ReadAt(b []byte, off int64) (n int, err error)
	WriteAt(b []byte, off int64) (n int, err error)
	Close() error
	Truncate(size int64) error
	Sync() error
	Size() int64
}

func NewBio(s Storage, path string, size int64) (Bio, error) {
	switch s {
	case FileStorage:
		return NewFile(path, size)
	case MemoryStorage:
		return NewMemory(size)
	case MmapStorage:
		return NewMmap(path, size)
	default:
		return nil, errors.New("unsupported bio storage")
	}
}
