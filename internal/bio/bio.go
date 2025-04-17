package bio

import "errors"

type BioEngine int

const (
	FileEngine BioEngine = iota
	MemoryEngine
	MmapEngine
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

func NewBio(engine BioEngine, path string, size int64) (Bio, error) {
	switch engine {
	case FileEngine:
		return NewFile(path, size)
	case MemoryEngine:
		return NewMemory(size)
	case MmapEngine:
		return NewMmap(path, size)
	default:
		return nil, errors.New("unsupported bio engine")
	}
}
