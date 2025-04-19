package godb

import (
	"hash/crc32"
	"time"

	"github.com/millken/godb/internal/bio"
)

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)
const (
	// MaxKeySize is the maximum length of a key, in bytes.
	MaxKeySize = 32768

	// MaxValueSize is the maximum length of a value, in bytes.
	MaxValueSize = (1 << 31) - 2
)

type IOStorage bio.Storage

const (
	File   IOStorage = IOStorage(bio.FileStorage)
	Memory IOStorage = IOStorage(bio.MemoryStorage)
	Mmap   IOStorage = IOStorage(bio.MmapStorage)
)

type Option func(*option)

type option struct {
	// fsync is used to sync the data to disk
	fsync bool
	// incrementSize is the size of each segment
	incrementSize int64
	io            IOStorage
	// compactionInterval is the interval for automatic compaction
	compactionInterval time.Duration
}

func defaultOption() *option {
	return &option{
		fsync:         false,
		io:            Mmap,
		incrementSize: 256 * MB,
	}
}
func WithIncrementSize(s int64) Option {
	return func(o *option) {
		o.incrementSize = s
	}
}

func WithStorage(s IOStorage) Option {
	return func(o *option) {
		o.io = s
	}
}

func WithFsync(fsync bool) Option {
	return func(o *option) {
		o.fsync = fsync
	}
}

// WithCompactionInterval 设置自动压缩间隔
func WithCompactionInterval(interval time.Duration) Option {
	return func(o *option) {
		o.compactionInterval = interval
	}
}

// WithCompactionDisabled 禁用自动压缩
func WithCompactionDisabled() Option {
	return func(o *option) {
		o.compactionInterval = 0 // 设为0禁用定时压缩
	}
}

func bucketID(name []byte) uint32 {
	return crc32.ChecksumIEEE(name)
}

func validateKey(key []byte) error {
	if len(key) == 0 {
		return ErrKeyRequired
	} else if len(key) > MaxKeySize {
		return ErrKeyTooLarge
	}
	return nil
}
