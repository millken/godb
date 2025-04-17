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

type IoEngine bio.BioEngine

const (
	File   IoEngine = IoEngine(bio.FileEngine)
	Memory IoEngine = IoEngine(bio.MemoryEngine)
	Mmap   IoEngine = IoEngine(bio.MmapEngine)
)

type Option func(*option)

type option struct {
	// fsync is used to sync the data to disk
	fsync bool
	// segmentSize is the size of each segment
	segmentSize int64
	io          IoEngine
	// compactionInterval is the interval for automatic compaction
	compactionInterval time.Duration
}

func defaultOption() *option {
	return &option{
		fsync:       false,
		io:          Mmap,
		segmentSize: 32 * MB,
	}
}
func WithSegmentSize(s int64) Option {
	return func(o *option) {
		o.segmentSize = s
	}
}

func WithIoEngine(engine IoEngine) Option {
	return func(o *option) {
		o.io = engine
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
	return nil
}
