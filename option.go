package godb

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

type Option func(*option) error

type option struct {
	// fsync is used to sync the data to disk
	fsync bool
	// segmentSize is the size of each segment
	segmentSize int64
}

func defaultOption() *option {
	return &option{
		fsync: false,
	}
}
func WithSegmentSize(s int64) Option {
	return func(o *option) error {
		o.segmentSize = s
		return nil
	}
}
func WithFsync(fsync bool) Option {
	return func(o *option) error {
		o.fsync = fsync
		return nil
	}
}
