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
}

func FsyncOption(fsync bool) Option {
	return func(db *option) error {
		db.fsync = fsync
		return nil
	}
}
