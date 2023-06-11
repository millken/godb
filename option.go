package godb

import "github.com/cespare/xxhash/v2"

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

type Option func(*option) error

// HashFunc defines a function to generate the hash which will be used as key in db
type HashFunc func([]byte) uint64

// DefaultHashFunc implements a default hash function
func DefaultHashFunc(b []byte) uint64 {
	return xxhash.Sum64(b)
}

type option struct {
	// hashFunc is used to generate the hash which will be used as key in db
	hashFunc HashFunc
	// fsync is used to sync the data to disk
	fsync bool
}
