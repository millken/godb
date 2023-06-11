package godb

const (
	// 32 KB
	blockSize = 32 * KB
)

type index struct {
	number uint32
	offset uint32
}
