package godb

import art "github.com/millken/godb/internal/radixtree"

type Bucket struct {
	tx     *Transaction
	bucket uint32
	idx    *art.Tree[int64]
}

func (b *Bucket) Put(key, value []byte) error {
	return b.tx.put(b.bucket, key, value)
}

func (b *Bucket) Get(key []byte) ([]byte, error) {
	return b.tx.get(b.idx, key)
}
