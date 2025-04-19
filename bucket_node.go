package godb

import (
	"io"

	"github.com/valyala/bytebufferpool"
)

var _ Node = (*bucketNode)(nil)

type bucketNode struct {
	node
	ID   uint32
	Name []byte
}

func newBucket(name []byte) *bucketNode {
	b := &bucketNode{
		node: node{
			Hdr: Header{},
		},
		ID:   bucketID(name),
		Name: name,
	}
	b.Hdr.EncodeState(TypeBucket, StateNormal)
	b.Hdr.SetEntrySize(uint32(len(name)))
	return b
}
func (b *bucketNode) BucketID() uint32 {
	return b.ID
}
func (b *bucketNode) Value() []byte {
	return nil
}

func (b *bucketNode) WriteTo(w io.Writer) (int64, error) {
	var n int64
	nn, err := w.Write(b.Hdr[:])
	n += int64(nn)
	if err != nil {
		return n, err
	}
	nn, err = w.Write(b.Name)
	n += int64(nn)
	return n, err
}

func (b *bucketNode) MarshalToBuffer(buff *bytebufferpool.ByteBuffer) error {
	wh, err := buff.Write(b.Hdr[:])
	if err != nil {
		return err
	}
	wk, err := buff.Write(b.Name)
	if err != nil {
		return err
	}
	if wh+wk != int(b.Size()) {
		return ErrInvalidRecord
	}
	return nil
}
