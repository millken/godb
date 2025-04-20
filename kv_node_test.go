package godb

import (
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/valyala/bytebufferpool"
)

func TestKVNode(t *testing.T) {
	r := require.New(t)
	key, value := []byte("foo"), []byte("bar")
	rec := acquireKVNode()
	rec.Set(1, key, value)
	r.Equal(uint32(1), rec.BucketID())
	r.Equal(crc32.ChecksumIEEE(value), rec.checkSum)
	r.False(rec.Hdr.IsBucket())
	r.True(rec.Hdr.IsNormal())
	buf := bytebufferpool.Get()
	rec.MarshalToBuffer(buf)
	r.Equal(int(rec.Size()), buf.Len())
	b1 := buf.Bytes()
	r.Equal(rec.Hdr[:], b1[:HeaderSize])

}
