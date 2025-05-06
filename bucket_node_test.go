package godb

import (
	"hash/crc32"
	"testing"

	"github.com/dnsoa/go/assert"
	"github.com/valyala/bytebufferpool"
)

func TestBucketNode(t *testing.T) {
	r := assert.New(t)
	bucketName := []byte("foo")
	b := newBucket(bucketName)
	r.Equal(crc32.ChecksumIEEE(bucketName), b.ID)
	r.True(b.Hdr.IsBucket())
	buf := bytebufferpool.Get()
	b.MarshalToBuffer(buf)
	r.Equal(int(b.Size()), buf.Len())
	b1 := buf.Bytes()
	r.Equal(b.Hdr[:], b1[:HeaderSize])
	r.Equal(b.Name, b1[HeaderSize:])
}
