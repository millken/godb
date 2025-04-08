package godb

import (
	"hash/crc32"

	"github.com/pkg/errors"
	"github.com/valyala/bytebufferpool"
)

var (
	ErrInvalidRecord = errors.New("invalid record")
)

type record struct {
	hdr    hdr
	offset uint32
	seg    uint16
	key    []byte
	value  []byte
}

func newRecord(key, value []byte, st state) *record {
	r := &record{
		key:   key,
		value: value,
	}
	r.hdr.setState(st).
		setKeySize(uint8(len(key))).
		setValueSize(uint32(len(value))).
		setChecksum(crc32.ChecksumIEEE(value))
	return r
}

func (e *record) marshalToBuffer(buff *bytebufferpool.ByteBuffer) error {
	wh, err := buff.Write(e.hdr[:])
	if err != nil {
		return err
	}
	wk, err := buff.Write(e.key)
	if err != nil {
		return err
	}
	wv, err := buff.Write(e.value)
	if err != nil {
		return err
	}
	if wh+wk+wv != int(e.size()) {
		return ErrInvalidRecord
	}
	return nil
}

func (e *record) size() uint32 {
	return e.hdr.entrySize()
}

type records []*record

func (r records) size() uint32 {
	var size uint32
	for _, e := range r {
		size += e.size()
	}
	return size
}
