package godb

import (
	"encoding/binary"
	"hash/crc32"
	"sync"

	"github.com/pkg/errors"
	"github.com/valyala/bytebufferpool"
)

var (
	ErrInvalidRecord = errors.New("invalid record")
)

type Node interface {
	// io.WriterTo
	Header() Header
	BucketID() uint32
	Size() uint32
	Value() []byte
	MarshalToBuffer(buff *bytebufferpool.ByteBuffer) error
}

type node struct {
	Hdr Header
}

func (rr *node) Size() uint32 {
	return HeaderSize + rr.Hdr.EntrySize()
}

func (rr *node) Header() Header {
	return rr.Hdr
}

var _ Node = (*kvNode)(nil)

var kvPool = &sync.Pool{
	New: func() interface{} {
		return &kvNode{
			node: node{
				Hdr: Header{},
			},
			key:   make([]byte, 0, 64),
			value: make([]byte, 0, 256),
		}
	},
}

func acquireKVNode() *kvNode {
	return kvPool.Get().(*kvNode)
}
func releaseKVNode(r *kvNode) {
	if r == nil {
		return
	}
	r.Reset()
	kvPool.Put(r)
}

func (r *kvNode) BucketID() uint32 {
	return r.bucketID
}

func (r *kvNode) Value() []byte {
	return r.value
}
func (r *kvNode) Reset() {
	r.bucketID = 0
	r.key = r.key[:0]
	r.value = r.value[:0]
	r.checkSum = 0
	// r.Hdr = Header{}
}

type kvNode struct {
	node
	bucketID uint32
	key      []byte
	value    []byte
	checkSum uint32
}

// func NewRecord(bucketID uint32, key, value []byte) *Record {
// 	r := &Record{
// 		bucketID: bucketID,
// 		key:      key,
// 		value:    value,
// 		checkSum: crc32.ChecksumIEEE(value),
// 	}
// 	r.Hdr.EncodeState(StateTypeRecord, StateRecordPutted)
// 	r.Hdr.SetEntrySize(uint32(len(key)+len(value)) + 8)
// 	return r
// }

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

func (r *kvNode) Set(bucketID uint32, key, value []byte) {
	r.Hdr.EncodeState(TypeKV, StateNormal)
	r.Hdr.SetEntrySize(uint32(len(key)+len(value)) + 14)
	r.bucketID = bucketID
	r.key = key
	r.value = value
	r.checkSum = crc32.ChecksumIEEE(value)
}

func (r *kvNode) MarshalToBuffer(buff *bytebufferpool.ByteBuffer) error {
	if r == nil {
		return errors.New("record is nil")
	}
	if len(r.key) == 0 {
		return errors.New("key is not set")
	}
	buff.Reset()
	var (
		n uint32
	)
	nn, err := buff.Write(r.Hdr[:])
	n += uint32(nn)
	if err != nil {
		return err
	}
	var bucketIDBytes [4]byte
	binary.LittleEndian.PutUint32(bucketIDBytes[:], r.bucketID)
	nn, err = buff.Write(bucketIDBytes[:])
	n += uint32(nn)
	if err != nil {
		return err
	}
	// write key length
	var keyLenBytes [2]byte
	binary.LittleEndian.PutUint16(keyLenBytes[:], uint16(len(r.key)))
	nn, err = buff.Write(keyLenBytes[:])
	n += uint32(nn)
	if err != nil {
		return err
	}
	nn, err = buff.Write(r.key)
	n += uint32(nn)
	if err != nil {
		return err
	}
	// write value length
	var valueLenBytes [4]byte
	binary.LittleEndian.PutUint32(valueLenBytes[:], uint32(len(r.value)))
	nn, err = buff.Write(valueLenBytes[:])
	n += uint32(nn)
	if err != nil {
		return err
	}
	nn, err = buff.Write(r.value)
	n += uint32(nn)
	if err != nil {
		return err
	}
	var checksumBytes [4]byte
	binary.LittleEndian.PutUint32(checksumBytes[:], r.checkSum)
	nn, err = buff.Write(checksumBytes[:])
	if err != nil {
		return err
	}
	n += uint32(nn)
	if n != r.Size() {
		return ErrInvalidRecord
	}
	return nil
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
