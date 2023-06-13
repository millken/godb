package godb

import (
	"errors"
	"hash/crc32"

	art "github.com/WenyXu/sync-adaptive-radix-tree"
)

var (
	ErrTableClosed   = errors.New("the table file is closed")
	ErrKeyNotFound   = errors.New("key not found")
	ErrChecksum      = errors.New("checksum error")
	ErrBlockNotFound = errors.New("block not found")
)

type Table struct {
	storage            Storage
	indexer            art.Tree[*index]
	close              bool
	currentBlockNumber uint32
	currentBlockSize   uint32
}

func OpenTable(filename string) (*Table, error) {
	var err error
	t := &Table{
		indexer: art.Tree[*index]{},
	}
	if t.storage, err = newFileStorage(filename); err != nil {
		return nil, err
	}
	return t, nil
}

func (t *Table) Put(key, value []byte) error {
	if t.close {
		return ErrTableClosed
	}
	// the left size is not enough for the header
	if t.currentBlockSize+hdrSize >= blockSize {
		t.currentBlockNumber++
		t.currentBlockSize = 0
	}
	data := append(key, value...)
	dataSize := uint32(len(data))
	// the left size can fit the entry
	if int(t.currentBlockSize+hdrSize+dataSize) <= blockSize {
		return t.writeChunk(key, data, flagChunkFull|flagEntryPut)
	}

	// the left size can not fit the entry
	// the entry should be written to the next block
	var leftSize = dataSize
	//ptr := unsafe.Pointer(&data[0])
	for leftSize > 0 {
		chunkSize := blockSize - t.currentBlockSize - hdrSize
		if leftSize < chunkSize {
			chunkSize = leftSize
		}
		chunk := make([]byte, chunkSize)
		var end = dataSize - leftSize + chunkSize
		if end > dataSize {
			end = dataSize
		}
		copy(chunk[:], data[dataSize-leftSize:end])
		// write the chunks
		var chunkFlag flag
		var err error
		if leftSize == dataSize {
			// First Chunk
			chunkFlag = flagEntryPut | flagChunkFirst
		} else if leftSize == chunkSize {
			// Last Chunk
			chunkFlag = flagEntryPut | flagChunkLast
		} else {
			// Middle Chunk
			chunkFlag = flagEntryPut | flagChunkMiddle
		}
		err = t.writeChunk(key, chunk, chunkFlag)
		if err != nil {
			return err
		}
		leftSize -= chunkSize
	}

	return nil
}
func (t *Table) writeChunk(key, chunk []byte, flag flag) error {
	h := hdr{}
	chunksum := crc32.ChecksumIEEE(chunk)
	h.setFlag(flag).
		setKeySize(uint8(len(key))).
		setChunkSize(uint32(len(chunk))).
		setChecksum(chunksum)
	if flag.IsChunkFirst() {
		t.indexer.Insert(key, &index{
			number: t.currentBlockNumber,
			offset: t.currentBlockSize,
		})
	}
	n := uint32(0)
	hn, err := t.storage.Write(h[:])
	if err != nil {
		return err
	}
	n += uint32(hn)
	dn, err := t.storage.Write(chunk)
	if err != nil {
		return err
	}
	n += uint32(dn)
	t.currentBlockSize += n
	if t.currentBlockSize == blockSize {
		t.currentBlockNumber++
		t.currentBlockSize = 0
	}
	return nil
}

func (t *Table) Get(key []byte) ([]byte, error) {
	idx, found := t.indexer.Search(key)
	if !found {
		return nil, ErrKeyNotFound
	}
	return t.readIndex(idx)
}

func (t *Table) readBlock(num uint32) ([]byte, uint32, error) {
	if t.close {
		return nil, 0, ErrTableClosed
	}
	if num > t.currentBlockNumber {
		return nil, 0, ErrBlockNotFound
	}
	var sz uint32 = blockSize
	if num == t.currentBlockNumber {
		sz = t.currentBlockSize
	}
	off := int64(num * blockSize)
	blk := make([]byte, sz)
	if _, err := t.storage.ReadAt(blk, off); err != nil {
		return nil, 0, err
	}
	return blk, sz, nil
}

func (t *Table) readIndex(idx *index) ([]byte, error) {
	if t.close {
		return nil, ErrTableClosed
	}
	var (
		result             []byte
		h                  hdr
		checksum           uint32
		entryPositionStart uint32
		entryPositionEnd   uint32
		offset             uint32
	)
	blockNum := idx.number
	offset = idx.offset
	for {
		blk, _, err := t.readBlock(blockNum)
		if err != nil {
			return nil, err
		}
		h = hdr(blk[offset : offset+hdrSize])
		flag := h.getFlag()
		if flag.IsEntryPut() {
			if flag.IsChunkFull() {
				entryPositionStart = offset + hdrSize
				entryPositionEnd = entryPositionStart + h.getChunkSize()
				checksum = crc32.ChecksumIEEE(blk[entryPositionStart:entryPositionEnd])
				if checksum != h.getChecksum() {
					return nil, ErrChecksum
				}
				//key := blk[entryPositionStart : entryPositionStart+uint32(h.getKeySize())]
				result = append(result, blk[entryPositionStart:entryPositionEnd]...)
				if flag.IsChunkLast() {
					return result[int(h.getKeySize()):], nil
				}
				blockNum++
				break
			} else {
				entryPositionStart = offset + hdrSize
				entryPositionEnd = entryPositionStart + h.getChunkSize()
				checksum = crc32.ChecksumIEEE(blk[entryPositionStart:entryPositionEnd])
				if checksum != h.getChecksum() {
					return nil, ErrChecksum
				}
				result = append(result, blk[entryPositionStart:entryPositionEnd]...)
				blockNum++
				offset = 0
				if flag.IsChunkLast() {
					return result[int(h.getKeySize()):], nil
				}
			}
		}
	}

	return result, nil
}

func (t *Table) Delete(key []byte) error {
	return nil
}

func (t *Table) Close() error {
	if t.close {
		return nil
	}
	t.close = true
	return t.storage.Close()
}
