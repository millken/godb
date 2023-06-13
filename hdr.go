package godb

const (
	hdrSize = 10
)

/*
* hdr format:
+----------+---------------+---------------+---------------+
| flag(1B) |  keySize (1B) | valueSize (4B)|  checksum (4B)|
+----------+---------------+---------------+---------------+
*
*/

type hdr [hdrSize]byte

func (h *hdr) getFlag() flag {
	return flag((*h)[0])
}

func (h *hdr) setFlag(f flag) *hdr {
	(*h)[0] = byte(f)
	return h
}

func (h *hdr) getKeySize() uint8 {
	return (*h)[1]
}

func (h *hdr) setKeySize(size uint8) *hdr {
	(*h)[1] = size
	return h
}

func (h *hdr) getChunkSize() uint32 {
	return uint32((*h)[2]) | uint32((*h)[3])<<8 | uint32((*h)[4])<<16 | uint32((*h)[5])<<24
}

func (h *hdr) setChunkSize(size uint32) *hdr {
	(*h)[2] = byte(size)
	(*h)[3] = byte(size >> 8)
	(*h)[4] = byte(size >> 16)
	(*h)[5] = byte(size >> 24)
	return h
}

func (h *hdr) getChecksum() uint32 {
	return uint32((*h)[6]) | uint32((*h)[7])<<8 | uint32((*h)[8])<<16 | uint32((*h)[9])<<24
}

func (h *hdr) setChecksum(checksum uint32) *hdr {
	(*h)[6] = byte(checksum)
	(*h)[7] = byte(checksum >> 8)
	(*h)[8] = byte(checksum >> 16)
	(*h)[9] = byte(checksum >> 24)
	return h
}
