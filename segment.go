package godb

import (
	"hash/crc32"
	"os"
	"sync"

	art "github.com/millken/godb/internal/radixtree"
	"github.com/pkg/errors"

	"github.com/go-mmap/mmap"
)

var (
	ErrInvalidSegment        = errors.New("invalid segment")
	ErrInvalidSegmentVersion = errors.New("invalid segment version")
	ErrSegmentNotWritable    = errors.New("segment not writable")
	ErrSegmentTooLarge       = errors.New("segment too large")
	ErrKeyNotFound           = errors.New("key not found")
)

const (
	segmentVersion       uint8 = 1
	segmentMagic               = "GoSeG"
	segmentMetaSize      uint8 = 8 // magic + version + id
	segmentIncrementSize       = 32 * MB
	maxSegmentSize             = 2 * GB
)

type segmentMeta [segmentMetaSize]byte

func (m *segmentMeta) isValid() bool {
	return string(m[:len(segmentMagic)]) == segmentMagic && m[5] == segmentVersion
}

func (m *segmentMeta) setID(id uint16) {
	(*m)[6] = byte(id >> 8)
	(*m)[7] = byte(id)
}

func (m *segmentMeta) ID() uint16 {
	return uint16(m[6])<<8 | uint16(m[7])
}

func (m *segmentMeta) Version() uint8 {
	return m[5]
}

func (m *segmentMeta) encode() []byte {
	copy(m[:len(segmentMagic)], segmentMagic)
	(*m)[5] = segmentVersion
	return m[:]
}

type segment struct {
	mu   sync.RWMutex
	mmap *mmap.File
	idx  *art.Tree[index]
	path string
	size uint32
	id   uint16
}

// newSegment returns a new instance of segment.
func newSegment(id uint16, path string, idx *art.Tree[index]) *segment {
	return &segment{
		id:   id,
		path: path,
		idx:  idx,
	}
}

// createSegment generates an empty segment at path.
func createSegment(id uint16, path string, idx *art.Tree[index]) (*segment, error) {
	// Generate segment in temp location.
	f, err := os.Create(path + ".initializing")
	if err != nil {
		return nil, err
	}

	var m segmentMeta
	m.setID(id)
	if _, err := f.Write(m.encode()); err != nil {
		return nil, err
	}
	// Write header to file and close.
	if err := f.Truncate(int64(segmentIncrementSize)); err != nil {
		return nil, err
	} else if err := f.Close(); err != nil {
		return nil, err
	}

	// Swap with target path.
	if err := os.Rename(f.Name(), path); err != nil {
		return nil, err
	}

	// Open segment at new location.
	segment := newSegment(id, path, idx)
	if err := segment.Open(); err != nil {
		return nil, err
	}
	return segment, nil
}
func (s *segment) Open() error {
	if err := func() (err error) {
		if s.mmap, err = mmap.OpenFile(s.path, mmap.Read|mmap.Write); err != nil {
			return err
		} else if err = s.loadIndexs(); err != nil {
			return err
		}
		meta, err := s.readMeta()
		if err != nil {
			return err
		}
		s.id = meta.ID()
		return nil
	}(); err != nil {
		s.Close()
		return err
	}

	return nil
}

func (s *segment) ID() uint16 {
	return s.id
}

func (s *segment) readMeta() (segmentMeta, error) {
	var m segmentMeta
	_, err := s.mmap.ReadAt(m[:], 0)
	return m, err
}

func (s *segment) loadIndexs() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.size = uint32(segmentMetaSize); s.size < uint32(s.mmap.Len()); {
		var h hdr
		_, err := s.mmap.ReadAt(h[:], int64(s.size))
		if err != nil {
			return err
		}
		if !h.isValid() {
			break
		}
		// if h.getFlag().isDeleted() {
		// 	s.size += h.entrySize()
		// 	continue
		// }
		key := make([]byte, h.getKeySize())
		_, err = s.mmap.ReadAt(key, int64(s.size+hdrSize))
		if err != nil {
			return err
		}
		s.idx.Put(key, newIndex(s.id, s.size))
		s.size += h.entrySize()
	}
	return nil
}

func (s *segment) CanWrite() bool {
	size := s.Size()
	return size+segmentIncrementSize <= maxSegmentSize
}

func (s *segment) write(b []byte) error {
	if s.size+uint32(len(b)) > uint32(s.mmap.Len()) {
		if err := s.reSize(s.size + segmentIncrementSize); err != nil {
			return err
		}
	}
	if _, err := s.mmap.WriteAt(b, int64(s.size)); err != nil {
		return err
	}
	s.size += uint32(len(b))
	return nil
}

func (s *segment) Write(key, value []byte, state state) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entrySize := uint32(len(key) + len(value) + hdrSize)
	if s.size+entrySize > uint32(s.mmap.Len()) {
		if err = s.reSize(s.size + segmentIncrementSize); err != nil {
			return err
		}
	}
	var (
		h = hdr{}
		n int
	)
	h.setState(state).
		setKeySize(uint8(len(key))).
		setValueSize(uint32(len(value))).
		setChecksum(crc32.ChecksumIEEE(value))
	if n, err = s.mmap.WriteAt(h[:], int64(s.size)); err != nil {
		return errors.Wrap(err, "write header")
	}
	s.size += uint32(n)
	if _, err = s.mmap.WriteAt(key, int64(s.size)); err != nil {
		return errors.Wrap(err, "write key")
	}
	s.size += uint32(len(key))
	if value != nil {
		if _, err = s.mmap.WriteAt(value, int64(s.size)); err != nil {
			return errors.Wrap(err, "write value")
		}
		s.size += uint32(len(value))
	}
	s.idx.Put(key, newIndex(s.id, s.size-entrySize))
	return nil
}

func (s *segment) ReadAt(off uint32) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var h hdr
	_, err := s.mmap.ReadAt(h[:], int64(off))
	if err != nil {
		return nil, err
	}
	if !h.isValid() {
		return nil, ErrInvalidSegment
	}
	if h.getState() == deleted {
		return nil, ErrKeyNotFound
	}
	if h.getValueSize() == 0 {
		return nil, nil
	}
	value := make([]byte, h.getValueSize())
	_, err = s.mmap.ReadAt(value, int64(off+hdrSize+uint32(h.getKeySize())))
	if err != nil {
		return nil, err
	}
	return value, nil
}

// Close unmaps the segment.
func (s *segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.mmap.Sync(); err != nil {
		return err
	}
	return s.mmap.Close()
}

// Sync flushes the segment to disk.
func (s *segment) Sync() error {
	return s.mmap.Sync()
}

func (s *segment) Size() uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.size
}

// reSize resizes the segment to the given size.
func (s *segment) reSize(size uint32) (err error) {
	if err = s.mmap.Sync(); err != nil {
		return err
	}
	if err = s.mmap.Close(); err != nil {
		return err
	}
	if size > maxSegmentSize {
		return ErrSegmentTooLarge
	}
	if err = os.Truncate(s.path, int64(size)); err != nil {
		return err
	}
	if s.mmap, err = mmap.OpenFile(s.path, mmap.Read|mmap.Write); err != nil {
		return err
	}
	return nil
}

func (s *segment) updateRecordState(off uint32, st state) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var h hdr
	_, err := s.mmap.ReadAt(h[:], int64(off))
	if err != nil {
		return err
	}
	if !h.isValid() {
		return ErrInvalidSegment
	}
	h.setState(st)
	_, err = s.mmap.WriteAt(h[:], int64(off))
	return err
}

func (s *segment) ScanHdr(fn func(h hdr) error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for off := uint32(segmentMetaSize); off < s.size; {
		var h hdr
		_, err := s.mmap.ReadAt(h[:], int64(off))
		if err != nil {
			break
		}
		if !h.isValid() {
			break
		}
		if err = fn(h); err != nil {
			break
		}
		off += h.entrySize()
	}
	return
}

func (s *segment) Scan(fn func(r *record) error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for off := uint32(segmentMetaSize); off < s.size; {
		var h hdr
		_, err := s.mmap.ReadAt(h[:], int64(off))
		if err != nil {
			break
		}
		if !h.isValid() {
			break
		}
		key := make([]byte, h.getKeySize())
		_, err = s.mmap.ReadAt(key, int64(off+hdrSize))
		if err != nil {
			break
		}
		value := make([]byte, h.getValueSize())
		if _, err = s.mmap.ReadAt(value, int64(off+hdrSize+uint32(h.getKeySize()))); err != nil {
			break
		}
		rec := &record{
			hdr:    h,
			offset: off,
			key:    key,
			value:  value,
			seg:    s.id,
		}
		if err = fn(rec); err != nil {
			break
		}
		off += h.entrySize()
	}
	return
}
