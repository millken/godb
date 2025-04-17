package bio

import (
	"os"

	"github.com/edsrzf/mmap-go"
)

var _ Bio = (*Mmap)(nil)

type Mmap struct {
	fd    *os.File  // system file descriptor
	data  mmap.MMap // the mapping area corresponding to the file
	dirty bool      // has changed
}

func NewMmap(path string, size int64) (*Mmap, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	fi, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if size > fi.Size() {
		err = file.Truncate(size)
		if err != nil {
			return nil, err
		}
	}
	data, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		return nil, err
	}
	return &Mmap{
		fd:   file,
		data: data,
	}, nil
}

func (m *Mmap) ReadAt(b []byte, off int64) (n int, err error) {
	if off < 0 || off >= int64(len(m.data)) {
		return 0, ErrInvalidOffset
	}
	if off+int64(len(b)) > int64(len(m.data)) {
		b = b[:int64(len(m.data))-off]
	}
	copy(b, m.data[off:])
	return len(b), nil
}

func (m *Mmap) WriteAt(b []byte, off int64) (n int, err error) {
	if off < 0 || off >= int64(len(m.data)) {
		return 0, ErrInvalidOffset
	}
	if off+int64(len(b)) > int64(len(m.data)) {
		b = b[:int64(len(m.data))-off]
	}
	copy(m.data[off:], b)
	m.dirty = true
	return len(b), nil
}

func (m *Mmap) Close() error {
	if err := m.Sync(); err != nil {
		return err
	}
	if err := m.data.Unmap(); err != nil {
		return err
	}
	if err := m.fd.Close(); err != nil {
		return err
	}
	return nil
}

func (m *Mmap) Size() int64 {
	return int64(len(m.data))
}

func (m *Mmap) Truncate(size int64) error {
	if m.fd != nil {
		var err error
		if err = m.fd.Truncate(size); err != nil {
			return err
		}
		// 重新映射以包含新扩展区域
		if err = m.data.Unmap(); err != nil {
			return err
		}
		m.data, err = mmap.Map(m.fd, mmap.RDWR, 0)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Mmap) Sync() error {
	if m.dirty {
		if err := m.data.Flush(); err != nil {
			return err
		}
		m.dirty = false
	}
	return nil
}
