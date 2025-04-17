package bio

var _ Bio = (*Memory)(nil)

type Memory struct {
	data []byte
	size int64
}

func NewMemory(size int64) (*Memory, error) {
	return &Memory{
		data: make([]byte, size),
		size: size,
	}, nil
}

func (m *Memory) ReadAt(b []byte, off int64) (n int, err error) {
	if off < 0 || off >= m.size {
		return 0, ErrInvalidOffset
	}
	if off+int64(len(b)) > m.size {
		b = b[:m.size-off]
	}
	copy(b, m.data[off:])
	return len(b), nil
}

func (m *Memory) WriteAt(b []byte, off int64) (n int, err error) {
	if off < 0 || off >= m.size {
		return 0, ErrInvalidOffset
	}
	if off+int64(len(b)) > m.size {
		b = b[:m.size-off]
	}
	copy(m.data[off:], b)
	return len(b), nil
}

func (m *Memory) Close() error {
	m.data = nil
	m.size = 0
	return nil
}

func (m *Memory) Size() int64 {
	return m.size
}

func (m *Memory) Truncate(size int64) error {
	if size < 0 {
		return ErrInvalidOffset
	}
	oldSize := int64(len(m.data))
	if size > oldSize {
		// 只扩容，不分配临时大 slice
		if int(size) > cap(m.data) {
			newData := make([]byte, size, size*3/2+1) // 预留空间，减少多次扩容
			copy(newData, m.data)
			m.data = newData
		} else {
			m.data = m.data[:size]
		}
	} else {
		m.data = m.data[:size]
	}
	m.size = size
	return nil
}

func (m *Memory) Sync() error {
	// No-op for memory
	return nil
}
