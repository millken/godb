package bio

import "os"

var _ Bio = (*File)(nil)

type File struct {
	file  *os.File
	size  int64
	dirty bool
}

func NewFile(path string, size int64) (*File, error) {
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
		fi, err = file.Stat()
		if err != nil {
			return nil, err
		}
	}
	if size < 0 {
		return nil, os.ErrInvalid
	}
	return &File{
		file: file,
		size: fi.Size(),
	}, nil
}

func (f *File) ReadAt(b []byte, off int64) (n int, err error) {
	if off < 0 || off >= f.size {
		return 0, ErrInvalidOffset
	}
	if off+int64(len(b)) > f.size {
		b = b[:f.size-off]
	}
	n, err = f.file.ReadAt(b, off)
	return n, err
}

func (f *File) WriteAt(b []byte, off int64) (n int, err error) {
	if off < 0 || off >= f.size {
		return 0, ErrInvalidOffset
	}
	if off+int64(len(b)) > f.size {
		b = b[:f.size-off]
	}
	n, err = f.file.WriteAt(b, off)
	if err == nil && off+int64(n) > f.size {
		f.size = off + int64(n)
	}
	f.dirty = true
	return n, err
}

func (f *File) Close() error {
	if f.file != nil {
		if err := f.Sync(); err != nil {
			return err
		}
		if err := f.file.Close(); err != nil {
			return err
		}
		f.file = nil
	}
	return nil
}

func (f *File) Size() int64 {
	return f.size
}

func (f *File) Truncate(size int64) error {
	if f.file != nil {
		err := f.file.Truncate(size)
		if err != nil {
			return err
		}
		f.size = size
		f.dirty = true
	}
	return nil
}

func (f *File) Sync() error {
	if f.dirty && f.file != nil {
		err := f.file.Sync()
		if err != nil {
			return err
		}
		f.dirty = false
	}
	return nil
}
