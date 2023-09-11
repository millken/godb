package godb

import (
	"testing"
	"unsafe"
)

func TestUnsafe(t *testing.T) {
	b := []byte("hello_world")
	t.Logf("%s", b)
	b1 := b[:5]
	t.Logf("%d %d %s", len(b1), cap(b1), b1)
	// use unsafe
	ptr := unsafe.Pointer(&b[0])
	// get a slice from ptr, it's a copy of b[:5]
	b2 := *(*[5]byte)(ptr)
	t.Logf("%d %d %s", len(b2), cap(b2), b2)
	// use unsafe get world
	ptr = unsafe.Pointer(uintptr(ptr) + 6)
	b3 := *(*[5]byte)(ptr)
	t.Logf("%d %d %s", len(b3), cap(b3), b3)
}

func TestHdr(t *testing.T) {
	h := hdr{}
	h.setKeySize(100)
	h.setValueSize(1000)
	h.setChecksum(10000)

	if h.getKeySize() != 100 {
		t.Fatalf("expect 100, got %v", h.getKeySize())
	}
	if h.getValueSize() != 1000 {
		t.Fatalf("expect 1000, got %v", h.getValueSize())
	}
	if h.getChecksum() != 10000 {
		t.Fatalf("expect 10000, got %v", h.getChecksum())
	}
}

func BenchmarkHdrSet(b *testing.B) {
	for i := 0; i < b.N; i++ {
		h := hdr{}
		h.setKeySize(100)
		h.setValueSize(1000)
		h.setChecksum(10000)
	}
}

func BenchmarkHdrGet(b *testing.B) {
	h := hdr{}
	for i := 0; i < b.N; i++ {
		h.getKeySize()
		h.getValueSize()
		h.getChecksum()
	}
}
