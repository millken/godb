package bio

import (
	"errors"
	"os"
	"testing"
)

func TestBio(t *testing.T) {
	t.Run("FileEngine", func(t *testing.T) {
		bio, err := NewBio(FileStorage, "testfile", 1024)
		if err != nil {
			t.Fatalf("NewBio(FileEngine) failed: %v", err)
		}
		defer bio.Close()
		defer os.Remove("testfile")
		if bio.Size() != 1024 {
			t.Errorf("Expected size 1024, got %d", bio.Size())
		}
		if err := bioTests(bio); err != nil {
			t.Errorf("bioTests failed: %v", err)
		}
	})

	t.Run("MemoryEngine", func(t *testing.T) {
		bio, err := NewBio(MemoryStorage, "", 1024)
		if err != nil {
			t.Fatalf("NewBio(MemoryEngine) failed: %v", err)
		}
		defer bio.Close()

		if bio.Size() != 1024 {
			t.Errorf("Expected size 1024, got %d", bio.Size())
		}
		if err := bioTests(bio); err != nil {
			t.Errorf("bioTests failed: %v", err)
		}
	})

	t.Run("MmapEngine", func(t *testing.T) {
		bio, err := NewBio(MmapStorage, "testmmap", 1024)
		if err != nil {
			t.Fatalf("NewBio(MmapEngine) failed: %v", err)
		}
		defer bio.Close()
		defer os.Remove("testmmap")
		if bio.Size() != 1024 {
			t.Errorf("Expected size 1024, got %d", bio.Size())
		}
		if err := bioTests(bio); err != nil {
			t.Errorf("bioTests failed: %v", err)
		}
	})
	t.Run("InvalidEngine", func(t *testing.T) {
		_, err := NewBio(-1, "", 1024)
		if err == nil {
			t.Fatalf("Expected error for invalid engine, got nil")
		}
	})
	t.Run("InvalidSize", func(t *testing.T) {
		_, err := NewBio(FileStorage, "", -1)
		if err == nil {
			t.Fatalf("Expected error for invalid size, got nil")
		}
	})
}

func bioTests(bio Bio) error {
	if bio == nil {
		return errors.New("bio is nil")
	}
	if err := bio.Truncate(2024); err != nil {
		return err
	}
	if bio.Size() != 2024 {
		return errors.New("size mismatch after truncate")
	}
	for i := range 100 {
		n, err := bio.WriteAt([]byte("test"), int64(i*5))
		if err != nil {
			return err
		}
		if n != 4 {
			return errors.New("write size mismatch")
		}
		var b = make([]byte, 4)
		n, err = bio.ReadAt(b, int64(i*5))
		if err != nil {
			return err
		}
		if n != 4 {
			return errors.New("read size mismatch")
		}
		if string(b) != "test" {
			return errors.New("read data mismatch")
		}
	}
	if err := bio.Sync(); err != nil {
		return err
	}
	return nil
}
