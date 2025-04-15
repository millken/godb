package godb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHeader(t *testing.T) {
	r := require.New(t)
	var hdr Header
	hdr.SetEntrySize(100)
	hdr.SetType(TypeBucket)
	hdr.EncodeState(TypeBucket, StateNormal)
	r.True(hdr.IsPutted())
	r.True(hdr.IsBucket())
	r.Equal(TypeBucket, hdr.StateType())
	r.Equal(uint32(100), hdr.EntrySize())
	hdr.SetType(TypeKV)
	hdr.SetRecord(StateDeleted)
	r.True(hdr.IsDeleted())
	r.False(hdr.IsBucket())
	r.True(hdr.IsKV())

}

func BenchmarkHeader(b *testing.B) {
	b.Run("Encode", func(b *testing.B) {
		var hdr Header
		for i := 0; i < b.N; i++ {
			hdr.SetType(TypeBucket)
			hdr.SetRecord(StateDeleted)
		}
	})
	b.Run("Decode", func(b *testing.B) {
		var hdr Header
		hdr.SetType(TypeBucket)
		hdr.SetRecord(StateDeleted)
		for i := 0; i < b.N; i++ {
			hdr.StateRecord()
		}
	})

}
