package godb

type flag uint8

const (
	// flagEntryPut means the entry is added
	flagEntryPut flag = 1 << iota
	// flagEntryDelete means the entry is deleted
	flagEntryDelete
	// flagChunkFirst means the entry is started
	flagChunkFirst
	// flagChunkMiddle means the entry is in the middle
	flagChunkMiddle
	// flagChunkLast means the entry is finished
	flagChunkLast
	// flagChunkFull means the entry is normal
	flagChunkFull = flagChunkFirst | flagChunkLast
)

// IsEntryPut returns true if the flag is flagEntryPut
func (f flag) IsEntryPut() bool {
	return f&flagEntryPut != 0
}

// IsDel returns true if the flag is flagEntryDel
func (f flag) IsEntryDelete() bool {
	return f&flagEntryDelete != 0
}

// IsChunkFirst returns true if the flag is flagEntryStart
func (f flag) IsChunkFirst() bool {
	return f&flagChunkFirst != 0
}

// IsChunkLast returns true if the flag is flagEntryFinish
func (f flag) IsChunkLast() bool {
	return f&flagChunkLast != 0
}

// IsChunkMiddle returns true if the flag is flagEntryMiddle
func (f flag) IsChunkMiddle() bool {
	return f&flagChunkMiddle != 0
}

// IsChunkFull returns true if the flag is flagEntrySingle
func (f flag) IsChunkFull() bool {
	return f.IsChunkFirst() && f.IsChunkLast()
}
