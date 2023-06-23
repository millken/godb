package godb

type flag uint8

const (
	// flagPut means the entry is added
	flagPut flag = 1 << iota
	// flagDelete means the entry is deleted
	flagDelete
)

// isPut returns true if the flag is flagEntryPut
func (f flag) isPut() bool {
	return f&flagPut != 0
}

// IsDel returns true if the flag is flagEntryDel
func (f flag) isDeleted() bool {
	return f&flagDelete != 0
}

func (f flag) isValid() bool {
	return f.isPut() || f.isDeleted()
}
