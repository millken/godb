package godb

// Tx represents a transaction on the database. This transaction can either be
// read-only or read/write. Read-only transactions can be used for retrieving
// values for keys and iterating through keys and values. Read/write
// transactions can set and delete keys.
//
// All transactions must be committed or rolled-back when done.
type Tx struct {
	db        *DB  // the underlying database.
	writable  bool // when false mutable operations fail.
	committed records
}

// lock locks the database based on the transaction type.
func (tx *Tx) lock() {
	if tx.writable {
		tx.db.mu.Lock()
	} else {
		tx.db.mu.RLock()
	}
}

// unlock unlocks the database based on the transaction type.
func (tx *Tx) unlock() {
	if tx.writable {
		tx.db.mu.Unlock()
	} else {
		tx.db.mu.RUnlock()
	}
}
func (tx *Tx) Commit() error {
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	}
	var err error
	if (len(tx.committed) > 0) && tx.writable {
		// If this operation fails then the write did failed and we must
		// rollback.
		err = tx.db.writeBatch(tx.committed)
		if err != nil {
			tx.rollback()
		}
	}

	// apply all commands
	err = tx.buildIndex(tx.committed)
	// Unlock the database and allow for another writable transaction.
	tx.unlock()
	// Clear the db field to disable this transaction from future use.
	tx.db = nil
	return err
}

// rollback handles the underlying rollback logic.
// Intended to be called from Commit() and Rollback().
func (tx *Tx) rollback() {
	tx.committed = nil
}

func (tx *Tx) buildIndex(recs []*record) error {
	for _, rec := range recs {
		tx.db.idx.Insert(rec.key, newIndex(rec.seg, rec.offset))
	}
	return nil
}

func (tx *Tx) addRecord(rec *record) {
	tx.committed = append(tx.committed, rec)
}

// Set saves a key-value pair.
func (tx *Tx) Put(key, value []byte) error {
	e := newRecord(key, value, putted)
	tx.addRecord(e)
	return nil
}
