package database

import (
	"errors"
	"github.com/dgraph-io/badger/v4"
	"iter"
	"strings"
)

type ModifyAll struct {
	Decoder Decoder
	Set     func(interface{}) error
}

func modifyAllBadgerV2(db *badger.DB, prefix string) iter.Seq2[string, ModifyAll] {
	return func(yield func(string, ModifyAll) bool) {
		txnwrite := db.NewTransaction(true)

		txnread := db.NewTransaction(false)
		defer txnread.Discard()
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		if len(prefix) > 0 {
			opts.Prefix = []byte(prefix + EntityPrefix)
		} else {
			opts.Prefix = []byte(EntityPrefix)
		}
		it := txnread.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			var key string
			var m ModifyAll
			if len(prefix) > 0 {
				key = strings.TrimPrefix(string(item.Key()), prefix+EntityPrefix)
			} else {
				key = strings.TrimPrefix(string(item.Key()), EntityPrefix)
			}
			if err := item.Value(func(val []byte) error {
				b := make([]byte, len(val))
				copy(b, val)
				m.Decoder = newGobDecoder(b)
				return nil
			}); err != nil {
				continue
			}
			m.Set = func(i interface{}) error {
				enc := newGobEncoder()
				if err := enc.Encode(i); err != nil {
					return err
				}

				err := txnwrite.Set(item.Key(), enc.Data())
				if errors.Is(err, badger.ErrTxnTooBig) {
					txnwrite.Commit()
					txnwrite = db.NewTransaction(true)
					return txnwrite.Set(item.Key(), enc.Data())
				} else if err != nil {
					return err
				}
				return nil
			}
			if !yield(key, m) {
				txnwrite.Commit()
				return
			}
		}
		txnwrite.Commit()
	}
}

func modifyAllBadgerV1(db *badger.DB, prefix string) iter.Seq2[string, ModifyAll] {
	return func(yield func(string, ModifyAll) bool) {
		txnwrite := db.NewTransaction(true)
		//defer txnwrite.Commit()

		txnread := db.NewTransaction(false)
		defer txnread.Discard()
		opts := badger.DefaultIteratorOptions
		if len(prefix) > 0 {
			opts.Prefix = []byte(prefix)
			opts.PrefetchValues = true
		} else {
			opts.PrefetchValues = false
		}
		it := txnread.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			var key string
			var m ModifyAll
			if len(prefix) > 0 {
				key = strings.TrimPrefix(string(item.Key()), prefix)
			} else {
				key = string(item.Key())
			}
			if strings.Contains(key, NodeSeparator) {
				continue
			}
			if err := item.Value(func(val []byte) error {
				b := make([]byte, len(val))
				copy(b, val)
				m.Decoder = newGobDecoder(b)
				return nil
			}); err != nil {
				continue
			}
			m.Set = func(i interface{}) error {
				enc := newGobEncoder()
				if err := enc.Encode(i); err != nil {
					return err
				}

				err := txnwrite.Set(item.Key(), enc.Data())
				if errors.Is(err, badger.ErrTxnTooBig) {
					txnwrite.Commit()
					txnwrite = db.NewTransaction(true)
					return txnwrite.Set(item.Key(), enc.Data())
				} else if err != nil {
					return err
				}
				return nil
			}
			if !yield(key, m) {
				txnwrite.Commit()
				return
			}
		}
		txnwrite.Commit()
	}
}

func readAllBadgerV2(db *badger.DB, prefix string) iter.Seq2[string, Decoder] {
	return func(yield func(string, Decoder) bool) {
		txn := db.NewTransaction(false)
		defer txn.Discard()
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		if len(prefix) > 0 {
			opts.Prefix = []byte(prefix + EntityPrefix)
		} else {
			opts.Prefix = []byte(EntityPrefix)
		}
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			var key string
			var dec Decoder
			if len(prefix) > 0 {
				key = strings.TrimPrefix(string(item.Key()), prefix+EntityPrefix)
			} else {
				key = strings.TrimPrefix(string(item.Key()), EntityPrefix)
			}
			if err := item.Value(func(val []byte) error {
				b := make([]byte, len(val))
				copy(b, val)
				dec = newGobDecoder(b)
				return nil
			}); err != nil {
				continue
			}
			if !yield(key, dec) {
				return
			}
		}
	}
}

func readAllBadgerV1(db *badger.DB, prefix string) iter.Seq2[string, Decoder] {
	return func(yield func(string, Decoder) bool) {
		txn := db.NewTransaction(false)
		defer txn.Discard()
		opts := badger.DefaultIteratorOptions
		if len(prefix) > 0 {
			opts.Prefix = []byte(prefix)
			opts.PrefetchValues = true
		} else {
			opts.PrefetchValues = false
		}
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			var key string
			var dec Decoder
			if len(prefix) > 0 {
				key = strings.TrimPrefix(string(item.Key()), prefix)
			} else {
				key = string(item.Key())
			}
			if strings.Contains(key, NodeSeparator) {
				continue
			}
			if err := item.Value(func(val []byte) error {
				b := make([]byte, len(val))
				copy(b, val)
				dec = newGobDecoder(b)
				return nil
			}); err != nil {
				continue
			}
			if !yield(key, dec) {
				return
			}
		}
	}
}

func (db *BadgerNode) ReadAll() iter.Seq2[string, Decoder] {
	if db.version.Version() < Version2 {
		return readAllBadgerV1(db.db, db.prefix)
	}
	return readAllBadgerV2(db.db, db.prefix)
}

func (db *BadgerExpiry) ReadAll() iter.Seq2[string, Decoder] {
	if db.version.Version() < Version2 {
		return readAllBadgerV1(db.db, db.prefix)
	}
	return readAllBadgerV2(db.db, db.prefix)
}

func (db *BadgerDB) ReadAll() iter.Seq2[string, Decoder] {
	if db.version.Version() < Version2 {
		return readAllBadgerV1(db.db, "")
	}
	return readAllBadgerV2(db.db, "")
}

func (db *BadgerNode) ModifyAll() iter.Seq2[string, ModifyAll] {
	if db.version.Version() < Version2 {
		return modifyAllBadgerV1(db.db, db.prefix)
	}
	return modifyAllBadgerV2(db.db, db.prefix)
}

func (db *BadgerExpiry) ModifyAll() iter.Seq2[string, ModifyAll] {
	if db.version.Version() < Version2 {
		return modifyAllBadgerV1(db.db, db.prefix)
	}
	return modifyAllBadgerV2(db.db, db.prefix)
}

func (db *BadgerDB) ModifyAll() iter.Seq2[string, ModifyAll] {
	if db.version.Version() < Version2 {
		return modifyAllBadgerV1(db.db, "")
	}
	return modifyAllBadgerV2(db.db, "")
}
