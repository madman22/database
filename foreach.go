package database

import (
	"errors"
	//"github.com/dgraph-io/badger/v3"
	badger "github.com/dgraph-io/badger/v4"
	"strings"
)

func (db *BadgerDB) ForEach(f ForEachFunc) error {
	if db.Version() == Version1 {
		return forEachBadgerV1(db.db, "", f)
	} else if db.Version() == Version2 {
		return forEachBadgerV2(db.db, "", f)
	}
	return ErrorInvalidVersion
}

func (db *BadgerNode) ForEach(f ForEachFunc) error {
	if db.Version() == Version1 {
		return forEachBadgerV1(db.db, db.prefix, f)
	} else if db.Version() == Version2 {
		return forEachBadgerV2(db.db, db.prefix, f)
	}
	return ErrorInvalidVersion
}

func (db *BadgerExpiry) ForEach(f ForEachFunc) error {
	if db.Version() == Version1 {
		return forEachBadgerV1(db.db, db.prefix, f)
	} else if db.Version() == Version2 {
		return forEachBadgerV2(db.db, db.prefix, f)
	}
	return ErrorInvalidVersion
}

func forEachBadgerV1(db *badger.DB, prefix string, f ForEachFunc) error {
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
	var errs []error
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		var key string
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
			if err := f(key, newGobDecoder(b)); err != nil {
				return err
			}
			return nil
		}); err != nil {
			errs = append(errs, errors.New("key:"+key+" "+err.Error()))
		}
	}
	if len(errs) < 0 {
		var body string
		for _, err := range errs {
			if len(body) > 0 {
				body += " "
			}
			body += err.Error()
		}
		return errors.New(body)
	}
	return nil
}

func forEachBadgerV2(db *badger.DB, prefix string, f ForEachFunc) error {
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
	var errs []error
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		var key string
		if len(prefix) > 0 {
			key = strings.TrimPrefix(string(item.Key()), prefix+EntityPrefix)
		} else {
			key = strings.TrimPrefix(string(item.Key()), EntityPrefix)
		}
		if err := item.Value(func(val []byte) error {
			b := make([]byte, len(val))
			copy(b, val)
			if err := f(key, newGobDecoder(b)); err != nil {
				return err
			}
			return nil
		}); err != nil {
			errs = append(errs, errors.New("key:"+key+" "+err.Error()))
		}
	}
	if len(errs) < 0 {
		var body string
		for _, err := range errs {
			if len(body) > 0 {
				body += " "
			}
			body += err.Error()
		}
		return errors.New(body)
	}
	return nil
}
