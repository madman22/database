package database

import (
	"archive/zip"
	"bytes"
	"encoding/gob"
	"errors"
	"strings"
	"sync"

	//"time"

	badger "github.com/dgraph-io/badger"
	"github.com/spf13/afero"
)

type Version int

const (
	Version1 Version = iota
	Version2
	//Version3
)

const LatestVersion = Version2

func (v Version) String() string {
	switch v {
	//case Version3:
	//return "v3"
	case Version2:
		return "v2"
	default:
		return "v1"
	}
}

type DatabaseVersioner struct {
	mux sync.RWMutex
	ver Version
}

func (dbv *DatabaseVersioner) Version() Version {
	dbv.mux.RLock()
	defer dbv.mux.RUnlock()
	switch dbv.ver {
	//case Version3:
	//return Version3
	case Version2:
		return Version2
	default:
		return Version1
	}
}

func (dbv *DatabaseVersioner) Set(v Version) error {
	dbv.mux.Lock()
	defer dbv.mux.Unlock()
	dbv.ver = v
	return nil
}

func (db *BadgerDB) Version() Version {
	if db.version == nil {
		return Version1
	}
	return db.version.Version()
}

func (db *BadgerNode) Version() Version {
	if db.version == nil {
		return Version1
	}
	return db.version.Version()
}

func (db *BadgerExpiry) Version() Version {
	if db.version == nil {
		return Version1
	}
	return db.version.Version()
}

func Upgrade(dbi Database, target Version) error {
	//func Upgrade(db *BadgerDB, target Version) error {
	db, ok := dbi.(*BadgerDB)
	if !ok {
		return errors.New("Not Root Database Node!")
	}
	if db == nil {
		return ErrorDatabaseNil
	}
	dbv := db.Version()
	if dbv == target {
		return nil
	}
	var AppFs = afero.NewMemMapFs()

	//buf := &bytes.Buffer{}

	f, err := AppFs.Create("tmp.zip")
	if err != nil {
		return err
	}

	wr := zip.NewWriter(f)
	if err := db.Backup(wr); err != nil {
		return err
	}
	if err := wr.Close(); err != nil {
		return err
	}
	if err := db.Clear(); err != nil {
		return err
	}
	if err := db.version.Set(target); err != nil {
		return err
	}
	if err := db.saveVersion(); err != nil {
		return err
	}
	sta, err := f.Stat()
	if err != nil {
		return err
	}

	rdr, err := zip.NewReader(f, sta.Size())
	if err != nil {
		return errors.New("Error Creating Reader:" + err.Error())
	}
	if err := db.Restore(rdr); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return nil
}

func getVersion(db *badger.DB) Version {
	if db == nil {
		return Version2
	}
	var dbv Version
	f := func(val []byte) error {
		dcr := gob.NewDecoder(bytes.NewReader(val))
		if err := dcr.Decode(&dbv); err != nil {
			return err
		}
		return nil
	}
	if err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(VersionID))
		if err != nil {
			return err
		}
		if err := item.Value(f); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return Version1
	}
	return dbv
}

func (dbd *BadgerDB) saveVersion() error {
	if dbd.db == nil {
		return ErrorDatabaseNil
	}
	id := VersionID
	v := dbd.version.Version()

	buf := &bytes.Buffer{}
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(&v); err != nil {
		return err
	}
	f := func(txn *badger.Txn) error {
		ent := badger.NewEntry([]byte(id), buf.Bytes())
		return txn.SetEntry(ent)
	}
	return dbd.db.Update(f)
}

/*func getVersion(db *badger.DB) *DatabaseVersioner {
	if db == nil {
		var vers DatabaseVersioner
		vers.ver = Version2
		return &vers
	}
	var dbv Version
	f := func(val []byte) error {
		dcr := gob.NewDecoder(bytes.NewReader(val))
		if err := dcr.Decode(&dbv); err != nil {
			return err
		}
		return nil
	}
	if err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(SettingsPrefix + "DatabaseVersion"))
		if err != nil {
			return err
		}
		if err := item.Value(f); err != nil {
			return err
		}
		return nil
	}); err != nil {
		var vers DatabaseVersioner
		vers.ver = Version1
		return &vers
	}
	var vers DatabaseVersioner
	vers.ver = dbv
	return &vers
}*/

func getBadger(db *badger.DB, dbv Version, prefix, id string, i interface{}) error {
	if db == nil {
		return ErrorDatabaseNil
	}
	if dbv >= Version2 {
		id = EntityPrefix + id
	}
	f := func(val []byte) error {
		if nb, ok := i.([]byte); ok {
			if cap(nb) < len(val) {
				nb = make([]byte, len(val))
			}
			copy(nb, val)
			return nil
		}
		dcr := gob.NewDecoder(bytes.NewReader(val))
		if err := dcr.Decode(i); err != nil {
			return err
		}
		return nil
	}
	if err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(prefix + id))
		if err != nil {
			return err
		}
		if err := item.Value(f); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func getAndDeleteBadger(db *badger.DB, dbv Version, prefix, id string, i interface{}) error {
	if db == nil {
		return ErrorDatabaseNil
	}
	if dbv >= Version2 {
		id = EntityPrefix + id
	}
	f := func(val []byte) error {
		if nb, ok := i.([]byte); ok {
			if cap(nb) < len(val) {
				nb = make([]byte, len(val))
			}
			copy(nb, val)
			return nil
		}
		dcr := gob.NewDecoder(bytes.NewReader(val))
		if err := dcr.Decode(i); err != nil {
			return err
		}
		return nil
	}
	if err := db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(prefix + id))
		if err != nil {
			return err
		}
		if err := item.Value(f); err != nil {
			return err
		}
		if err := txn.Delete([]byte(prefix + id)); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func getValueBadger(db *badger.DB, dbv Version, prefix, id string) ([]byte, error) {
	if db == nil {
		return []byte{}, ErrorDatabaseNil
	}
	if dbv >= Version2 {
		id = EntityPrefix + id
	}
	var content []byte
	f := func(val []byte) error {
		if len(val) < 1 {
			content = make([]byte, 0)
			return nil
		}
		if cap(content) != len(val) {
			content = make([]byte, len(val))
		}
		copy(content, val)
		return nil
	}
	if err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(prefix + id))
		if err != nil {
			return err
		}
		if err := item.Value(f); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return []byte{}, err
	}
	return content, nil
}

func setBadger(db *badger.DB, dbv Version, prefix, id string, i interface{}) error {
	if db == nil {
		return ErrorDatabaseNil
	}
	if len(id) < 1 {
		return ErrorInvalidID
	}
	if dbv >= Version2 {
		id = EntityPrefix + id
	}
	buf := &bytes.Buffer{}
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(i); err != nil {
		return err
	}
	f := func(txn *badger.Txn) error {
		ent := badger.NewEntry([]byte(prefix+id), buf.Bytes())
		return txn.SetEntry(ent)
	}
	return db.Update(f)
}

func setValueBadger(db *badger.DB, dbv Version, prefix, id string, content []byte) error {
	if db == nil {
		return ErrorDatabaseNil
	}
	if len(id) < 1 {
		return ErrorInvalidID
	}
	if dbv >= Version2 {
		id = EntityPrefix + id
	}
	err := db.Update(func(txn *badger.Txn) error {
		ent := badger.NewEntry([]byte(prefix+id), content)
		return txn.SetEntry(ent)
	})
	if err != nil {
		return err
	}
	return nil
}

func deleteBadger(db *badger.DB, dbv Version, prefix, id string) error {
	if db == nil {
		return ErrorDatabaseNil
	}
	if dbv >= Version2 {
		id = EntityPrefix + id
	}
	f := func(txn *badger.Txn) error {
		return txn.Delete([]byte(prefix + id))
	}
	if err := db.Update(f); err != nil {
		return err
	}
	return nil
}

func getAllBadger(db *badger.DB, dbv Version, prefix string) (List, error) {
	if db == nil {
		return nil, ErrorDatabaseNil
	}
	if dbv < Version2 {
		return getAllBadgerV1(db, prefix)
	}
	return getAllBadgerV2(db, prefix)
}

func getAllBadgerV1(db *badger.DB, prefix string) (List, error) {
	list := make(List)
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
			return list.Add(key, b)
		}); err != nil {
			errs = append(errs, errors.New("key:"+key+" "+err.Error()))
		}
	}
	return list, nil
}

func getAllBadgerV2(db *badger.DB, prefix string) (List, error) {
	list := make(List)
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
		if strings.Contains(key, NodeSeparator) {
			continue
		}
		if err := item.Value(func(val []byte) error {
			b := make([]byte, len(val))
			copy(b, val)
			return list.Add(key, b)
		}); err != nil {
			errs = append(errs, errors.New("key:"+key+" "+err.Error()))
		}
	}
	return list, nil
}

func mergeBadger(db *badger.DB, dbv Version, prefix, id string, f MergeFunc) error {
	if db == nil {
		return ErrorDatabaseNil
	}
	if len(id) < 1 {
		return ErrorMissingID
	}
	if dbv >= Version2 {
		id = EntityPrefix + id
	}
	txn := db.NewTransaction(true)
	defer txn.Discard()
	var content []byte
	ff := func(val []byte) error {
		if len(val) < 1 {
			content = make([]byte, 0)
			return nil
		}
		if cap(content) != len(val) {
			content = make([]byte, len(val))
		}
		copy(content, val)
		return nil
	}
	if item, err := txn.Get([]byte(prefix + id)); err == nil {
		if err := item.Value(ff); err != nil {
			return err
		}
	}
	newdata, err := f(content)
	if err != nil {
		return err
	}
	if len(newdata) < 1 {
		return nil
	}
	if err := txn.Set([]byte(prefix+id), newdata); err != nil {
		return err
	}
	if err := txn.Commit(); err != nil {
		return err
	}
	return nil
}

func lenBadger(db *badger.DB, dbv Version, prefix string) int {
	if dbv > Version1 {
		return lenBadgerv2(db, prefix)
	} else {
		return lenBadgerv1(db, prefix)
	}
}

func lenBadgerv1(db *badger.DB, prefix string) int {
	if db == nil {
		return 0
	}
	var count int
	txn := db.NewTransaction(false)
	defer txn.Discard()

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	if len(prefix) > 0 {
		opts.Prefix = []byte(prefix)
	}
	it := txn.NewIterator(opts)
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		var key string
		if len(prefix) > 0 {
			key = strings.TrimPrefix(string(item.Key()), prefix)
		} else {
			key = string(item.Key())
		}
		if strings.HasPrefix(key, NodeSeparator) {
			continue
		}
		count++
	}
	return count
}

func lenBadgerv2(db *badger.DB, prefix string) int {
	if db == nil {
		return 0
	}
	var count int
	txn := db.NewTransaction(false)
	defer txn.Discard()

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	if len(prefix) > 0 {
		opts.Prefix = []byte(prefix + EntityPrefix)
	} else {
		opts.Prefix = []byte(EntityPrefix)
	}
	it := txn.NewIterator(opts)
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		count++
	}
	return count
}

func rangeBadger(db *badger.DB, dbv Version, prefix string, page, count int) (List, error) {
	if db == nil {
		return nil, ErrorDatabaseNil
	}
	if page < 1 {
		page = 1
	}
	if count < 1 {
		count = 1
	}
	list := make(List)
	txn := db.NewTransaction(false)
	defer txn.Discard()
	opts := badger.DefaultIteratorOptions
	if dbv > Version1 {
		opts.PrefetchValues = true
		if len(prefix) > 0 {
			opts.Prefix = []byte(prefix + EntityPrefix)
		} else {
			opts.Prefix = []byte(EntityPrefix)
		}
	} else {
		opts.PrefetchValues = false
		if len(prefix) > 0 {
			opts.Prefix = []byte(prefix)
		}
	}

	it := txn.NewIterator(opts)
	defer it.Close()
	var visited int
	var currentPage int = 1
	var errs []string
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		var key string
		if len(prefix) > 0 {
			if dbv > Version1 {
				key = strings.TrimPrefix(string(item.Key()), prefix+EntityPrefix)
			} else {
				key = strings.TrimPrefix(string(item.Key()), prefix)
			}
		} else {
			if dbv > Version1 {
				key = strings.TrimPrefix(string(item.Key()), EntityPrefix)
			} else {
				key = string(item.Key())
			}
		}
		if strings.HasPrefix(key, NodeSeparator) {
			continue
		}
		visited++
		if currentPage == page {
			if err := item.Value(func(val []byte) error {
				b := make([]byte, len(val))
				copy(b, val)
				return list.Add(key, b)
			}); err != nil {
				errs = append(errs, key+" "+err.Error())
			}
		} else if currentPage > page {
			break
		}
		if visited == count {
			currentPage++
			visited = 0
		}
	}
	if len(errs) > 0 {
		return list, errors.New(strings.Join(errs, " "))
	}
	return list, nil
}

/*func getNodesBadger(db *badger.DB, dbv Version, prefix string) ([]string, error) {
	if db == nil {
		return []string{}, ErrorDatabaseNil
	}
	if dbv >= Version2 {
		return getNodesBadgerV2(db, prefix)
	} else {
		return getNodesBadgerV1(db, prefix)
	}
}*/

func getNodesBadger(db *badger.DB, dbv Version, prefix string) ([]string, error) {
	if db == nil {
		return []string{}, ErrorDatabaseNil
	}
	list := make(map[string]struct{})

	txn := db.NewTransaction(false)
	defer txn.Discard()
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	if len(prefix) > 0 {
		opts.Prefix = []byte(prefix + NodeSeparator)
	} else {
		opts.Prefix = []byte(NodeSeparator)
	}

	it := txn.NewIterator(opts)
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		var key string
		if len(prefix) > 0 {
			key = strings.TrimPrefix(string(item.Key()), prefix+NodeSeparator)
		} else {
			key = string(item.Key()[1:])
		}
		if dbv >= Version2 {
			if strings.HasPrefix(key, EntityPrefix) {
				continue
			}
		}
		idex := strings.Index(key, NodeSeparator)
		if idex < 1 {
			continue
		}
		key = key[:idex]
		list[key] = struct{}{}
	}
	var nl []string
	for key, _ := range list {
		nl = append(nl, key)
	}
	return nl, nil
}

func nodeCountBadger(db *badger.DB, dbv Version, prefix string) int {
	if db == nil {
		return 0
	}
	list := make(map[string]struct{})

	txn := db.NewTransaction(false)
	defer txn.Discard()

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	if len(prefix) > 0 {
		opts.Prefix = []byte(prefix + NodeSeparator)
	} else {
		opts.Prefix = []byte(NodeSeparator)
	}

	it := txn.NewIterator(opts)
	defer it.Close()

	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		var key string
		if len(prefix) > 0 {
			key = strings.TrimPrefix(string(item.Key()), prefix+NodeSeparator)
		} else {
			key = string(item.Key()[1:])
		}
		if dbv >= Version2 {
			if strings.HasPrefix(key, EntityPrefix) {
				continue
			}
		}

		idex := strings.Index(key, NodeSeparator)
		if idex < 1 {
			continue
		}
		key = key[:idex]
		list[key] = struct{}{}
	}
	return len(list)
}

/*
func loadSettingsBadger(db *badger.DB, dbv Version, prefix string) error {
	if db == nil {
		return ErrorDatabaseNil
	}
	var timeout time.Duration


	f := func(val []byte) error {
		dcr := gob.NewDecoder(bytes.NewReader(val))
		if err := dcr.Decode(&timeout); err != nil {
			return err
		}
		return nil
	}
	if err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(SettingsPrefix + "Version"))
		if err != nil {
			return err
		}
		if err := item.Value(f); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return Version1
	}

}*/
