package database

import (
	"archive/zip"
	"bytes"
	"encoding/gob"
	"errors"
	"runtime"
	"strings"
	"sync"

	//"time"

	//badger "github.com/dgraph-io/badger/v3"
	badger "github.com/dgraph-io/badger/v4"
	"github.com/spf13/afero"
)

type Version int

const (
	Version1 Version = iota
	Version2
	Version3
)

const LatestVersion = Version2

//const LatestVersion = Version3

func (v Version) String() string {
	switch v {
	case Version3:
		return "v3"
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
	case Version3:
		return Version3
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

/*func getEncoder(db *badger.DB) int {
	if db == nil {
		return 0
	}
	var enc int
	f := func(val []byte) error {
		dcr := gob.NewDecoder(bytes.NewReader(val))
		if err := dcr.Decode(&enc); err != nil {
			return err
		}
		return nil
	}
	if err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(EncoderID))
		if err != nil {
			return err
		}
		if err := item.Value(f); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return 0
	}
	return enc
}*/

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

func getBadger(db *badger.DB, dbv Version, prefix, oid string, i interface{}) error {
	if db == nil {
		return ErrorDatabaseNil
	}
	var id string
	if dbv >= Version2 {
		id = EntityPrefix + oid
	} else {
		id = oid
	}
	f := buildGetBadger(i)
	f2 := buildGetBadgerV3(i)
	if err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(prefix + id))
		if err != nil {
			return err
		}
		decb := item.UserMeta()
		if decb == 2 {
			if err := item.Value(f2); err != nil {
				return err
			}
		} else {
			if err := item.Value(f); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		return errors.New(err.Error() + " " + oid)
	}
	return nil
}

func getAndDeleteBadger(db *badger.DB, dbv Version, prefix, id string, i interface{}) error {
	//func getAndDeleteBadger(db *badger.DB, dbv Version, subs *dbSubscribe, prefix, id string, i interface{}) error {
	if db == nil {
		return ErrorDatabaseNil
	}
	if dbv >= Version2 {
		id = EntityPrefix + id
	}
	/*f := func(val []byte) error {
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
	}*/
	f := buildGetBadger(i)
	f2 := buildGetBadgerV3(i)
	if err := db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(prefix + id))
		if err != nil {
			return err
		}
		decb := item.UserMeta()
		if decb == 2 {
			if err := item.Value(f2); err != nil {
				return err
			}
		} else {
			if err := item.Value(f); err != nil {
				return err
			}
		}
		if err := txn.Delete([]byte(prefix + id)); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	/*if subs == nil {
		return nil
	}
	if err := subs.Delete(id, prefix); err != nil {
		return err
	}

	*/
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
	//func setBadger(db *badger.DB, dbv Version, subs *dbSubscribe, prefix, id string, i interface{}) error {
	if db == nil {
		return ErrorDatabaseNil
	}
	if len(id) < 1 {
		pc, _, _, ok := runtime.Caller(1)
		details := runtime.FuncForPC(pc)
		if ok && details != nil {
			return errors.New(ErrorMissingID.Error() + " " + details.Name())
		}
		return ErrorMissingID
	}
	if dbv >= Version2 {
		id = EntityPrefix + id
	}
	var enc Encoder
	if dbv >= Version3 {
		enc = newCborEncoder()
	} else {
		enc = newGobEncoder()
	}
	//buf := &bytes.Buffer{}
	//enc := gob.NewEncoder(buf)

	if err := enc.Encode(i); err != nil {
		return err
	}
	f := func(txn *badger.Txn) error {
		if dbv >= Version3 {
			ent := badger.NewEntry([]byte(prefix+id), enc.Data()).WithMeta(byte(2))
			return txn.SetEntry(ent)
		} else {
			ent := badger.NewEntry([]byte(prefix+id), enc.Data())
			return txn.SetEntry(ent)
		}

	}
	/*if subs != nil {
		if err := subs.Send(id, prefix, enc.Data()); err != nil {
			return err
		}
	}

	*/
	return db.Update(f)
}

func setValueBadger(db *badger.DB, dbv Version, prefix, id string, content []byte) error {
	//func setValueBadger(db *badger.DB, dbv Version, subs *dbSubscribe, prefix, id string, content []byte) error {
	if db == nil {
		return ErrorDatabaseNil
	}
	if len(id) < 1 {
		return ErrorMissingID
	}
	if dbv >= Version2 && !strings.Contains(id, EntityPrefix) {
		id = EntityPrefix + id
	}
	err := db.Update(func(txn *badger.Txn) error {
		ent := badger.NewEntry([]byte(prefix+id), content)
		return txn.SetEntry(ent)
	})
	if err != nil {
		return err
	}
	/*if subs == nil {
		return nil
	}
	if err := subs.Send(id, prefix, content); err != nil {
		return err
	}

	*/
	return nil
}

func deleteBadger(db *badger.DB, dbv Version, prefix, id string) error {
	//func deleteBadger(db *badger.DB, dbv Version, subs *dbSubscribe, prefix, id string) error {
	if db == nil {
		return ErrorDatabaseNil
	}
	if dbv >= Version2 && !strings.Contains(id, EntityPrefix) {
		id = EntityPrefix + id
	}
	f := func(txn *badger.Txn) error {
		return txn.Delete([]byte(prefix + id))
	}
	if err := db.Update(f); err != nil {
		return err
	}
	/*if subs == nil {
		return nil
	}
	if err := subs.Delete(id, prefix); err != nil {
		return err
	}

	*/
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

func getAllIDsBadger(db *badger.DB, dbv Version, prefix string) ([]string, error) {
	if db == nil {
		return nil, ErrorDatabaseNil
	}
	if dbv < Version2 {
		return getAllIDsBadgerV1(db, prefix)
	}
	return getAllIDsBadgerV2(db, prefix)
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
	if len(errs) < 0 {
		var body string
		for _, err := range errs {
			if len(body) > 0 {
				body += " "
			}
			body += err.Error()
		}
		return list, errors.New(body)
	}
	return list, nil
}

func getAllIDsBadgerV1(db *badger.DB, prefix string) ([]string, error) {
	txn := db.NewTransaction(false)
	defer txn.Discard()
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	defer it.Close()
	//var errs []error
	var idList []string
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
		idList = append(idList, key)
	}
	return idList, nil
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
		enc := item.UserMeta()

		if err := item.Value(func(val []byte) error {
			b := make([]byte, len(val))
			copy(b, val)
			if enc == 2 {
				return list.AddV3(key, b)
			} else {
				return list.Add(key, b)
			}
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
		return list, errors.New(body)
	}
	return list, nil
}

func getAllIDsBadgerV2(db *badger.DB, prefix string) ([]string, error) {
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
	//var errs []error
	var idList []string
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		var key string
		if len(prefix) > 0 {
			key = strings.TrimPrefix(string(item.Key()), prefix+EntityPrefix)
		} else {
			key = strings.TrimPrefix(string(item.Key()), EntityPrefix)
		}
		idList = append(idList, key)
	}
	/*if len(errs) < 0 {
		var body string
		for _, err := range errs {
			if len(body) > 0 {
				body += " "
			}
			body += err.Error()
		}
		return []string{}, errors.New(body)
	}*/
	return idList, nil
}

func mergeBadger(db *badger.DB, dbv Version, prefix, id string, f MergeFunc) error {
	//func mergeBadger(db *badger.DB, dbv Version, prefix, id string, f MergeFunc, subs *dbSubscribe) error {
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
	/*if subs != nil {
		if err := subs.Send(id, prefix, newdata); err != nil {
			return err
		}
	}

	*/
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

			enc := item.UserMeta()

			if err := item.Value(func(val []byte) error {
				b := make([]byte, len(val))
				copy(b, val)
				if enc == 2 {
					return list.AddV3(key, b)
				} else {
					return list.Add(key, b)
				}
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
