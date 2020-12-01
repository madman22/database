package database

import (
	"bytes"
	"encoding/gob"
	"errors"
	"strings"
	"time"

	badger "github.com/dgraph-io/badger"
)

type BadgerExpiry struct {
	db      *badger.DB
	timeout time.Duration
	prefix  string
	id      string
}

func NewExpiryNode(db *badger.DB, prefix, id string, dur time.Duration) (Database, error) {
	if db == nil {
		return nil, ErrorDatabaseNil
	}
	if dur < 1 {
		return nil, ErrorNilValue
	}
	if len(id) < 1 || strings.Contains(id, NodeSeparator) {
		return nil, ErrorInvalidID
	}
	var de BadgerExpiry
	de.id = id
	de.timeout = dur
	de.db = db
	de.prefix = prefix + NodeSeparator + id + NodeSeparator
	/*if db, ok := dbi.(*BadgerDB); ok {
		if db.db == nil {
			return nil, ErrorDatabaseNil
		}
		de.db = db.db
		de.prefix = NodeSeparator + id + NodeSeparator
	} else {
		if db, ok := dbi.(*BadgerNode); ok {
			de.db = db.db
			if db.db == nil {
				return nil, ErrorDatabaseNil
			}
			de.prefix = db.prefix + NodeSeparator + id + NodeSeparator
		}
	}*/

	return &de, nil
}

func (bdb *BadgerExpiry) Parent() (Database, error) {
	if bdb.db == nil {
		return nil, ErrorDatabaseNil
	}
	nid := strings.TrimSuffix(bdb.prefix, NodeSeparator)
	nid = strings.TrimSuffix(nid, bdb.id)
	nid = strings.TrimSuffix(nid, NodeSeparator)
	nid = strings.TrimSuffix(nid, NodeSeparator)
	lindex := strings.LastIndex(nid, NodeSeparator)
	if lindex < 0 {
		var ndb BadgerNode
		ndb.prefix = ""
		ndb.db = bdb.db
		ndb.id = ""
		return &ndb, nil
	} else if lindex == 0 {
		var ndb BadgerNode
		ndb.id = nid[1:]
		ndb.prefix = NodeSeparator + ndb.id
		ndb.db = bdb.db
		return &ndb, nil
	}
	id := nid[lindex:]
	if len(id) < 1 {
		var ndb BadgerNode
		ndb.prefix = ""
		ndb.db = bdb.db
		ndb.id = ""
		return &ndb, nil
	}
	var ndb BadgerNode
	ndb.prefix = nid + NodeSeparator
	ndb.db = bdb.db
	ndb.id = id
	return &ndb, nil
}

//TODO backup only specific node prefix
// maybe gob encoder with a map[string][]byte?
/*func (ndb *BadgerExpiry) Backup() ([]byte, error) {
	if ndb.db == nil {
		return []byte{}, ErrorDatabaseNil
	}
	return ndb.Backup()
}*/

func (ndb *BadgerExpiry) Close() error {
	return ErrorNotClosable
}

func (ndb *BadgerExpiry) Delete(id string) error {
	if ndb.db == nil {
		return ErrorDatabaseNil
	}
	f := func(txn *badger.Txn) error {
		return txn.Delete([]byte(ndb.prefix + id))
	}
	if err := ndb.db.Update(f); err != nil {
		return err
	}
	return nil
}

func (ndb *BadgerExpiry) DropNode(id string) error {
	if ndb.db == nil {
		return ErrorDatabaseNil
	}
	if err := ndb.db.DropPrefix([]byte(ndb.prefix + NodeSeparator + id + NodeSeparator)); err != nil {
		return err
	}
	return nil
}

func (ndb *BadgerExpiry) Set(id string, i interface{}) error {
	if ndb.db == nil {
		return ErrorDatabaseNil
	}
	buf := &bytes.Buffer{}
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(i); err != nil {
		return err
	}
	f := func(txn *badger.Txn) error {
		ent := badger.NewEntry([]byte(ndb.prefix+id), buf.Bytes()).WithTTL(ndb.timeout)
		return txn.SetEntry(ent)
	}
	return ndb.db.Update(f)
}

func (ndb *BadgerExpiry) SetValue(id string, content []byte) error {
	if ndb.db == nil {
		return ErrorDatabaseNil
	}
	err := ndb.db.Update(func(txn *badger.Txn) error {
		ent := badger.NewEntry([]byte(ndb.prefix+id), content).WithTTL(ndb.timeout)
		return txn.SetEntry(ent)
	})
	if err != nil {
		return err
	}
	return nil
}

func (ndb *BadgerExpiry) Get(id string, i interface{}) error {
	if ndb.db == nil {
		return ErrorDatabaseNil
	}
	pid := ndb.prefix + id
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
	if err := ndb.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(pid))
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

func (ndb *BadgerExpiry) GetAndDelete(id string, i interface{}) error {
	if ndb.db == nil {
		return ErrorDatabaseNil
	}
	pid := ndb.prefix + id
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
	if err := ndb.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(pid))
		if err != nil {
			return err
		}
		if err := item.Value(f); err != nil {
			return err
		}
		if err := txn.Delete([]byte(pid)); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (ndb *BadgerExpiry) GetValue(id string) ([]byte, error) {
	if ndb.db == nil {
		return []byte{}, ErrorDatabaseNil
	}
	pid := ndb.prefix + id
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

	if err := ndb.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(pid))
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

func (ndb *BadgerExpiry) GetAll() (List, error) {
	if ndb.db == nil {
		return nil, ErrorDatabaseNil
	}
	list := make(List)

	txn := ndb.db.NewTransaction(false)
	defer txn.Discard()

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	opts.Prefix = []byte(ndb.prefix)

	it := txn.NewIterator(opts)
	defer it.Close()
	var errs []error
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		key := strings.TrimPrefix(string(item.Key()), ndb.prefix)
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

func (ndb *BadgerExpiry) GetNodes() ([]string, error) {
	if ndb.db == nil {
		return nil, ErrorDatabaseNil
	}
	list := make(map[string]struct{})
	txn := ndb.db.NewTransaction(false)
	defer txn.Discard()
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	defer it.Close()
	key := []byte(ndb.prefix + NodeSeparator)
	for it.Seek(key); it.ValidForPrefix(key); it.Next() {
		item := it.Item()
		ik := strings.TrimPrefix(string(item.Key()), string(key))
		idex := strings.Index(ik, NodeSeparator)
		if idex < 1 {
			continue
		}
		ik = ik[:idex]
		list[ik] = struct{}{}
	}
	var nl []string
	for id, _ := range list {
		nl = append(nl, id)
	}
	return nl, nil
}

func (ndb *BadgerExpiry) NewNode(id string) (Database, error) {
	if len(id) < 1 || strings.Contains(id, NodeSeparator) {
		return nil, ErrorInvalidID
	}
	if ndb.db == nil {
		return nil, ErrorDatabaseNil
	}
	var node BadgerNode
	node.db = ndb.db
	node.id = id
	node.prefix = ndb.prefix + NodeSeparator + id + NodeSeparator
	return &node, nil
}

func (db *BadgerExpiry) Merge(id string, f MergeFunc) error {
	if len(id) < 1 {
		return ErrorMissingID
	}
	txn := db.db.NewTransaction(true)
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
	if item, err := txn.Get([]byte(db.prefix + id)); err == nil {
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
	if err := txn.Set([]byte(db.prefix+id), newdata); err != nil {
		return err
	}
	if err := txn.Commit(); err != nil {
		return err
	}
	return nil
}

func (ndb *BadgerExpiry) Length() int {
	if ndb.db == nil {
		return 0
	}
	var count int

	txn := ndb.db.NewTransaction(false)
	defer txn.Discard()

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	opts.Prefix = []byte(ndb.prefix)

	it := txn.NewIterator(opts)
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		key := strings.TrimPrefix(string(item.Key()), ndb.prefix)
		if strings.HasPrefix(key, NodeSeparator) {
			continue
		}
		count++
	}
	return count
}

func (ndb *BadgerExpiry) NodeCount() int {
	if ndb.db == nil {
		return 0
	}
	list := make(map[string]struct{})

	txn := ndb.db.NewTransaction(false)
	defer txn.Discard()
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	defer it.Close()
	key := []byte(ndb.prefix + NodeSeparator)
	for it.Seek(key); it.ValidForPrefix(key); it.Next() {
		item := it.Item()
		ik := strings.TrimPrefix(string(item.Key()), string(key))
		idex := strings.Index(ik, NodeSeparator)
		if idex < 1 {
			continue
		}
		ik = ik[:idex]
		list[ik] = struct{}{}
	}
	return len(list)
}

func (dbd *BadgerExpiry) Range(page, count int) (List, error) {
	if dbd.db == nil {
		return nil, ErrorDatabaseNil
	}
	if page < 1 {
		page = 1
	}
	if count < 1 {
		count = 1
	}
	list := make(List)
	txn := dbd.db.NewTransaction(false)
	defer txn.Discard()
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	opts.Prefix = []byte(dbd.prefix)
	it := txn.NewIterator(opts)
	defer it.Close()
	var visited int
	var currentPage int = 1
	var errs []string
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		key := strings.TrimPrefix(string(item.Key()), dbd.prefix)
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

func (dbd *BadgerExpiry) Pages(count int) int {
	l := dbd.Length()
	if count > l {
		return 1
	}
	pages := l / count
	if l%count > 0 {
		pages++
	}
	return pages
}

func (dbd *BadgerExpiry) NewExpiryNode(id string, dur time.Duration) (Database, error) {
	return NewExpiryNode(dbd.db, "", id, dur)
}
