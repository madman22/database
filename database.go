package database

import (
	"bytes"
	"context"

	//"encoding/json"
	"encoding/gob"
	"errors"

	//"fmt"
	//"os"
	"strings"
	"time"

	badger "github.com/dgraph-io/badger"
)

const NodeSeparator = `|`

var ErrorDatabaseNil = errors.New("Database Nil!")
var ErrorNotImplemented = errors.New("Not Implemented!")
var ErrorNotClosable = errors.New("Node not closable")
var ErrorInvalidKeyCharacter = errors.New("Cannot use " + NodeSeparator + " as part of a key.  That is used for designating nodes")
var ErrorMissingID = errors.New("ID cannot be empty")
var ErrorNotFound = errors.New("Item not found in database")
var ErrorNilValue = errors.New("Nil Interface Value")
var ErrorKeysNil = errors.New("Key Map is nil, try adding elements with Set")
var ErrorInvalidID = errors.New("Cannot use the same ID as the key maps")

type Database interface {
	DatabaseReader
	DatabaseWriter
	DatabaseNode
	DatabaseIO
}

type DatabaseIO interface {
	Close() error
	Backup() ([]byte, error)
}

type DatabaseReader interface {
	Get(string, interface{}) error
	GetValue(string) ([]byte, error)
	GetAll() (List, error)
	Length() int
}

type DatabaseWriter interface {
	Set(string, interface{}) error
	SetValue(string, []byte) error
	Delete(string) error
	GetAndDelete(string, interface{}) error
	Merge(string, MergeFunc) error
}

type MergeFunc func([]byte) ([]byte, error)

type DatabaseNode interface {
	NodeCount() int
	GetNodes() ([]string, error)
	NewNode(string) (Database, error)
	DropNode(string) error
	Parent() (Database, error)
}

type List map[string]Decoder

type Decoder interface {
	Data() []byte
	Decode(interface{}) error
}

type gobDecoder struct {
	data *bytes.Buffer
	dec  *gob.Decoder
}

func newDecoder(data []byte) Decoder {
	var gd gobDecoder
	gd.data = bytes.NewBuffer(data)
	gd.dec = gob.NewDecoder(gd.data)
	return &gd
}

func (gd *gobDecoder) Data() []byte {
	return gd.data.Bytes()
}

func (gd *gobDecoder) Decode(i interface{}) error {
	if gd.dec == nil {
		return errors.New("Decoder not built!")
	}
	if gd.data == nil {
		return errors.New("Empty data")
	}
	return gd.dec.Decode(i)
}

func (l List) Add(key string, content []byte) error {
	if l == nil {
		l = make(List)
	}
	if len(key) < 1 {
		return ErrorKeysNil
	}
	l[key] = newDecoder(content)
	return nil
}

type BadgerDB struct {
	dbname  string
	db      *badger.DB
	gccount uint64
	ctx     context.Context
	cancel  context.CancelFunc
}

type BadgerNode struct {
	prefix string
	id     string
	db     *badger.DB
}

func NewDefaultDatabase(name string) (Database, error) {
	return NewBadger(name, context.Background(), 15*time.Minute)
}

func NewInMemoryBadger(ctx context.Context, dur time.Duration) (*BadgerDB, error) {
	var bdb BadgerDB
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	if err != nil {
		return nil, err
	}
	bdb.db = db
	if dur < 1*time.Second {
		dur = 1 * time.Hour
	}
	bdb.ctx, bdb.cancel = context.WithCancel(ctx)
	go bdb.startGC(bdb.ctx, dur)
	return &bdb, nil
}

//New Badger database with the given name as the file structure and the context and duration used for garbage collection.
func NewBadger(name string, ctx context.Context, dur time.Duration) (*BadgerDB, error) {
	var bdb BadgerDB
	db, err := badger.Open(badger.DefaultOptions(name))
	if err != nil {
		return nil, err
	}
	bdb.db = db
	if dur < 1*time.Second {
		dur = 1 * time.Minute
	}
	bdb.ctx, bdb.cancel = context.WithCancel(ctx)
	go bdb.startGC(bdb.ctx, dur)
	return &bdb, nil
}

func (bdb *BadgerDB) startGC(ctx context.Context, dur time.Duration) {
	ticker := time.NewTicker(dur)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			//again:
			err := bdb.db.RunValueLogGC(0.7)
			if err == nil {
				continue
			}
		case <-ctx.Done():
			return
		}
	}
}

func (bdb *BadgerDB) Parent() (Database, error) {
	return nil, ErrorDatabaseNil
}

func (bdb *BadgerDB) Get(id string, i interface{}) error {
	if bdb.db == nil {
		return ErrorDatabaseNil
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
	if err := bdb.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(id))
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

func (bdb *BadgerDB) GetAndDelete(id string, i interface{}) error {
	if bdb.db == nil {
		return ErrorDatabaseNil
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
	if err := bdb.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(id))
		if err != nil {
			return err
		}
		if err := item.Value(f); err != nil {
			return err
		}
		if err := txn.Delete([]byte(id)); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (bdb *BadgerDB) GetValue(id string) ([]byte, error) {
	if bdb.db == nil {
		return []byte{}, ErrorDatabaseNil
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
	if err := bdb.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(id))
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

func (bdb *BadgerDB) Set(id string, i interface{}) error {
	if bdb.db == nil {
		return ErrorDatabaseNil
	}
	buf := &bytes.Buffer{}
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(i); err != nil {
		return err
	}
	f := func(txn *badger.Txn) error {
		ent := badger.NewEntry([]byte(id), buf.Bytes())
		return txn.SetEntry(ent)
	}

	return bdb.db.Update(f)
}

func (bdb *BadgerDB) SetValue(id string, content []byte) error {
	if bdb.db == nil {
		return ErrorDatabaseNil
	}
	err := bdb.db.Update(func(txn *badger.Txn) error {
		ent := badger.NewEntry([]byte(id), content)
		return txn.SetEntry(ent)
	})
	if err != nil {
		return err
	}
	return nil
}

func (bdb *BadgerDB) Backup() ([]byte, error) {
	if bdb.db == nil {
		return []byte{}, ErrorDatabaseNil
	}
	buf := &bytes.Buffer{}
	_, err := bdb.db.Backup(buf, 0)
	if err != nil {
		return []byte{}, err
	}
	return buf.Bytes(), nil
}

func (bdb *BadgerDB) Close() error {
	if bdb.db == nil {
		return ErrorDatabaseNil
	}
	if bdb.cancel != nil {
		bdb.cancel()
	}
	return bdb.db.Close()
}

func (bdb *BadgerDB) Delete(id string) error {
	if bdb.db == nil {
		return ErrorDatabaseNil
	}
	f := func(txn *badger.Txn) error {
		return txn.Delete([]byte(id))
	}
	if err := bdb.db.Update(f); err != nil {
		return err
	}
	return nil
}

func (bdb *BadgerDB) DropNode(id string) error {
	if bdb.db == nil {
		return ErrorDatabaseNil
	}
	if err := bdb.db.DropPrefix([]byte(NodeSeparator + id + NodeSeparator)); err != nil {
		return err
	}
	return nil
}

func (bdb *BadgerDB) GetAll() (List, error) {
	if bdb.db == nil {
		return nil, ErrorDatabaseNil
	}
	list := make(List)

	txn := bdb.db.NewTransaction(false)
	defer txn.Discard()

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false

	it := txn.NewIterator(opts)
	defer it.Close()
	var errs []error
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		key := string(item.Key())
		if strings.Contains(key, NodeSeparator) {
			continue
		}
		if err := item.Value(func(val []byte) error {
			return list.Add(key, val)
		}); err != nil {
			errs = append(errs, errors.New("key:"+key+" "+err.Error()))
		}
	}
	return list, nil
}

func (bdb *BadgerDB) GetNodes() ([]string, error) {
	if bdb.db == nil {
		return nil, ErrorDatabaseNil
	}
	list := make(map[string]struct{})

	txn := bdb.db.NewTransaction(false)
	defer txn.Discard()

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false

	it := txn.NewIterator(opts)
	defer it.Close()

	key := []byte(NodeSeparator)
	for it.Seek(key); it.ValidForPrefix(key); it.Next() {
		item := it.Item()
		key := string(item.Key()[1:])
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

func (bdb *BadgerDB) NewNode(id string) (Database, error) {
	if strings.Contains(id, NodeSeparator) {
		return nil, ErrorInvalidKeyCharacter
	}
	if len(id) < 1 {
		return nil, ErrorInvalidID
	}
	if bdb.db == nil {
		return nil, ErrorDatabaseNil
	}
	var ndb BadgerNode
	ndb.db = bdb.db
	ndb.id = id
	ndb.prefix = NodeSeparator + id + NodeSeparator
	return &ndb, nil
}

func (bdb *BadgerNode) Parent() (Database, error) {
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
func (ndb *BadgerNode) Backup() ([]byte, error) {
	if ndb.db == nil {
		return []byte{}, ErrorDatabaseNil
	}
	return ndb.Backup()
}

func (ndb *BadgerNode) Close() error {
	return ErrorNotClosable
}

func (ndb *BadgerNode) Delete(id string) error {
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

func (ndb *BadgerNode) DropNode(id string) error {
	if ndb.db == nil {
		return ErrorDatabaseNil
	}
	if err := ndb.db.DropPrefix([]byte(ndb.prefix + NodeSeparator + id + NodeSeparator)); err != nil {
		return err
	}
	return nil
}

func (ndb *BadgerNode) Set(id string, i interface{}) error {
	if ndb.db == nil {
		return ErrorDatabaseNil
	}
	buf := &bytes.Buffer{}
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(i); err != nil {
		return err
	}
	f := func(txn *badger.Txn) error {
		ent := badger.NewEntry([]byte(ndb.prefix+id), buf.Bytes())
		return txn.SetEntry(ent)
	}
	return ndb.db.Update(f)
}

func (ndb *BadgerNode) SetValue(id string, content []byte) error {
	if ndb.db == nil {
		return ErrorDatabaseNil
	}
	err := ndb.db.Update(func(txn *badger.Txn) error {
		ent := badger.NewEntry([]byte(ndb.prefix+id), content)
		return txn.SetEntry(ent)
	})
	if err != nil {
		return err
	}
	return nil
}

func (ndb *BadgerNode) Get(id string, i interface{}) error {
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

func (ndb *BadgerNode) GetAndDelete(id string, i interface{}) error {
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

func (ndb *BadgerNode) GetValue(id string) ([]byte, error) {
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

func (ndb *BadgerNode) GetAll() (List, error) {
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
			return list.Add(key, val)
		}); err != nil {
			errs = append(errs, errors.New("key:"+key+" "+err.Error()))
		}
	}
	return list, nil
}

func (ndb *BadgerNode) GetNodes() ([]string, error) {
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

func (ndb *BadgerNode) NewNode(id string) (Database, error) {
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

func (db *BadgerDB) Merge(id string, f MergeFunc) error {
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
	if item, err := txn.Get([]byte(id)); err == nil {
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
	if err := txn.Set([]byte(id), newdata); err != nil {
		return err
	}
	if err := txn.Commit(); err != nil {
		return err
	}
	return nil
}

func (db *BadgerNode) Merge(id string, f MergeFunc) error {
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

func (bdb *BadgerDB) Length() int {
	if bdb.db == nil {
		return 0
	}
	var count int
	txn := bdb.db.NewTransaction(false)
	defer txn.Discard()

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false

	it := txn.NewIterator(opts)
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		key := string(item.Key())
		if strings.HasPrefix(key, NodeSeparator) {
			continue
		}
		count++
	}
	return count
}

func (ndb *BadgerNode) Length() int {
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
		if strings.Contains(key, NodeSeparator) {
			continue
		}
		count++
	}
	return count
}

func (bdb *BadgerDB) NodeCount() int {
	if bdb.db == nil {
		return 0
	}
	list := make(map[string]struct{})

	txn := bdb.db.NewTransaction(false)
	defer txn.Discard()

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false

	it := txn.NewIterator(opts)
	defer it.Close()

	key := []byte(NodeSeparator)
	for it.Seek(key); it.ValidForPrefix(key); it.Next() {
		item := it.Item()
		key := string(item.Key()[1:])
		idex := strings.Index(key, NodeSeparator)
		if idex < 1 {
			continue
		}
		key = key[:idex]
		list[key] = struct{}{}
	}
	return len(list)
}

func (ndb *BadgerNode) NodeCount() int {
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
