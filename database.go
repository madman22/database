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
var ErrorInvalidKeyCharacter = errors.New("Cannot use " + NodeSeparator + " as part of a key.  That is used for disignating nodes aka buckets")
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
	//Backup(string) error
	Backup() ([]byte, error)
}

type DatabaseReader interface {
	Get(string, interface{}) error
	GetValue(string) ([]byte, error)
	GetAll() (List, error)
}

type DatabaseWriter interface {
	Set(string, interface{}) error
	SetValue(string, []byte) error
	Delete(string) error
	Merge(string, MergeFunc) error
}

//type MergeFunc func([]byte, []byte) error
type MergeFunc func([]byte) ([]byte, error)

type DatabaseNode interface {
	GetNodes() ([]string, error)
	NewNode(string) (Database, error)
	DropNode(string) error
}

/*
type MergeF func(Decoder) (Encoder, error)

type Encoder interface {
	Interface() interface{}
	Encode([]byte) error
}

type gobEncoder struct {
	i interface{}
	enc *gob.Encoder
}

func newEncoder(i interface{} Encoder) {
	var ge gobEncoder
	ge.i = i
	return &ge
}

func (ge *gobEncoder) Encode(data []byte) error {
	if gd.dec == nil {
		return errors.New("Decoder not built!")
	}
	if len(gd.data) < 1 {
		return errors.New("Empty data")
	}
	gob.NewEncoder(\)
	return gd.dec.Decode(i)
}*/

type List map[string]Decoder

type Decoder interface {
	Data() []byte
	Decode(interface{}) error
}

type gobDecoder struct {
	data []byte
	dec  *gob.Decoder
}

func newDecoder(data []byte) Decoder {
	var gd gobDecoder
	gd.data = make([]byte, len(data))
	copy(gd.data, data)
	gd.dec = gob.NewDecoder(bytes.NewBuffer(data))
	return &gd
}

func (gd *gobDecoder) Data() []byte {
	return gd.data
}

func (gd *gobDecoder) Decode(i interface{}) error {
	if gd.dec == nil {
		return errors.New("Decoder not built!")
	}
	if len(gd.data) < 1 {
		return errors.New("Empty data")
	}
	return gd.dec.Decode(i)
}

func (l List) Add(key string, content []byte) error {
	//func (l List) AddContent(key string, content []byte) error {
	if l == nil {
		l = make(List)
	}
	if len(key) < 1 {
		return ErrorKeysNil
	}
	//body := make([]byte, len(content))
	//copy(body, content)
	//l[key] = body
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
		//if err := json.Unmarshal(val, i); err != nil {
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

/*func (bdb *BadgerDB) Get(id string, i interface{}) error {
	if bdb.db == nil {
		return ErrorDatabaseNil
	}
	if err := bdb.db.View(buildViewFuncIface(id, i)); err != nil {
		return err
	}
	return nil
}

func buildViewFuncIface(id string, i interface{}) func(*badger.Txn) error {
	return func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(id))
		if err != nil {
			return err
		}
		f := func(val []byte) error {
			if nb, ok := i.([]byte); ok {
				if cap(nb) < len(val) {
					nb = make([]byte, len(val))
				}
				copy(nb, val)
				return nil
			}
			//if err := json.Unmarshal(val, i); err != nil {
			dcr := gob.NewDecoder(bytes.NewReader(val))
			if err := dcr.Decode(i); err != nil {
				return err
			}
			return nil
		}
		if err := item.Value(f); err != nil {
			return err
		}
		return nil
	}
}*/

//func buildUnMarshalValue(i interface{}) func([]byte) error {
//	return
//}

/*func buildViewFuncIface(id string, i interface{}) func(*badger.Txn) error {
	return func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(id))
		if err != nil {
			return err
		}
		if err := item.Value(buildUnMarshalValue(i)); err != nil {
			return err
		}
		return nil
	}
}

func buildUnMarshalValue(i interface{}) func([]byte) error {
	return func(val []byte) error {
		if nb, ok := i.([]byte); ok {
			if cap(nb) < len(val) {
				nb = make([]byte, len(val))
			}
			copy(nb, val)
			return nil
		}
		//if err := json.Unmarshal(val, i); err != nil {
		dcr := gob.NewDecoder(bytes.NewReader(val))
		if err := dcr.Decode(i); err != nil {
			return err
		}
		return nil
	}
}*/

//func (bdb *BadgerDB) GetValue(id string, content []byte) error {
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

/*func (bdb *BadgerDB) GetValue(id string, content []byte) error {
	if bdb.db == nil {
		return ErrorDatabaseNil
	}
	if err := bdb.db.View(buildViewFunc(id, content)); err != nil {
		return err
	}
	return nil
}

func buildViewFunc(id string, content []byte) func(*badger.Txn) error {
	return func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(id))
		if err != nil {
			return err
		}
		if err := item.Value(buildCopyValue(content)); err != nil {
			return err
		}
		return nil
	}
}

func buildCopyValue(content []byte) func([]byte) error {
	return func(val []byte) error {
		//if cap(content) < len(val) {
		//	content = make([]byte, len(val))
		//}
		if cap(content) != len(val) {
			content = make([]byte, len(val))
		}
		copy(content, val)
		return nil
	}
}*/

func (bdb *BadgerDB) Set(id string, i interface{}) error {
	if bdb.db == nil {
		return ErrorDatabaseNil
	}
	//if err := bdb.db.Update(buildUpdateFuncIface(id, i)); err != nil {
	//	return err
	//}

	f := func(txn *badger.Txn) error {
		buf := &bytes.Buffer{}
		enc := gob.NewEncoder(buf)
		if err := enc.Encode(i); err != nil {
			return err
		}
		ent := badger.NewEntry([]byte(id), buf.Bytes())
		return txn.SetEntry(ent)
	}

	return bdb.db.Update(f)
}

/*func buildUpdateFuncIface(id string, i interface{}) func(*badger.Txn) error {
	return func(txn *badger.Txn) error {
		buf := &bytes.Buffer{}
		enc := gob.NewEncoder(buf)
		if err := enc.Encode(i); err != nil {
			return err
		}
		ent := badger.NewEntry([]byte(id), buf.Bytes())
		return txn.SetEntry(ent)
	}
}*/

/*func buildUpdateFunc(id string, content []byte) func(*badger.Txn) error {
	return func(txn *badger.Txn) error {
		ent := badger.NewEntry([]byte(id), content)
		return txn.SetEntry(ent)
	}
}*/

func (bdb *BadgerDB) SetValue(id string, content []byte) error {
	if bdb.db == nil {
		return ErrorDatabaseNil
	}
	//if err := bdb.db.Update(buildUpdateFunc(id, content)); err != nil {
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
	/*f, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer f.Close()
	l, err := bdb.db.Backup(f, 0)
	if err != nil {
		return err
	}
	if l < 1 {
		return errors.New("Nothing saved to database!")
	}
	return nil*/
}

/*func (bdb *BadgerDB) BackupOld(filename string) error {
	if bdb.db == nil {
		return ErrorDatabaseNil
	}
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer f.Close()
	l, err := bdb.db.Backup(f, 0)
	if err != nil {
		return err
	}
	if l < 1 {
		return errors.New("Nothing saved to database!")
	}
	return nil
}*/

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

/*func buildDeleteFunc(id string) func(*badger.Txn) error {
	return func(txn *badger.Txn) error {
		return txn.Delete([]byte(id))
	}
}*/

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
	//list := make(List)
	//var list []string
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

//TODO backup only specific node prefix
func (ndb *BadgerNode) Backup() ([]byte, error) {
	if ndb.db == nil {
		return []byte{}, ErrorDatabaseNil
	}
	return ndb.Backup()
}

func (ndb *BadgerNode) BackupBad(filename string) error {
	if ndb.db == nil {
		return ErrorDatabaseNil
	}
	return ndb.BackupBad(filename)
}

func (ndb *BadgerNode) Close() error {
	return ErrorNotClosable
}

func (ndb *BadgerNode) Delete(id string) error {
	if ndb.db == nil {
		return ErrorDatabaseNil
	}
	//if err := ndb.db.Update(buildDeleteFunc(ndb.prefix + id)); err != nil {
	//	return err
	//}
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
	/*b, err := json.Marshal(i)
	if err != nil {
		return err
	}*/
	//if err := ndb.db.Update(buildUpdateFuncIface(ndb.prefix+id, i)); err != nil {
	//	return err
	//}
	f := func(txn *badger.Txn) error {
		buf := &bytes.Buffer{}
		enc := gob.NewEncoder(buf)
		if err := enc.Encode(i); err != nil {
			return err
		}
		ent := badger.NewEntry([]byte(ndb.prefix+id), buf.Bytes())
		return txn.SetEntry(ent)
	}
	return ndb.db.Update(f)
}

func (ndb *BadgerNode) SetValue(id string, content []byte) error {
	if ndb.db == nil {
		return ErrorDatabaseNil
	}
	//if err := ndb.db.Update(buildUpdateFunc(ndb.prefix+id, content)); err != nil {
	//	return err
	//}
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
		//if err := json.Unmarshal(val, i); err != nil {
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

/*func (ndb *BadgerNode) Get(id string, i interface{}) error {
	if ndb.db == nil {
		return ErrorDatabaseNil
	}
	prefix := ndb.prefix+id
	if err := ndb.db.View(buildViewFuncIface(ndb.prefix+id, i)); err != nil {
		return err
	}
	return nil
}
*/

//func (ndb *BadgerNode) GetValue(id string, content []byte) error {
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
	//m := db.db.GetMergeOperator(id, f, 100*time.Millisecond)
	//defer m.Stop()
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
		//if err := item.Value(buildCopyValue(content)); err != nil {
		if err := item.Value(ff); err != nil {
			return err
		}
	}
	//var newdata []byte
	//if err := f(content, newdata); err != nil {
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
	//m.Add()
	return nil
}

func (db *BadgerNode) Merge(id string, f MergeFunc) error {
	if len(id) < 1 {
		return ErrorMissingID
	}
	//m := db.db.GetMergeOperator(id, f, 100*time.Millisecond)
	//defer m.Stop()
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
	/*var newdata []byte
	if err := f(content, newdata); err != nil {
		return err
	}*/
	newdata, err := f(content)
	if err != nil {
		return err
	}
	if len(newdata) < 1 {
		return nil
	}
	//fmt.Println("merging new bytes:" + string(newdata))
	if err := txn.Set([]byte(db.prefix+id), newdata); err != nil {
		return err
	}
	if err := txn.Commit(); err != nil {
		return err
	}
	//m.Add()
	return nil
}
