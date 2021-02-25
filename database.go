package database

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"strings"
	"time"

	badger "github.com/dgraph-io/badger/v2"
)

const NodeSeparator = `|`
const EntityPrefix = `**`
const SettingsPrefix = `$$`

const VersionID = SettingsPrefix + "Version"
const TimeoutID = SettingsPrefix + "Timeout"

var ErrorDatabaseNil = errors.New("Database Nil!")
var ErrorNotImplemented = errors.New("Not Implemented!")
var ErrorNotClosable = errors.New("Node not closable")
var ErrorInvalidKeyCharacter = errors.New("Cannot use " + NodeSeparator + " as part of a key.  That is used for designating nodes")
var ErrorMissingID = errors.New("ID cannot be empty")
var ErrorNotFound = errors.New("Item not found in database")
var ErrorNilValue = errors.New("Nil Interface Value")
var ErrorKeysNil = errors.New("Key Map is nil, try adding elements with Set")
var ErrorInvalidID = errors.New("Cannot use thid ID, conflicts with built-in keys")

type Database interface {
	DatabaseReader
	DatabaseWriter
	DatabaseNode
	DatabaseIO
	DatabaseExpiry
	DatabaseVersion
}

type DatabaseIO interface {
	DatabaseBackups
	Close() error
}

type DatabaseReader interface {
	Get(string, interface{}) error
	GetValue(string) ([]byte, error)
	GetAll() (List, error)
	Length() int
	Range(page, count int) (List, error)
	Pages(int) int
}

type DatabaseWriter interface {
	Set(string, interface{}) error
	SetValue(string, []byte) error
	Delete(string) error
	Clear() error
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

type DatabaseExpiry interface {
	NewExpiryNode(string, time.Duration, *DatabaseVersioner) (Database, error)
}

type DatabaseVersion interface {
	Version() Version
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
	dbname string
	db     *badger.DB
	//gccount uint64
	ctx     context.Context
	cancel  context.CancelFunc
	version *DatabaseVersioner
}

type BadgerNode struct {
	prefix  string
	id      string
	db      *badger.DB
	version *DatabaseVersioner
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

	v := getVersion(db)
	if v == Version1 {
		if lsm, vl := db.Size(); lsm == 0 && vl == 0 {
			v = LatestVersion
		}
	}
	bdb.version = &DatabaseVersioner{ver: v}
	if err := bdb.saveVersion(); err != nil {
		return &bdb, err
	}

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

	v := getVersion(db)
	if v == Version1 {
		if lsm, vl := db.Size(); lsm == 0 && vl == 0 {
			v = LatestVersion
		}
	}
	bdb.version = &DatabaseVersioner{ver: v}
	if err := bdb.saveVersion(); err != nil {
		return &bdb, err
	}

	return &bdb, nil
}

func (bdb *BadgerDB) startGC(ctx context.Context, dur time.Duration) {
	ticker := time.NewTicker(dur)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
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
	return getBadger(bdb.db, bdb.version.Version(), "", id, i)
}

func (bdb *BadgerDB) GetAndDelete(id string, i interface{}) error {
	return getAndDeleteBadger(bdb.db, bdb.version.Version(), "", id, i)
}

func (bdb *BadgerDB) GetValue(id string) ([]byte, error) {
	return getValueBadger(bdb.db, bdb.version.Version(), "", id)
}

func (bdb *BadgerDB) Set(id string, i interface{}) error {
	return setBadger(bdb.db, bdb.version.Version(), "", id, i)
}

func (bdb *BadgerDB) SetValue(id string, content []byte) error {
	return setValueBadger(bdb.db, bdb.version.Version(), "", id, content)
}

func (bdb *BadgerDB) Close() error {
	if bdb.db == nil {
		return ErrorDatabaseNil
	}
	if err := bdb.saveVersion(); err != nil {
		return err
	}
	if bdb.cancel != nil {
		bdb.cancel()
	}
	return bdb.db.Close()
}

func (bdb *BadgerDB) Delete(id string) error {
	return deleteBadger(bdb.db, bdb.version.Version(), "", id)
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
	return getAllBadger(bdb.db, bdb.version.Version(), "")
}

func (bdb *BadgerDB) GetNodes() ([]string, error) {
	return getNodesBadger(bdb.db, bdb.version.Version(), "")
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
	ndb.version = bdb.version
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
		ndb.version = bdb.version
		return &ndb, nil
	} else if lindex == 0 {
		var ndb BadgerNode
		ndb.id = nid[1:]
		ndb.prefix = NodeSeparator + ndb.id
		ndb.db = bdb.db
		ndb.version = bdb.version
		return &ndb, nil
	}
	id := nid[lindex:]
	if len(id) < 1 {
		var ndb BadgerNode
		ndb.prefix = ""
		ndb.db = bdb.db
		ndb.id = ""
		ndb.version = bdb.version
		return &ndb, nil
	}
	var ndb BadgerNode
	ndb.prefix = nid + NodeSeparator
	ndb.db = bdb.db
	ndb.id = id
	ndb.version = bdb.version
	return &ndb, nil
}

func (ndb *BadgerNode) Close() error {
	return ErrorNotClosable
}

func (ndb *BadgerNode) Delete(id string) error {
	return deleteBadger(ndb.db, ndb.version.Version(), ndb.prefix, id)
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
	return setBadger(ndb.db, ndb.version.Version(), ndb.prefix, id, i)
}

func (ndb *BadgerNode) SetValue(id string, content []byte) error {
	return setValueBadger(ndb.db, ndb.version.Version(), ndb.prefix, id, content)
}

func (ndb *BadgerNode) Get(id string, i interface{}) error {
	return getBadger(ndb.db, ndb.version.Version(), ndb.prefix, id, i)
}

func (ndb *BadgerNode) GetAndDelete(id string, i interface{}) error {
	return getAndDeleteBadger(ndb.db, ndb.version.Version(), ndb.prefix, id, i)
}

func (ndb *BadgerNode) GetValue(id string) ([]byte, error) {
	return getValueBadger(ndb.db, ndb.version.Version(), ndb.prefix, id)
}

func (ndb *BadgerNode) GetAll() (List, error) {
	return getAllBadger(ndb.db, ndb.version.Version(), ndb.prefix)
}

func (ndb *BadgerNode) GetNodes() ([]string, error) {
	return getNodesBadger(ndb.db, ndb.version.Version(), ndb.prefix)
}

func (ndb *BadgerNode) NewNode(id string) (Database, error) {
	if len(id) < 1 || strings.Contains(id, NodeSeparator) || strings.HasPrefix(id, EntityPrefix) {
		return nil, ErrorInvalidID
	}
	if ndb.db == nil {
		return nil, ErrorDatabaseNil
	}
	var node BadgerNode
	node.db = ndb.db
	node.id = id
	node.version = ndb.version
	node.prefix = ndb.prefix + NodeSeparator + id + NodeSeparator
	return &node, nil
}

func (db *BadgerDB) Merge(id string, f MergeFunc) error {
	return mergeBadger(db.db, db.version.Version(), "", id, f)
}

func (db *BadgerNode) Merge(id string, f MergeFunc) error {
	return mergeBadger(db.db, db.version.Version(), db.prefix, id, f)
}

func (bdb *BadgerDB) Length() int {
	return lenBadger(bdb.db, bdb.version.Version(), "")
}

func (ndb *BadgerNode) Length() int {
	return lenBadger(ndb.db, ndb.version.Version(), ndb.prefix)
}

func (bdb *BadgerDB) NodeCount() int {
	return nodeCountBadger(bdb.db, bdb.version.Version(), "")
}

func (ndb *BadgerNode) NodeCount() int {
	return nodeCountBadger(ndb.db, ndb.version.Version(), ndb.prefix)
}

func (dbd *BadgerDB) Range(page, count int) (List, error) {
	return rangeBadger(dbd.db, dbd.version.Version(), "", page, count)
}

func (nbd *BadgerNode) Range(page, count int) (List, error) {
	return rangeBadger(nbd.db, nbd.version.Version(), nbd.prefix, page, count)
}

func (dbd *BadgerDB) Pages(count int) int {
	l := dbd.Length()
	if count > l || l == 0 || count == 0 {
		return 1
	}
	pages := l / count
	if l%count > 0 {
		pages++
	}
	return pages
}

func (dbd *BadgerNode) Pages(count int) int {
	l := dbd.Length()
	if count > l || l == 0 || count == 0 {
		return 1
	}
	pages := l / count
	if l%count > 0 {
		pages++
	}
	return pages
}

func (dbd *BadgerNode) NewExpiryNode(id string, dur time.Duration, dbv *DatabaseVersioner) (Database, error) {
	return newExpiryNode(dbd.db, dbd.prefix, id, dur, dbv)
}

func (dbd *BadgerDB) NewExpiryNode(id string, dur time.Duration, dbv *DatabaseVersioner) (Database, error) {
	return newExpiryNode(dbd.db, "", id, dur, dbv)
}

func (dbd *BadgerDB) Clear() error {
	return clearDB(dbd)
}

func (dbd *BadgerNode) Clear() error {
	return clearDB(dbd)
}

func (dbd *BadgerExpiry) Clear() error {
	return clearDB(dbd)
}

func clearDB(dbd Database) error {
	items, err := dbd.GetAll()
	if err != nil {
		return err
	}
	var errs []error
	if len(items) > 0 {
		for id, _ := range items {
			if err := dbd.Delete(id); err != nil {
				errs = append(errs, err)
				continue
			}
		}
	}
	nodes, err := dbd.GetNodes()
	if err != nil {
		return err
	}
	if len(nodes) > 0 {
		for _, name := range nodes {
			nd, err := dbd.NewNode(name)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if err := nd.Clear(); err != nil {
				errs = append(errs, err)
				continue
			}
		}
	}
	if len(errs) > 0 {
		var body string
		for _, err := range errs {
			if len(body) > 0 {
				body += " "
			}
			body += err.Error()
		}

	}
	return nil
}
