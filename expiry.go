package database

import (
	"bytes"
	"encoding/gob"

	//"errors"
	"strings"
	"time"

	badger "github.com/dgraph-io/badger/v3"
	"github.com/tevino/abool"
)

type BadgerExpiry struct {
	db       *badger.DB
	timeout  time.Duration
	prefix   string
	id       string
	version  *DatabaseVersioner
	readonly *abool.AtomicBool
	//subs     *dbSubscribe
}

func newExpiryNode(db *badger.DB, prefix, id string, dur time.Duration, dbv *DatabaseVersioner, readonly *abool.AtomicBool, subs *dbSubscribe) (Database, error) {
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
	//de.subs = subs
	de.readonly = readonly
	de.id = id
	de.timeout = dur
	de.db = db
	de.version = dbv
	de.prefix = prefix + NodeSeparator + id + NodeSeparator
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
		//ndb.subs = bdb.subs
		ndb.prefix = ""
		ndb.db = bdb.db
		ndb.id = ""
		return &ndb, nil
	} else if lindex == 0 {
		var ndb BadgerNode
		//ndb.subs = bdb.subs
		ndb.id = nid[1:]
		ndb.prefix = NodeSeparator + ndb.id
		ndb.db = bdb.db
		return &ndb, nil
	}
	id := nid[lindex:]
	if len(id) < 1 {
		var ndb BadgerNode
		//ndb.subs = bdb.subs
		ndb.prefix = ""
		ndb.db = bdb.db
		ndb.id = ""
		return &ndb, nil
	}
	var ndb BadgerNode
	//ndb.subs = bdb.subs
	ndb.prefix = nid + NodeSeparator
	ndb.db = bdb.db
	ndb.id = id
	ndb.version = bdb.version
	return &ndb, nil
}

func (ndb *BadgerExpiry) Close() error {
	return ErrorNotClosable
}

func (ndb *BadgerExpiry) Delete(id string) error {
	if ndb.readonly != nil {
		if ndb.readonly.IsSet() {
			return nil
		}
	}
	return deleteBadger(ndb.db, ndb.version.Version(), nil, ndb.prefix, id)
}

func (ndb *BadgerExpiry) DropNode(id string) error {
	if ndb.db == nil {
		return ErrorDatabaseNil
	}
	if ndb.readonly != nil {
		if ndb.readonly.IsSet() {
			return nil
		}
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
	if len(id) < 1 {
		return ErrorInvalidID
	}
	if ndb.readonly != nil {
		if ndb.readonly.IsSet() {
			return nil
		}
	}
	if ndb.version.Version() > Version1 {
		id = EntityPrefix + id
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

	/*if ndb.subs != nil {
		if err := ndb.subs.Send(id, ndb.prefix, buf.Bytes()); err != nil {
			return err
		}
	}*/
	return ndb.db.Update(f)
}

func (ndb *BadgerExpiry) SetValue(id string, content []byte) error {
	if ndb.db == nil {
		return ErrorDatabaseNil
	}
	if len(id) < 1 {
		return ErrorInvalidID
	}
	if ndb.readonly != nil {
		if ndb.readonly.IsSet() {
			return nil
		}
	}
	if ndb.version.Version() > Version1 {
		id = EntityPrefix + id
	}
	err := ndb.db.Update(func(txn *badger.Txn) error {
		ent := badger.NewEntry([]byte(ndb.prefix+id), content).WithTTL(ndb.timeout)
		return txn.SetEntry(ent)
	})
	if err != nil {
		return err
	}
	/*if ndb.subs != nil {
		if err := ndb.subs.Send(id, ndb.prefix, content); err != nil {
			return err
		}
	}*/
	return nil
}

func (ndb *BadgerExpiry) Get(id string, i interface{}) error {
	return getBadger(ndb.db, ndb.Version(), ndb.prefix, id, i)
}

func (ndb *BadgerExpiry) GetAndDelete(id string, i interface{}) error {
	if ndb.readonly != nil {
		if ndb.readonly.IsSet() {
			//return nil
			return getBadger(ndb.db, ndb.version.Version(), ndb.prefix, id, i)
		}
	}
	//return getAndDeleteBadger(ndb.db, ndb.version.Version(), ndb.subs, ndb.prefix, id, i)
	return getAndDeleteBadger(ndb.db, ndb.version.Version(), nil, ndb.prefix, id, i)
}

func (ndb *BadgerExpiry) GetValue(id string) ([]byte, error) {
	return getValueBadger(ndb.db, ndb.version.Version(), ndb.prefix, id)
}

func (ndb *BadgerExpiry) GetAll() (List, error) {
	return getAllBadger(ndb.db, ndb.version.Version(), ndb.prefix)
}

func (ndb *BadgerExpiry) GetNodes() ([]string, error) {
	return getNodesBadger(ndb.db, ndb.version.Version(), ndb.prefix)
}

func (ndb *BadgerExpiry) NewNode(id string) (Database, error) {
	if len(id) < 1 || strings.Contains(id, NodeSeparator) {
		return nil, ErrorInvalidID
	}
	if ndb.db == nil {
		return nil, ErrorDatabaseNil
	}
	var node BadgerNode
	//node.subs = newDbSubscribe(ndb.subs.queue)
	node.readonly = ndb.readonly
	node.db = ndb.db
	node.id = id
	node.version = ndb.version
	node.prefix = ndb.prefix + NodeSeparator + id + NodeSeparator
	return &node, nil
}

func (db *BadgerExpiry) Merge(id string, f MergeFunc) error {
	if db.readonly != nil {
		if db.readonly.IsSet() {
			return nil
		}
	}
	//return mergeBadger(db.db, db.version.Version(), db.prefix, id, f, db.subs)
	return mergeBadger(db.db, db.version.Version(), db.prefix, id, f, nil)
}

func (ndb *BadgerExpiry) Length() int {
	return lenBadger(ndb.db, ndb.version.Version(), ndb.prefix)
}

func (ndb *BadgerExpiry) NodeCount() int {
	return nodeCountBadger(ndb.db, ndb.version.Version(), ndb.prefix)
}

func (dbd *BadgerExpiry) Range(page, count int) (List, error) {
	return rangeBadger(dbd.db, dbd.version.Version(), dbd.prefix, page, count)
}

func (dbd *BadgerExpiry) Pages(count int) int {
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

func (dbd *BadgerExpiry) NewExpiryNode(id string, dur time.Duration, dbv *DatabaseVersioner) (Database, error) {
	//return newExpiryNode(dbd.db, dbd.prefix, id, dur, dbv, dbd.readonly, dbd.subs)
	return newExpiryNode(dbd.db, dbd.prefix, id, dur, dbv, dbd.readonly, nil)
}

func (db *BadgerExpiry) GetIDs() ([]string, error) {
	return getAllIDsBadger(db.db, db.Version(), db.prefix)
}
