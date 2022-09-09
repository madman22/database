package database

import (
	"bytes"
	"encoding/gob"
	"errors"
	"sync"
)

// Queue receiver is responsible for closing the channel
func (db *BadgerDB) Queue() (chan DatabaseSubscribeItem, error) {
	dbr := make(chan DatabaseSubscribeItem)
	go func() {
		for dec := range dbr {
			switch dec.Op {
			case DatabaseWrite:
				if err := setValueBadger(db.db, db.Version(), db.subs, dec.Prefix, dec.ID, dec.Data); err != nil {
					continue
				}
			case DatabaseDelete:
				if err := deleteBadger(db.db, db.Version(), db.subs, dec.Prefix, dec.ID); err != nil {
					continue
				}
			case DatabaseDropPrefix:
				if err := db.db.DropPrefix([]byte(dec.ID)); err != nil {
					continue
				}
			}
		}
	}()
	return dbr, nil
}

// Queue receiver is responsible for closing the channel
func (db *BadgerNode) Queue() (chan DatabaseSubscribeItem, error) {
	dbr := make(chan DatabaseSubscribeItem)
	go func() {
		for dec := range dbr {
			switch dec.Op {
			case DatabaseWrite:
				//if err := setValueBadger(db.db, db.Version(), db.subs, dec.Prefix, dec.ID, dec.Data); err != nil {
				if err := setValueBadger(db.db, db.Version(), nil, dec.Prefix, dec.ID, dec.Data); err != nil {
					continue
				}
			case DatabaseDelete:
				//if err := deleteBadger(db.db, db.Version(), db.subs, dec.Prefix, dec.ID); err != nil {
				if err := deleteBadger(db.db, db.Version(), nil, dec.Prefix, dec.ID); err != nil {
					continue
				}
			case DatabaseDropPrefix:
				if err := db.db.DropPrefix([]byte(dec.ID)); err != nil {
					continue
				}
			}
		}
	}()
	return dbr, nil
}

// Queue receiver is responsible for closing the channel
func (db *BadgerExpiry) Queue() (chan DatabaseSubscribeItem, error) {
	dbr := make(chan DatabaseSubscribeItem)
	go func() {
		for dec := range dbr {
			switch dec.Op {
			case DatabaseWrite:
				if err := setValueBadger(db.db, db.Version(), nil, dec.Prefix, dec.ID, dec.Data); err != nil {
					//if err := setValueBadger(db.db, db.Version(), db.subs, dec.Prefix, dec.ID, dec.Data); err != nil {
					continue
				}
			case DatabaseDelete:
				//if err := deleteBadger(db.db, db.Version(), db.subs, dec.Prefix, dec.ID); err != nil {
				if err := deleteBadger(db.db, db.Version(), nil, dec.Prefix, dec.ID); err != nil {
					continue
				}
			case DatabaseDropPrefix:
				if err := db.db.DropPrefix([]byte(dec.ID)); err != nil {
					continue
				}
			}
		}
	}()
	return dbr, nil
}

type dbSubscribe struct {
	subs    map[chan DatabaseSubscribeItem]struct{}
	queue   chan DatabaseSubscribeItem
	parent  chan DatabaseSubscribeItem
	subsmux sync.RWMutex
}

func (dsi *DatabaseSubscribeItem) DecodeGob(i interface{}) error {
	return gob.NewDecoder(bytes.NewBuffer(dsi.Data)).Decode(i)
}

type Operation int

const (
	DatabaseWrite Operation = iota
	DatabaseDelete
	DatabaseDropPrefix
)

func newDbSubscribe(parent chan DatabaseSubscribeItem) *dbSubscribe {
	var dbs dbSubscribe
	dbs.parent = parent
	dbs.queue = make(chan DatabaseSubscribeItem, 1000)
	dbs.subs = make(map[chan DatabaseSubscribeItem]struct{})
	go func() {
		for dec := range dbs.queue {
			if dbs.parent != nil {
				dbs.parent <- dec
			}
			dbs.subsmux.RLock()
			if len(dbs.subs) < 1 {
				dbs.subsmux.RUnlock()
				continue
			}
			for c := range dbs.subs {
				go func(c chan DatabaseSubscribeItem, dec DatabaseSubscribeItem) {
					defer func() {
						if r := recover(); r != nil {
							dbs.subsmux.Lock()
							if _, ok := dbs.subs[c]; ok {
								delete(dbs.subs, c)
							}
							dbs.subsmux.Unlock()
							return
						}
					}()
					c <- dec
				}(c, dec)
			}
			dbs.subsmux.RUnlock()
		}
	}()
	return &dbs
}

func (dbs *dbSubscribe) Stop() error {
	if dbs.queue != nil {
		close(dbs.queue)
	}
	return nil
}

func (db *BadgerDB) Subscribe(c chan DatabaseSubscribeItem) error {
	if db.subs == nil {
		return errors.New("Subscriptions not built")
	}
	return db.subs.Subscribe(c)
}

func (db *BadgerDB) UnSubscribe(c chan DatabaseSubscribeItem) error {
	if db.subs == nil {
		return errors.New("Subscriptions not built")
	}
	return db.subs.UnSubscribe(c)
}

func (db *BadgerNode) Subscribe(c chan DatabaseSubscribeItem) error {
	/*if db.subs == nil {
		return errors.New("Subscriptions not built")
	}
	return db.subs.Subscribe(c)*/
	return nil
}

func (db *BadgerNode) UnSubscribe(c chan DatabaseSubscribeItem) error {
	/*if db.subs == nil {
		return errors.New("Subscriptions not built")
	}
	return db.subs.UnSubscribe(c)*/
	return nil
}

func (db *BadgerExpiry) Subscribe(c chan DatabaseSubscribeItem) error {
	/*if db.subs == nil {
		return errors.New("Subscriptions not built")
	}
	return db.subs.Subscribe(c)*/
	return nil
}

func (db *BadgerExpiry) UnSubscribe(c chan DatabaseSubscribeItem) error {
	/*if db.subs == nil {
		return errors.New("Subscriptions not built")
	}
	return db.subs.UnSubscribe(c)*/
	return nil
}

func (dbs *dbSubscribe) Subscribe(c chan DatabaseSubscribeItem) error {
	dbs.subsmux.Lock()
	defer dbs.subsmux.Unlock()
	if dbs.subs == nil {
		dbs.subs = make(map[chan DatabaseSubscribeItem]struct{})
	}
	dbs.subs[c] = struct{}{}
	return nil
}

func (dbs *dbSubscribe) UnSubscribe(c chan DatabaseSubscribeItem) error {
	dbs.subsmux.Lock()
	defer dbs.subsmux.Unlock()
	if dbs.subs == nil {
		return errors.New("nil subs map")
	}
	if _, ok := dbs.subs[c]; ok {
		delete(dbs.subs, c)
		return nil
	}
	return errors.New("channel not found in map")
}

func (dbs *dbSubscribe) Send(id, prefix string, data []byte) error {
	dbs.subsmux.RLock()
	defer dbs.subsmux.RUnlock()
	if dbs.queue == nil {
		return errors.New("queue nil")
	}
	var write DatabaseSubscribeItem
	write.ID = id
	write.Prefix = prefix
	write.Data = data
	write.Op = DatabaseWrite
	dbs.queue <- write
	return nil
}

func (dbs *dbSubscribe) Delete(id, prefix string) error {
	dbs.subsmux.RLock()
	defer dbs.subsmux.RUnlock()
	if dbs.queue == nil {
		return errors.New("queue nil")
	}
	var del DatabaseSubscribeItem
	del.Prefix = prefix
	del.Op = DatabaseDelete
	del.ID = id
	dbs.queue <- del
	return nil
}

func (dbs *dbSubscribe) DropPrefix(prefix string) error {
	var del DatabaseSubscribeItem
	del.Op = DatabaseDropPrefix
	del.ID = prefix
	dbs.queue <- del
	return nil
}
