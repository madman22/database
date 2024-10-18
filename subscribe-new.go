package database

/*
import (
	"errors"
	"sync"
)



type Subscription interface {
	Subscribe(chan DatabaseSubscribeItem) error
	UnSubscribe(chan DatabaseSubscribeItem) error
}

type Subscriber interface {
	Queue() (chan DatabaseSubscribeItem, error)
}

type DatabaseSubscribeItem struct {
	ID     string
	Prefix string
	Op     Operation
	Data   []byte
}

type SubscribeItem struct {
	Op   Operation
	ID   string
	Data []byte
}

type dbSub struct {
	subs   map[string]prefixSubs
	submux sync.RWMutex
}

type prefixSubs map[chan SubscribeItem]struct{}

func (ds *dbSub) Subscribe(prefix string, c chan SubscribeItem) error {
	ds.submux.Lock()
	defer ds.submux.Unlock()
	if ds.subs == nil {
		ds.subs = make(map[string]prefixSubs)
	}
	if list, ok := ds.subs[prefix]; ok {
		if _, ok := list[c]; ok {
			return nil
		} else {
			list[c] = struct{}{}
			ds.subs[prefix] = list
		}
	} else {
		list = make(prefixSubs)
		list[c] = struct{}{}
		ds.subs[prefix] = list
	}
	return nil
}

func (ds *dbSub) UnSubscribe(prefix string, c chan SubscribeItem) error {
	ds.submux.Lock()
	defer ds.submux.Unlock()
	if ds.subs == nil {
		return errors.New("Nil Subs Map")
	}
	if list, ok := ds.subs[prefix]; !ok {
		return errors.New("No Subscribers for prefix")
	} else {
		if _, ok := list[c]; !ok {
			return errors.New("Not Subscribed")
		} else {
			delete(list, c)
			ds.subs[prefix] = list
		}
	}
	return nil
}

func (ds *dbSub) Send(prefix, id string, data []byte) {
	ds.submux.RLock()
	defer ds.submux.RUnlock()
	if ds.subs == nil {
		return
	}
	if list, ok := ds.subs[prefix]; !ok {
		return
	} else {
		subItem := SubscribeItem{ID: id, Data: data, Op: DatabaseWrite}
		for c := range list {
			c <- subItem
		}
	}
}

func (ds *dbSub) Delete(prefix, id string) {
	ds.submux.RLock()
	defer ds.submux.RUnlock()
	if ds.subs == nil {
		return
	}
	if list, ok := ds.subs[prefix]; !ok {
		return
	} else {
		subItem := SubscribeItem{ID: id, Op: DatabaseDelete}
		for c := range list {
			c <- subItem
		}
	}
}

func (ds *dbSub) DropPrefix(prefix string) {
	ds.submux.RLock()
	defer ds.submux.RUnlock()
	if ds.subs == nil {
		return
	}
	if list, ok := ds.subs[prefix]; !ok {
		return
	} else {
		subItem := SubscribeItem{Op: DatabaseDropPrefix, ID: prefix}
		for c := range list {
			c <- subItem
		}
	}
}

func (db *BadgerDB) QueueChan() chan SubscribeItem {
	c := make(chan SubscribeItem)
	go func() {
		for si := range c {
			switch si.Op {
			case DatabaseWrite:
				setValueBadger(db.db, db.Version(), nil, "", si.ID, si.Data)
			case DatabaseDelete:
				deleteBadger(db.db, db.Version(), nil, "", si.ID)
			case DatabaseDropPrefix:
				db.db.DropPrefix([]byte(si.ID))
			}
		}
	}()
	return c
}

func (db *BadgerNode) QueueChan() chan SubscribeItem {
	c := make(chan SubscribeItem)
	go func() {
		for si := range c {
			switch si.Op {
			case DatabaseWrite:
				setValueBadger(db.db, db.Version(), nil, db.prefix, si.ID, si.Data)
			case DatabaseDelete:
				deleteBadger(db.db, db.Version(), nil, db.prefix, si.ID)
			case DatabaseDropPrefix:
				db.db.DropPrefix([]byte(si.ID))
			}
		}
	}()
	return c
}

func (db *BadgerExpiry) QueueChan() chan SubscribeItem {
	c := make(chan SubscribeItem)
	go func() {
		for si := range c {
			switch si.Op {
			case DatabaseWrite:
				setValueBadger(db.db, db.Version(), nil, db.prefix, si.ID, si.Data)
			case DatabaseDelete:
				deleteBadger(db.db, db.Version(), nil, db.prefix, si.ID)
			case DatabaseDropPrefix:
				db.db.DropPrefix([]byte(si.ID))
			}
		}
	}()
	return c
}
*/
