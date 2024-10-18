package database

import (
	"context"
	"sync"
)

type SubOp int

const (
	DatabaseNOP SubOp = iota
	DatabaseWrite
	DatabaseDelete
	DatabaseDropPrefix
)

type DatabaseSubItem struct {
	Op     SubOp
	Prefix string
	ID     string
	Data   []byte
}

type SubChan chan DatabaseSubItem

type DBSubscriptions struct {
	submux sync.RWMutex
	subs   map[string][]SubChan
}

func (dbs *DBSubscriptions) Subscribe(prefix string, c SubChan) error {
	dbs.submux.Lock()
	defer dbs.submux.Unlock()
	if dbs.subs == nil {
		dbs.subs = make(map[string][]SubChan)
	}
	if subs, ok := dbs.subs[prefix]; ok {
		subs = append(subs, c)
		dbs.subs[prefix] = subs
	} else {
		dbs.subs[prefix] = []SubChan{c}
	}

	return nil
}

func (dbs *DBSubscriptions) UnSubscribe(prefix string, c SubChan) error {
	dbs.submux.Lock()
	defer dbs.submux.Unlock()
	if dbs.subs == nil {
		dbs.subs = make(map[string][]SubChan)
		return nil
	}
	subs, ok := dbs.subs[prefix]
	if !ok {
		return nil
	}
	for i, sub := range subs {
		if sub == c {
			dbs.subs[prefix] = append(subs[:i], subs[i+1:]...)
			break
		}
	}
	return nil
}

func (dbs *DBSubscriptions) GoSend(dsi *DatabaseSubItem) error {
	dbs.submux.RLock()
	if dbs.subs == nil {
		dbs.submux.RUnlock()
		return nil
	}
	if len(dbs.subs) < 1 {
		dbs.submux.RUnlock()
		return nil
	}

	subs, ok := dbs.subs[dsi.Prefix]
	if !ok {
		dbs.submux.RUnlock()
		return nil
	} else {
		dbs.submux.RUnlock()
	}

	for _, subChan := range subs {
		go func(subChan SubChan, dsi DatabaseSubItem) {
			defer func() {
				if r := recover(); r != nil {
					dbs.UnSubscribe(dsi.Prefix, subChan)
				}
			}()
			subChan <- dsi
		}(subChan, *dsi)
	}

	return nil
}

// TODO cancel on ctx close
// TODO check prefix and send to nodes?
// TODO or just append prefix to id?
func (dbd *BadgerDB) CreateSubscriber(ctx context.Context) SubChan {
	c := make(SubChan)
	go func() {
		for dbi := range c {
			switch dbi.Op {
			case DatabaseWrite:
				dbd.SetValue(dbi.ID, dbi.Data)
			case DatabaseDelete:
				dbd.Delete(dbi.ID)
			case DatabaseDropPrefix:
				dbd.DropNode(dbi.Prefix)
			case DatabaseNOP:
				return
			default:
				continue
			}
		}
	}()
	return c
}

/*func NewSubscription(db Database) SubChan {
	c := make(SubChan)
	prefix := db.Prefix()

}

*/
