package database

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"iter"
	"strings"
)

func DecodeList[T any](l List) (map[string]T, error) {
	out := make(map[string]T)
	for id, item := range l {
		var gen T
		if err := item.Decode(&gen); err != nil {
			return out, err
		}
		out[id] = gen
	}
	return out, nil
}

func DecodeListToSlice[T any](l List) ([]T, error) {
	var out []T
	for _, item := range l {
		var gen T
		if err := item.Decode(&gen); err != nil {
			return out, err
		}
	}
	return out, nil
}

func GetAll[T any](db Database) (map[string]T, error) {
	output := make(map[string]T)
	f := func(id string, item Decoder) error {
		var gen T
		if err := item.Decode(&gen); err != nil {
			return err
		}
		output[id] = gen
		return nil
	}
	if err := db.ForEach(f); err != nil {
		return output, err
	}
	return output, nil
}

func GetAllSlice[T any](db Database) ([]T, error) {
	var output []T
	f := func(id string, item Decoder) error {
		var gen T
		if err := item.Decode(&gen); err != nil {
			return err
		}
		output = append(output, gen)
		return nil
	}
	if err := db.ForEach(f); err != nil {
		return output, err
	}
	return output, nil
}

type GenericNode[T any] struct {
	db *BadgerNode
}

func NewGenericNode[T any](db Database) (*GenericNode[T], error) {

	bdb, ok := db.(*BadgerNode)
	if !ok {
		dbb, ok := db.(*BadgerDB)
		if !ok {
			return nil, ErrorInvalidVersion
		}
		t := fmt.Sprintf("%T", new(T))
		node, err := dbb.NewNode(t)
		if err != nil {
			return nil, err
		}
		bdb, ok = node.(*BadgerNode)
		if !ok {
			return nil, ErrorInvalidVersion
		}
	}

	gn := &GenericNode[T]{bdb}
	return gn, nil
}

func (gn *GenericNode[T]) Get(id string) (T, error) {
	var item T

	if err := gn.db.Get(id, &item); err != nil {
		return item, err
	}
	return item, nil
}

func (gn *GenericNode[T]) GetAndDelete(id string) (T, error) {
	var item T
	if err := gn.db.GetAndDelete(id, &item); err != nil {
		return item, err
	}
	return item, nil
}

func (gn *GenericNode[T]) Set(id string, item T) error {
	return gn.db.Set(id, item)
}

func (gn *GenericNode[T]) ReadAll() iter.Seq2[string, T] {
	return func(yield func(string, T) bool) {
		txn := gn.db.db.NewTransaction(false)
		defer txn.Discard()
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		if len(gn.db.prefix) > 0 {
			opts.Prefix = []byte(gn.db.prefix + EntityPrefix)
		} else {
			opts.Prefix = []byte(EntityPrefix)
		}
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			var key string
			var gt T
			if len(gn.db.prefix) > 0 {
				key = strings.TrimPrefix(string(item.Key()), gn.db.prefix+EntityPrefix)
			} else {
				key = strings.TrimPrefix(string(item.Key()), EntityPrefix)
			}
			if err := item.Value(func(val []byte) error {
				b := make([]byte, len(val))
				copy(b, val)
				if err := gob.NewDecoder(bytes.NewBuffer(b)).Decode(&gt); err != nil {
					return err
				}
				return nil
			}); err != nil {
				continue
			}
			if !yield(key, gt) {
				return
			}
		}
	}
}
