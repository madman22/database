package database

import (
	"archive/zip"
	"bytes"
	"encoding/gob"
	"io"
	"strings"

	"github.com/dgraph-io/badger/v3"
	//"github.com/dgraph-io/badger"

	//"bytes"
	//"crypto/sha256"
	//"encoding/gob"
	"errors"
	//"io"
	//"strings"
	//"time"

	"github.com/twinj/uuid"
)

type DatabaseBackups interface {
	Backup(*zip.Writer) error
	Restore(*zip.Reader) error
	BackupPrefixes([]string, *zip.Writer) error
	RestorePrefixes([]string, *zip.Reader) error
}

type BackupInfo struct {
	//Nodes        []string
	FilenameToId map[string]string
	Version      Version
}

/*type backupNodeInfo struct {
	NodeName     string
	Prefix       string
	Nodes        []string
	FilenameToId map[string]string
}

type backupExpiryInfo struct {
	NodeName     string
	Prefix       string
	Duration     time.Duration
	Nodes        []string
	FilenameToId map[string]string
}*/

func (db *BadgerDB) BackupPrefixes(prefixes []string, writer *zip.Writer) error {
	return backupDBPrefixes(prefixes, db, writer)
}
func (db *BadgerNode) BackupPrefixes(prefixes []string, writer *zip.Writer) error {
	return backupNodePrefixes(prefixes, db, writer)
}
func (db *BadgerExpiry) BackupPrefixes(prefixes []string, writer *zip.Writer) error {
	return backupExpiryPrefixes(prefixes, db, writer)
}

func (db *BadgerDB) RestorePrefixes(prefixes []string, rdr *zip.Reader) error {
	if len(prefixes) > 0 {
		for _, prefix := range prefixes {
			if err := db.db.DropPrefix([]byte(prefix)); err != nil {
				continue
			}
		}
	}
	return restoreDBPrefixes(prefixes, db, rdr)
}
func (db *BadgerNode) RestorePrefixes(prefixes []string, rdr *zip.Reader) error {
	if len(prefixes) > 0 {
		for _, prefix := range prefixes {
			if err := db.db.DropPrefix([]byte(prefix)); err != nil {
				continue
			}
		}
	}
	return restoreNodePrefixes(prefixes, db, rdr)
}
func (db *BadgerExpiry) RestorePrefixes(prefixes []string, rdr *zip.Reader) error {
	if len(prefixes) > 0 {
		for _, prefix := range prefixes {
			if err := db.db.DropPrefix([]byte(prefix)); err != nil {
				continue
			}
		}
	}
	return restoreExpiryPrefixes(prefixes, db, rdr)
}

func backupDBPrefixes(prefixes []string, db *BadgerDB, w *zip.Writer) error {
	if db == nil {
		return ErrorDatabaseNil
	}
	txn := db.db.NewTransaction(false)
	defer txn.Discard()

	opts := badger.DefaultIteratorOptions
	//opts.PrefetchValues = true
	//opts.Prefix = prefix

	it := txn.NewIterator(opts)
	defer it.Close()
	var errs []error

	var bi BackupInfo
	bi.Version = db.Version()
	bi.FilenameToId = make(map[string]string)

	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		key := string(item.Key())

		if len(prefixes) > 0 {
			good := false
			for _, prefix := range prefixes {
				if strings.HasPrefix(key, prefix) {
					good = true
					break
				}
			}
			if !good {
				continue
			}
		}

		filename := uuid.NewV4().String()
		f, err := w.Create(filename)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if err := item.Value(func(val []byte) error {
			b := make([]byte, len(val))
			copy(b, val)
			f.Write(b)
			return nil
		}); err != nil {
			errs = append(errs, errors.New("key:"+key+" "+err.Error()))
		}
		bi.FilenameToId[filename] = key
		w.Flush()
	}

	if f, err := w.Create("backupInfo"); err != nil {
		return err
	} else {
		enc := gob.NewEncoder(f)
		if err := enc.Encode(&bi); err != nil {
			return err
		}
	}
	w.Flush()
	return nil
}

func backupDB(db *BadgerDB, w *zip.Writer) error {
	if db == nil {
		return ErrorDatabaseNil
	}
	txn := db.db.NewTransaction(false)
	defer txn.Discard()

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = true

	it := txn.NewIterator(opts)
	defer it.Close()
	var errs []error

	var bi BackupInfo
	bi.Version = db.Version()
	bi.FilenameToId = make(map[string]string)

	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		key := string(item.Key())
		filename := uuid.NewV4().String()
		f, err := w.Create(filename)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if err := item.Value(func(val []byte) error {
			b := make([]byte, len(val))
			copy(b, val)
			f.Write(b)
			return nil
		}); err != nil {
			errs = append(errs, errors.New("key:"+key+" "+err.Error()))
		}
		bi.FilenameToId[filename] = key
		w.Flush()
	}

	if f, err := w.Create("backupInfo"); err != nil {
		return err
	} else {
		enc := gob.NewEncoder(f)
		if err := enc.Encode(&bi); err != nil {
			return err
		}
	}
	w.Flush()
	return nil
}

func restoreDBPrefixes(prefixes []string, db *BadgerDB, r *zip.Reader) error {
	if db == nil {
		return ErrorDatabaseNil
	}
	var bi BackupInfo
	for _, file := range r.File {
		if file.Name == "backupInfo" {
			rdr, err := file.Open()
			if err != nil {
				return err
			}
			dec := gob.NewDecoder(rdr)
			if err := dec.Decode(&bi); err != nil {
				rdr.Close()
				return err
			}
			rdr.Close()
			break
		}
	}
	if len(bi.FilenameToId) < 1 {
		return nil
	}

	txn := db.db.NewTransaction(true)
	//defer txn.Discard()

	nv := db.Version()

	//defer txn.Commit()

	var errs []error
	for _, f := range r.File {
		if f.Name == "backupInfo" {
			continue
		}
		id, ok := bi.FilenameToId[f.Name]
		if !ok {
			errs = append(errs, errors.New("Missing ID for file:"+f.Name))
			continue
		}

		if len(prefixes) > 0 {
			good := false
			for _, prefix := range prefixes {
				if strings.HasPrefix(id, prefix) {
					good = true
					break
				}
			}
			if !good {
				continue
			}
		}

		if bi.Version < Version2 && nv > Version1 && !strings.HasPrefix(id, SettingsPrefix) {
			npos := strings.LastIndex(id, NodeSeparator)
			if npos < 1 || npos >= len(id) {
				id = EntityPrefix + id
			} else {
				id = id[:npos+1] + EntityPrefix + id[npos+1:]
			}
		}
		rdr, err := f.Open()
		if err != nil {
			errs = append(errs, errors.New(err.Error()+":"+f.Name))
			continue
		}
		data := new(bytes.Buffer)
		_, err = io.Copy(data, rdr)
		rdr.Close()
		if err != nil {
			errs = append(errs, errors.New(err.Error()+":"+f.Name))
			continue
		}
		if err := txn.Set([]byte(id), data.Bytes()); err != nil {
			if err == badger.ErrTxnTooBig {
				txn.Commit()
				txn = db.db.NewTransaction(true)
				if err := txn.Set([]byte(id), data.Bytes()); err != nil {
					errs = append(errs, errors.New(err.Error()+":"+f.Name))
					continue
				}
			} else {
				errs = append(errs, errors.New(err.Error()+":"+f.Name))
				continue
			}
		}
	}
	txn.Commit()
	if len(errs) > 0 {
		var body string
		for _, err := range errs {
			if len(body) > 0 {
				body += " "
			}
			body += err.Error()
		}
		return errors.New(body)
	}
	return nil
}

func restoreDB(db *BadgerDB, r *zip.Reader) error {
	if db == nil {
		return ErrorDatabaseNil
	}
	var bi BackupInfo
	for _, file := range r.File {
		if file.Name == "backupInfo" {
			rdr, err := file.Open()
			if err != nil {
				return err
			}
			dec := gob.NewDecoder(rdr)
			if err := dec.Decode(&bi); err != nil {
				rdr.Close()
				return err
			}
			rdr.Close()
			break
		}
	}
	if len(bi.FilenameToId) < 1 {
		return nil
	}

	txn := db.db.NewTransaction(true)
	//defer txn.Discard()

	nv := db.Version()

	//defer txn.Commit()

	var errs []error
	for _, f := range r.File {
		if f.Name == "backupInfo" {
			continue
		}
		id, ok := bi.FilenameToId[f.Name]
		if !ok {
			errs = append(errs, errors.New("Missing ID for file:"+f.Name))
			continue
		}
		if bi.Version < Version2 && nv > Version1 && !strings.HasPrefix(id, SettingsPrefix) {
			npos := strings.LastIndex(id, NodeSeparator)
			if npos < 1 || npos >= len(id) {
				id = EntityPrefix + id
			} else {
				id = id[:npos+1] + EntityPrefix + id[npos+1:]
			}
		}
		rdr, err := f.Open()
		if err != nil {
			errs = append(errs, errors.New(err.Error()+":"+f.Name))
			continue
		}
		data := new(bytes.Buffer)
		_, err = io.Copy(data, rdr)
		rdr.Close()
		if err != nil {
			errs = append(errs, errors.New(err.Error()+":"+f.Name))
			continue
		}
		if err := txn.Set([]byte(id), data.Bytes()); err != nil {
			if err == badger.ErrTxnTooBig {
				txn.Commit()
				txn = db.db.NewTransaction(true)
				if err := txn.Set([]byte(id), data.Bytes()); err != nil {
					errs = append(errs, errors.New(err.Error()+":"+f.Name))
					continue
				}
			} else {
				errs = append(errs, errors.New(err.Error()+":"+f.Name))
				continue
			}
		}
	}
	txn.Commit()
	if len(errs) > 0 {
		var body string
		for _, err := range errs {
			if len(body) > 0 {
				body += " "
			}
			body += err.Error()
		}
		return errors.New(body)
	}
	return nil
}

func backupNodePrefixes(prefixes []string, db *BadgerNode, w *zip.Writer) error {
	if db == nil {
		return ErrorDatabaseNil
	}
	txn := db.db.NewTransaction(false)
	defer txn.Discard()

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = true
	opts.Prefix = []byte(db.prefix)

	it := txn.NewIterator(opts)
	defer it.Close()
	var errs []error

	var bi BackupInfo
	bi.Version = db.Version()
	bi.FilenameToId = make(map[string]string)

	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		key := string(item.Key())
		if len(prefixes) > 0 {
			good := false
			for _, prefix := range prefixes {
				if strings.HasPrefix(key, prefix) {
					good = true
					break
				}
			}
			if !good {
				continue
			}
		}
		filename := uuid.NewV4().String()
		f, err := w.Create(filename)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if err := item.Value(func(val []byte) error {
			b := make([]byte, len(val))
			copy(b, val)
			f.Write(b)
			return nil
		}); err != nil {
			errs = append(errs, errors.New("key:"+key+" "+err.Error()))
		}
		bi.FilenameToId[filename] = key
	}
	if f, err := w.Create("backupInfo"); err != nil {
		return err
	} else {
		enc := gob.NewEncoder(f)
		if err := enc.Encode(&bi); err != nil {
			return err
		}
	}
	return nil
}

func backupNode(db *BadgerNode, w *zip.Writer) error {
	if db == nil {
		return ErrorDatabaseNil
	}
	txn := db.db.NewTransaction(false)
	defer txn.Discard()

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = true
	opts.Prefix = []byte(db.prefix)

	it := txn.NewIterator(opts)
	defer it.Close()
	var errs []error

	var bi BackupInfo
	bi.Version = db.Version()
	bi.FilenameToId = make(map[string]string)

	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		key := string(item.Key())
		filename := uuid.NewV4().String()
		f, err := w.Create(filename)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if err := item.Value(func(val []byte) error {
			b := make([]byte, len(val))
			copy(b, val)
			f.Write(b)
			return nil
		}); err != nil {
			errs = append(errs, errors.New("key:"+key+" "+err.Error()))
		}
		bi.FilenameToId[filename] = key
	}
	if f, err := w.Create("backupInfo"); err != nil {
		return err
	} else {
		enc := gob.NewEncoder(f)
		if err := enc.Encode(&bi); err != nil {
			return err
		}
	}
	return nil
}

func restoreNodePrefixes(prefixes []string, db *BadgerNode, r *zip.Reader) error {
	if db == nil {
		return ErrorDatabaseNil
	}
	var bi BackupInfo
	for _, file := range r.File {
		if file.Name == "backupInfo" {
			rdr, err := file.Open()
			if err != nil {
				return err
			}
			dec := gob.NewDecoder(rdr)
			if err := dec.Decode(&bi); err != nil {
				rdr.Close()
				return err
			}
			rdr.Close()
			break
		}
	}
	if len(bi.FilenameToId) < 1 {
		return nil
	}

	txn := db.db.NewTransaction(true)
	//defer txn.Commit()

	var errs []error
	for _, f := range r.File {
		if f.Name == "backupInfo" {
			continue
		}
		id, ok := bi.FilenameToId[f.Name]
		if !ok {
			errs = append(errs, errors.New("Missing ID for file:"+f.Name))
			continue
		}
		if len(prefixes) > 0 {
			good := false
			for _, prefix := range prefixes {
				if strings.HasPrefix(id, prefix) {
					good = true
					break
				}
			}
			if !good {
				continue
			}
		}
		rdr, err := f.Open()
		if err != nil {
			errs = append(errs, errors.New(err.Error()+":"+f.Name))
			continue
		}
		data := new(bytes.Buffer)
		_, err = io.Copy(data, rdr)
		rdr.Close()
		if err != nil {
			errs = append(errs, errors.New(err.Error()+":"+f.Name))
			continue
		}
		if err := txn.Set([]byte(id), data.Bytes()); err != nil {
			if err == badger.ErrTxnTooBig {
				txn.Commit()
				txn = db.db.NewTransaction(true)
				if err := txn.Set([]byte(id), data.Bytes()); err != nil {
					errs = append(errs, errors.New(err.Error()+":"+f.Name))
					continue
				}
			} else {
				errs = append(errs, errors.New(err.Error()+":"+f.Name))
				continue
			}
		}
	}
	txn.Commit()
	if len(errs) > 0 {
		var body string
		for _, err := range errs {
			if len(body) > 0 {
				body += " "
			}
			body += err.Error()
		}
		return errors.New(body)
	}
	return nil
}

func restoreNode(db *BadgerNode, r *zip.Reader) error {
	if db == nil {
		return ErrorDatabaseNil
	}
	var bi BackupInfo
	for _, file := range r.File {
		if file.Name == "backupInfo" {
			rdr, err := file.Open()
			if err != nil {
				return err
			}
			dec := gob.NewDecoder(rdr)
			if err := dec.Decode(&bi); err != nil {
				rdr.Close()
				return err
			}
			rdr.Close()
			break
		}
	}
	if len(bi.FilenameToId) < 1 {
		return nil
	}

	txn := db.db.NewTransaction(true)
	//defer txn.Commit()

	var errs []error
	for _, f := range r.File {
		if f.Name == "backupInfo" {
			continue
		}
		id, ok := bi.FilenameToId[f.Name]
		if !ok {
			errs = append(errs, errors.New("Missing ID for file:"+f.Name))
			continue
		}
		rdr, err := f.Open()
		if err != nil {
			errs = append(errs, errors.New(err.Error()+":"+f.Name))
			continue
		}
		data := new(bytes.Buffer)
		_, err = io.Copy(data, rdr)
		rdr.Close()
		if err != nil {
			errs = append(errs, errors.New(err.Error()+":"+f.Name))
			continue
		}
		if err := txn.Set([]byte(id), data.Bytes()); err != nil {
			if err == badger.ErrTxnTooBig {
				txn.Commit()
				txn = db.db.NewTransaction(true)
				if err := txn.Set([]byte(id), data.Bytes()); err != nil {
					errs = append(errs, errors.New(err.Error()+":"+f.Name))
					continue
				}
			} else {
				errs = append(errs, errors.New(err.Error()+":"+f.Name))
				continue
			}
		}
	}
	txn.Commit()
	if len(errs) > 0 {
		var body string
		for _, err := range errs {
			if len(body) > 0 {
				body += " "
			}
			body += err.Error()
		}
		return errors.New(body)
	}
	return nil
}

func backupExpiryPrefixes(prefixes []string, db *BadgerExpiry, w *zip.Writer) error {
	if db == nil {
		return ErrorDatabaseNil
	}
	txn := db.db.NewTransaction(false)
	defer txn.Discard()

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = true
	opts.Prefix = []byte(db.prefix)

	it := txn.NewIterator(opts)
	defer it.Close()
	var errs []error

	var bi BackupInfo
	bi.FilenameToId = make(map[string]string)

	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		key := string(item.Key())
		if len(prefixes) > 0 {
			good := false
			for _, prefix := range prefixes {
				if strings.HasPrefix(key, prefix) {
					good = true
					break
				}
			}
			if !good {
				continue
			}
		}
		filename := uuid.NewV4().String()
		f, err := w.Create(filename)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if err := item.Value(func(val []byte) error {
			b := make([]byte, len(val))
			copy(b, val)
			f.Write(b)
			return nil
		}); err != nil {
			errs = append(errs, errors.New("key:"+key+" "+err.Error()))
		}
		bi.FilenameToId[filename] = key
	}
	if f, err := w.Create("backupInfo"); err != nil {
		return err
	} else {
		enc := gob.NewEncoder(f)
		if err := enc.Encode(&bi); err != nil {
			return err
		}
	}
	return nil
}

func backupExpiry(db *BadgerExpiry, w *zip.Writer) error {
	if db == nil {
		return ErrorDatabaseNil
	}
	txn := db.db.NewTransaction(false)
	defer txn.Discard()

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = true
	opts.Prefix = []byte(db.prefix)

	it := txn.NewIterator(opts)
	defer it.Close()
	var errs []error

	var bi BackupInfo
	bi.FilenameToId = make(map[string]string)

	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		key := string(item.Key())
		filename := uuid.NewV4().String()
		f, err := w.Create(filename)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if err := item.Value(func(val []byte) error {
			b := make([]byte, len(val))
			copy(b, val)
			f.Write(b)
			return nil
		}); err != nil {
			errs = append(errs, errors.New("key:"+key+" "+err.Error()))
		}
		bi.FilenameToId[filename] = key
	}
	if f, err := w.Create("backupInfo"); err != nil {
		return err
	} else {
		enc := gob.NewEncoder(f)
		if err := enc.Encode(&bi); err != nil {
			return err
		}
	}
	return nil
}

func restoreExpiryPrefixes(prefixes []string, db *BadgerExpiry, r *zip.Reader) error {
	if db == nil {
		return ErrorDatabaseNil
	}
	var bi BackupInfo
	for _, file := range r.File {
		if file.Name == "backupInfo" {
			rdr, err := file.Open()
			if err != nil {
				return err
			}
			dec := gob.NewDecoder(rdr)
			if err := dec.Decode(&bi); err != nil {
				rdr.Close()
				return err
			}
			rdr.Close()
			break
		}
	}
	if len(bi.FilenameToId) < 1 {
		return nil
	}

	txn := db.db.NewTransaction(true)
	//defer txn.Commit()

	var errs []error
	for _, f := range r.File {
		if f.Name == "backupInfo" {
			continue
		}
		id, ok := bi.FilenameToId[f.Name]
		if !ok {
			errs = append(errs, errors.New("Missing ID for file:"+f.Name))
			continue
		}
		if len(prefixes) > 0 {
			good := false
			for _, prefix := range prefixes {
				if strings.HasPrefix(id, prefix) {
					good = true
					break
				}
			}
			if !good {
				continue
			}
		}
		rdr, err := f.Open()
		if err != nil {
			errs = append(errs, errors.New(err.Error()+":"+f.Name))
			continue
		}
		data := new(bytes.Buffer)
		_, err = io.Copy(data, rdr)
		rdr.Close()
		if err != nil {
			errs = append(errs, errors.New(err.Error()+":"+f.Name))
			continue
		}
		if err := txn.Set([]byte(id), data.Bytes()); err != nil {
			if err == badger.ErrTxnTooBig {
				txn.Commit()
				txn = db.db.NewTransaction(true)
				if err := txn.Set([]byte(id), data.Bytes()); err != nil {
					errs = append(errs, errors.New(err.Error()+":"+f.Name))
					continue
				}
			} else {
				errs = append(errs, errors.New(err.Error()+":"+f.Name))
				continue
			}
		}
	}
	txn.Commit()
	if len(errs) > 0 {
		var body string
		for _, err := range errs {
			if len(body) > 0 {
				body += " "
			}
			body += err.Error()
		}
		return errors.New(body)
	}
	return nil
}

func restoreExpiry(db *BadgerExpiry, r *zip.Reader) error {
	if db == nil {
		return ErrorDatabaseNil
	}
	var bi BackupInfo
	for _, file := range r.File {
		if file.Name == "backupInfo" {
			rdr, err := file.Open()
			if err != nil {
				return err
			}
			dec := gob.NewDecoder(rdr)
			if err := dec.Decode(&bi); err != nil {
				rdr.Close()
				return err
			}
			rdr.Close()
			break
		}
	}
	if len(bi.FilenameToId) < 1 {
		return nil
	}

	txn := db.db.NewTransaction(true)
	//defer txn.Commit()

	var errs []error
	for _, f := range r.File {
		if f.Name == "backupInfo" {
			continue
		}
		id, ok := bi.FilenameToId[f.Name]
		if !ok {
			errs = append(errs, errors.New("Missing ID for file:"+f.Name))
			continue
		}
		rdr, err := f.Open()
		if err != nil {
			errs = append(errs, errors.New(err.Error()+":"+f.Name))
			continue
		}
		data := new(bytes.Buffer)
		_, err = io.Copy(data, rdr)
		rdr.Close()
		if err != nil {
			errs = append(errs, errors.New(err.Error()+":"+f.Name))
			continue
		}
		if err := txn.Set([]byte(id), data.Bytes()); err != nil {
			if err == badger.ErrTxnTooBig {
				txn.Commit()
				txn = db.db.NewTransaction(true)
				if err := txn.Set([]byte(id), data.Bytes()); err != nil {
					errs = append(errs, errors.New(err.Error()+":"+f.Name))
					continue
				}
			} else {
				errs = append(errs, errors.New(err.Error()+":"+f.Name))
				continue
			}
		}
	}
	txn.Commit()
	if len(errs) > 0 {
		var body string
		for _, err := range errs {
			if len(body) > 0 {
				body += " "
			}
			body += err.Error()
		}
		return errors.New(body)
	}
	return nil
}

func (db *BadgerDB) Backup(w *zip.Writer) error {
	if db.readonly != nil {
		if db.readonly.IsSet() {
			return nil
		}
	}
	return backupDB(db, w)
}

func (db *BadgerDB) Restore(r *zip.Reader) error {
	if db.readonly != nil {
		if db.readonly.IsSet() {
			return nil
		}
	}
	return restoreDB(db, r)
}

func (db *BadgerNode) Backup(w *zip.Writer) error {
	if db.readonly != nil {
		if db.readonly.IsSet() {
			return nil
		}
	}
	return backupNode(db, w)
}

func (db *BadgerNode) Restore(r *zip.Reader) error {
	if db.readonly != nil {
		if db.readonly.IsSet() {
			return nil
		}
	}
	return restoreNode(db, r)
}

func (db *BadgerExpiry) Backup(w *zip.Writer) error {
	if db.readonly != nil {
		if db.readonly.IsSet() {
			return nil
		}
	}
	return backupExpiry(db, w)
}

func (db *BadgerExpiry) Restore(r *zip.Reader) error {
	if db.readonly != nil {
		if db.readonly.IsSet() {
			return nil
		}
	}
	return restoreExpiry(db, r)
}
