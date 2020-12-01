package database

import (
	"archive/zip"
	"bytes"
	"encoding/gob"
	"io"

	"github.com/dgraph-io/badger"

	//"bytes"
	//"crypto/sha256"
	//"encoding/gob"
	"errors"
	//"io"
	//"strings"
	"time"

	"github.com/twinj/uuid"
)

type DatabaseBackups interface {
	Backup(*zip.Writer) error
	Restore(*zip.Reader) error
}

type backupInfo struct {
	//Nodes        []string
	FilenameToId map[string]string
}

type backupNodeInfo struct {
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

	var bi backupInfo
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

func restoreDB(db *BadgerDB, r *zip.Reader) error {
	if db == nil {
		return ErrorDatabaseNil
	}
	var bi backupInfo
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
	defer txn.Commit()

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
			errs = append(errs, errors.New(err.Error()+":"+f.Name))
			continue
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
		return errors.New(body)
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

	var bi backupInfo
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

func restoreNode(db *BadgerNode, r *zip.Reader) error {
	if db == nil {
		return ErrorDatabaseNil
	}
	var bi backupInfo
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
	defer txn.Commit()

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
			errs = append(errs, errors.New(err.Error()+":"+f.Name))
			continue
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
		return errors.New(body)
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

	var bi backupInfo
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

func restoreExpiry(db *BadgerExpiry, r *zip.Reader) error {
	if db == nil {
		return ErrorDatabaseNil
	}
	var bi backupInfo
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
	defer txn.Commit()

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
			errs = append(errs, errors.New(err.Error()+":"+f.Name))
			continue
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
		return errors.New(body)
	}
	return nil
}

func (db *BadgerDB) Backup(w *zip.Writer) error {
	return backupDB(db, w)
}

func (db *BadgerDB) Restore(r *zip.Reader) error {
	return restoreDB(db, r)
}

func (db *BadgerNode) Backup(w *zip.Writer) error {
	return backupNode(db, w)
}

func (db *BadgerNode) Restore(r *zip.Reader) error {
	return restoreNode(db, r)
}

func (db *BadgerExpiry) Backup(w *zip.Writer) error {
	return backupExpiry(db, w)
}

func (db *BadgerExpiry) Restore(r *zip.Reader) error {
	return restoreExpiry(db, r)
}

/*
func backupDB(db Database, w *zip.Writer) error {
	if db == nil {
		return ErrorDatabaseNil
	}
	var bi backupInfo
	var errs []error
	bi.FilenameToId = make(map[string]string)
	//bi.Checksums = make(map[string][]byte)
	if nodes, err := db.GetNodes(); err == nil {
		if len(nodes) > 0 {
			for _, name := range nodes {
				if node, err := db.NewNode(name); err != nil {
					errs = append(errs, err)
				} else {
					if err := node.Backup(w); err != nil {
						errs = append(errs, err)
					}
				}
			}
			bi.Nodes = nodes
		}
	}
	if items, err := db.GetAll(); err == nil {
		if len(items) > 0 {
			for id, data := range items {
				filename := uuid.NewV4().String()
				f, err := w.Create(filename)
				if err != nil {
					errs = append(errs, err)
					continue
				}
				if _, err := f.Write(data.Data()); err != nil {
					errs = append(errs, err)
					continue
				}
				bi.FilenameToId[filename] = id
				//h := sha256.New()
				//bi.Checksums[filename] = h.Sum(data.Data())
			}
		}
	}
	fi, err := w.Create("BackupInfo")
	if err != nil {
		return err
	}
	enc := gob.NewEncoder(fi)
	if err := enc.Encode(&bi); err != nil {
		return err
	}
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

func backupNode(dbn *BadgerNode, w *zip.Writer) error {
	if dbn == nil {
		return ErrorDatabaseNil
	}
	h := sha256.New()
	dir := string(h.Sum([]byte(dbn.id)))
	if _, err := w.Create(dir + `/`); err != nil {
		return err
	}

	var bi backupNodeInfo
	var errs []error
	bi.NodeName = dbn.id
	bi.Prefix = dbn.prefix
	bi.FilenameToId = make(map[string]string)
	//bi.Checksums = make(map[string][]byte)
	if nodes, err := dbn.GetNodes(); err == nil {
		if len(nodes) > 0 {
			for _, name := range nodes {
				if node, err := dbn.NewNode(name); err != nil {
					errs = append(errs, err)
				} else {
					if err := node.Backup(w); err != nil {
						errs = append(errs, err)
					}
				}
			}
			bi.Nodes = nodes
		}
	}
	if items, err := dbn.GetAll(); err == nil {
		if len(items) > 0 {
			for id, data := range items {
				filename := uuid.NewV4().String()
				f, err := w.Create(dir + `/` + filename)
				if err != nil {
					errs = append(errs, err)
					continue
				}
				if _, err := f.Write(data.Data()); err != nil {
					errs = append(errs, err)
					continue
				}
				bi.FilenameToId[filename] = id
				//h := sha256.New()
				//bi.Checksums[filename] = h.Sum(data.Data())
			}
		}
	}
	fi, err := w.Create(dir + `/` + "backupNodeInfo")
	if err != nil {
		return err
	}
	enc := gob.NewEncoder(fi)
	if err := enc.Encode(&bi); err != nil {
		return err
	}
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

func backupExpiry(dbn *BadgerExpiry, w *zip.Writer) error {
	if dbn == nil {
		return ErrorDatabaseNil
	}
	h := sha256.New()
	dir := string(h.Sum([]byte(dbn.id)))
	if _, err := w.Create(dir + `/`); err != nil {
		return err
	}

	var bi backupExpiryInfo
	var errs []error
	bi.NodeName = dbn.id
	bi.Prefix = dbn.prefix
	bi.Duration = dbn.timeout
	bi.FilenameToId = make(map[string]string)
	//bi.Checksums = make(map[string][]byte)
	if nodes, err := dbn.GetNodes(); err == nil {
		if len(nodes) > 0 {
			for _, name := range nodes {
				if node, err := dbn.NewNode(name); err != nil {
					errs = append(errs, err)
				} else {
					if err := node.Backup(w); err != nil {
						errs = append(errs, err)
					}
				}
			}
			bi.Nodes = nodes
		}
	}
	if items, err := dbn.GetAll(); err == nil {
		if len(items) > 0 {
			for id, data := range items {
				filename := uuid.NewV4().String()
				f, err := w.Create(dir + `/` + filename)
				if err != nil {
					errs = append(errs, err)
					continue
				}
				if _, err := f.Write(data.Data()); err != nil {
					errs = append(errs, err)
					continue
				}
				bi.FilenameToId[filename] = id
				//h := sha256.New()
				//bi.Checksums[filename] = h.Sum(data.Data())
			}
		}
	}
	fi, err := w.Create(dir + `/` + "backupExpiryInfo")
	if err != nil {
		return err
	}
	enc := gob.NewEncoder(fi)
	if err := enc.Encode(&bi); err != nil {
		return err
	}
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
	if len(r.File) < 1 {
		return errors.New("Backup Empty")
	}
	var bi backupInfo
	exists := false
	for _, f := range r.File {
		if f.Name == "BackupInfo" {
			rdr, err := f.Open()
			if err != nil {
				break
			}
			dec := gob.NewDecoder(rdr)
			if err := dec.Decode(&bi); err != nil {
				rdr.Close()
				break
			}
			exists = true
			rdr.Close()
			break
		}
	}
	var nodes []Database
	if !exists {
		for _, f := range r.File {
			if strings.HasSuffix(f.Name, "backupNodeInfo") {
				var bi backupNodeInfo
				rdr, err := f.Open()
				if err != nil {
					continue
				}
				dec := gob.NewDecoder(rdr)
				if err := dec.Decode(&bi); err != nil {
					rdr.Close()
					continue
				}
				rdr.Close()
				dbn := &BadgerNode{id: bi.NodeName, prefix: bi.Prefix, db: db.db}
				nodes = append(nodes, dbn)
			} else if strings.HasSuffix(f.Name, "backupExpiryInfo") {
				var bi backupExpiryInfo
				rdr, err := f.Open()
				if err != nil {
					continue
				}
				dec := gob.NewDecoder(rdr)
				if err := dec.Decode(&bi); err != nil {
					rdr.Close()
					continue
				}
				rdr.Close()
				dbn := &BadgerExpiry{id: bi.NodeName, prefix: bi.Prefix, db: db.db, timeout: bi.Duration}
				nodes = append(nodes, dbn)
			}
		}
	}
	if len(bi.Nodes) > 0 {
		for _, name := range bi.Nodes {
			h := sha256.New()
			dir := string(h.Sum([]byte(name)))
			for _, f := range r.File {
				if f.Name == dir+`/`+"backupNodeInfo" {
					node, err := db.NewNode(name)
					if err != nil {
						return err
					}
					nodes = append(nodes, node)
					break
				} else if f.Name == dir+`/`+"backupExpiryInfo" {
					var be backupExpiryInfo
					file, err := f.Open()
					if err != nil {
						return err
					}
					dec := gob.NewDecoder(file)
					if err := dec.Decode(&be); err != nil {
						file.Close()
						return err
					}
					file.Close()
					node, err := db.NewExpiryNode(name, be.Duration)
					if err != nil {
						return err
					}
					nodes = append(nodes, node)
					break
				}
			}
		}
	}
	if len(nodes) > 0 {
		for _, node := range nodes {
			if err := node.Restore(r); err != nil {
				return err
			}
		}
	}
	if bi.FilenameToId != nil {
		if len(bi.FilenameToId) > 0 {
			for name, id := range bi.FilenameToId {
				for _, f := range r.File {
					if f.Name == name {
						rdr, err := f.Open()
						if err != nil {
							return err
						}
						buf := bytes.NewBuffer(nil)
						_, err = io.Copy(buf, rdr)
						rdr.Close()
						if err != nil {
							return err
						}
						if err := db.SetValue(id, buf.Bytes()); err != nil {
							return err
						}
						break
					}
				}
			}
		}
	}
	return nil
}

func restoreNode(dbn *BadgerNode, r *zip.Reader) error {
	if dbn == nil {
		return ErrorDatabaseNil
	}
	h := sha256.New()
	dir := string(h.Sum([]byte(dbn.id)))
	var bi backupNodeInfo
	for _, file := range r.File {
		if file.Name == dir+`/`+"backupNodeInfo" {
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
	if len(bi.Prefix) < 1 {
		return ErrorMissingID
	}
	dbn.prefix = bi.Prefix
	if len(bi.Nodes) > 0 {
		var nodes []Database
		for _, name := range bi.Nodes {
			h := sha256.New()
			dir := string(h.Sum([]byte(name)))
			for _, f := range r.File {
				if f.Name == dir+`/`+"backupNodeInfo" {
					node, err := dbn.NewNode(name)
					if err != nil {
						return err
					}
					nodes = append(nodes, node)
					break
				} else if f.Name == dir+`/`+"backupExpiryInfo" {
					var be backupExpiryInfo
					file, err := f.Open()
					if err != nil {
						return err
					}
					dec := gob.NewDecoder(file)
					if err := dec.Decode(&be); err != nil {
						file.Close()
						return err
					}
					file.Close()
					node, err := dbn.NewExpiryNode(name, be.Duration)
					if err != nil {
						return err
					}
					nodes = append(nodes, node)
					break
				}
			}
		}
		if len(nodes) > 0 {
			for _, node := range nodes {
				if err := node.Restore(r); err != nil {
					return err
				}
			}
		}
	}
	if bi.FilenameToId != nil {
		if len(bi.FilenameToId) > 0 {
			for name, id := range bi.FilenameToId {
				for _, f := range r.File {
					if f.Name == name {
						rdr, err := f.Open()
						if err != nil {
							return err
						}
						buf := bytes.NewBuffer(nil)
						_, err = io.Copy(buf, rdr)
						rdr.Close()
						if err != nil {
							return err
						}
						if err := dbn.SetValue(id, buf.Bytes()); err != nil {
							return err
						}
						break
					}
				}
			}
		}
	}
	return nil
}

func restoreExpiry(dbn *BadgerExpiry, r *zip.Reader) error {
	if dbn == nil {
		return ErrorDatabaseNil
	}
	h := sha256.New()
	dir := string(h.Sum([]byte(dbn.id)))
	var bi backupExpiryInfo
	for _, file := range r.File {
		if file.Name == dir+`/`+"backupExpiryInfo" {
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
	if len(bi.Prefix) < 1 {
		return ErrorMissingID
	}
	dbn.prefix = bi.Prefix
	if len(bi.Nodes) > 0 {
		var nodes []Database
		for _, name := range bi.Nodes {
			h := sha256.New()
			dir := string(h.Sum([]byte(name)))
			for _, f := range r.File {
				if f.Name == dir+`/`+"backupNodeInfo" {
					node, err := dbn.NewNode(name)
					if err != nil {
						return err
					}
					nodes = append(nodes, node)
					break
				} else if f.Name == dir+`/`+"backupExpiryInfo" {
					var be backupExpiryInfo
					file, err := f.Open()
					if err != nil {
						return err
					}
					dec := gob.NewDecoder(file)
					if err := dec.Decode(&be); err != nil {
						file.Close()
						return err
					}
					file.Close()
					node, err := dbn.NewExpiryNode(name, be.Duration)
					if err != nil {
						return err
					}
					nodes = append(nodes, node)
					break
				}
			}
		}
		if len(nodes) > 0 {
			for _, node := range nodes {
				if err := node.Restore(r); err != nil {
					return err
				}
			}
		}
	}
	if bi.FilenameToId != nil {
		if len(bi.FilenameToId) > 0 {
			for name, id := range bi.FilenameToId {
				for _, f := range r.File {
					if f.Name == name {
						rdr, err := f.Open()
						if err != nil {
							return err
						}
						buf := bytes.NewBuffer(nil)
						_, err = io.Copy(buf, rdr)
						rdr.Close()
						if err != nil {
							return err
						}
						if err := dbn.SetValue(id, buf.Bytes()); err != nil {
							return err
						}
						break
					}
				}
			}
		}
	}
	return nil
}
*/
