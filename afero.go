package database

import (
	"errors"
	"os"
	"time"

	//"github.com/dgraph-io/badger"
	"github.com/spf13/afero"
)

type FileDatabase struct {
	//db   *badger.DB
	db      Database
	name    string
	version *DatabaseVersioner
}

/*
Chmod(name string, mode os.FileMode) : error
Chown(name string, uid, gid int) : error
Chtimes(name string, atime time.Time, mtime time.Time) : error
Create(name string) : File, error
Mkdir(name string, perm os.FileMode) : error
MkdirAll(path string, perm os.FileMode) : error
Name() : string
Open(name string) : File, error
OpenFile(name string, flag int, perm os.FileMode) : File, error
Remove(name string) : error
RemoveAll(path string) : error
Rename(oldname, newname string) : error
Stat(name string) : os.FileInfo, error
*/

var ErrorOldVersion = errors.New("Version too old.  Please upgrade")

func (fd *FileDatabase) checkVersion() error {
	if fd.db == nil {
		return ErrorDatabaseNil
	}
	if fd.db.Version() < Version2 {
		return ErrorOldVersion
	} else {
		return nil
	}
}

func (fd *FileDatabase) Open(path string) (afero.File, error) {
	return nil, ErrorNotImplemented
}

func (fd *FileDatabase) Chmod(name string, mode os.FileMode) error {
	return ErrorNotImplemented
}

func (fd *FileDatabase) Chown(name string, uid, gid int) error {
	return ErrorNotImplemented
}
func (fd *FileDatabase) Chtimes(name string, atime time.Time, mtime time.Time) error {
	return ErrorNotImplemented
}
func (fd *FileDatabase) Create(name string) (afero.File, error) {
	return nil, ErrorNotImplemented
}
func (fd *FileDatabase) Mkdir(path string, perm os.FileMode) error {
	return ErrorNotImplemented
}
func (fd *FileDatabase) MkdirAll(name string, uid, gid int) error {
	return ErrorNotImplemented
}
func (fd *FileDatabase) Name() string {
	return fd.name
}
func (fd *FileDatabase) OpenFile(flag int, perm os.FileMode) (afero.File, error) {
	return nil, ErrorNotImplemented
}
func (fd *FileDatabase) Remove() error {
	return ErrorNotImplemented
}
func (fd *FileDatabase) RemoveAll(path string) error {
	return ErrorNotImplemented
}
func (fd *FileDatabase) Rename(oldname, newname string) error {
	return ErrorNotImplemented
}
func (fd *FileDatabase) Stat() (os.FileInfo, error) {
	return nil, ErrorNotImplemented
}
