package database

import (
	"archive/zip"
	"time"
)

type DatabaseS3 struct {
}

func NewS3Database() Database {
	var ds3 DatabaseS3

	return &ds3
}

func (ds *DatabaseS3) Get(id string, item interface{}) error {

	return ErrorNotImplemented
}

func (ds *DatabaseS3) GetValue(id string) ([]byte, error) {

	return []byte{}, ErrorNotImplemented
}

func (ds *DatabaseS3) GetAll() (List, error) {

	return nil, ErrorNotImplemented
}

func (ds *DatabaseS3) GetIDs() ([]string, error) {
	return []string{}, ErrorDatabaseNil
}

func (ds *DatabaseS3) Length() int {
	return 0
}

func (ds *DatabaseS3) Size() uint64 {
	return 0
}

func (ds *DatabaseS3) Range(page, size int) (List, error) {
	return nil, ErrorNotImplemented
}

func (ds *DatabaseS3) Pages(size int) int {
	return 0
}

func (ds *DatabaseS3) Prefix() string {
	return ""
}

func (ds *DatabaseS3) ForEach(f ForEachFunc) error {
	return ErrorNotImplemented
}

func (ds *DatabaseS3) Exists(id string) bool {
	return false
}

func (ds *DatabaseS3) Set(id string, item interface{}) error {
	return ErrorNotImplemented
}

func (ds *DatabaseS3) SetValue(id string, value []byte) error {
	return ErrorNotImplemented
}

func (ds *DatabaseS3) Delete(id string) error {
	return ErrorNotImplemented
}

func (ds *DatabaseS3) Clear() error {
	return ErrorNotImplemented
}

func (ds *DatabaseS3) GetAndDelete(id string, item interface{}) error {
	return ErrorNotImplemented
}

func (ds *DatabaseS3) Merge(id string, mf MergeFunc) error {
	return ErrorNotImplemented
}

func (ds *DatabaseS3) NodeCount() int {
	return 0
}

func (ds *DatabaseS3) GetNodes() ([]string, error) {
	return []string{}, ErrorNotImplemented
}

func (ds *DatabaseS3) NewNode(name string) (Database, error) {
	return nil, ErrorNotImplemented
}

func (ds *DatabaseS3) DropNode(name string) error {
	return ErrorNotImplemented
}

func (ds *DatabaseS3) Parent() (Database, error) {
	return nil, ErrorNotImplemented
}

func (ds *DatabaseS3) Backup(writer *zip.Writer) error {
	return ErrorNotImplemented
}

func (ds *DatabaseS3) Restore(reader *zip.Reader) error {
	return ErrorNotImplemented
}

func (ds *DatabaseS3) BackupPrefixes(prefs []string, writer *zip.Writer) error {
	return ErrorNotImplemented
}

func (ds *DatabaseS3) RestorePrefixes(prefs []string, reader *zip.Reader) error {
	return ErrorNotImplemented
}

func (ds *DatabaseS3) Close() error {
	return ErrorNotImplemented
}

func (ds *DatabaseS3) SetReadOnly() error {
	return ErrorNotImplemented
}

func (ds *DatabaseS3) SetReadWrite() error {
	return ErrorNotImplemented
}

func (ds *DatabaseS3) NewExpiryNode(name string, dur time.Duration, ver *DatabaseVersioner) (Database, error) {
	return nil, ErrorNotImplemented
}

func (ds *DatabaseS3) Version() Version {
	return Version1
}
