package forestdb

//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

//#cgo LDFLAGS: -lforestdb
//#include <stdlib.h>
//#include <libforestdb/forestdb.h>
import "C"

import (
	"unsafe"
)

// Database handle
type File struct {
	dbfile *C.fdb_file_handle
}

// Open opens the database with a given file name
func Open(filename string, config *Config) (*File, error) {

	if config == nil {
		config = DefaultConfig()
	}

	dbname := C.CString(filename)
	defer C.free(unsafe.Pointer(dbname))

	rv := File{}
	errNo := C.fdb_open(&rv.dbfile, dbname, config.config)
	if errNo != RESULT_SUCCESS {
		return nil, Error(errNo)
	}
	return &rv, nil
}

// FIXME disabling this for now, as I can't get it
// to work as I expect.  Set the comparator in the
// KVStoreConfig directly.

// func OpenCustomCmp(filename string, config *Config, cmp map[string]unsafe.Pointer) (*File, error) {

// 	// prepare the custom comparator
// 	num_functions := C.size_t(len(cmp))
// 	kvs_names := make([]*C.char, len(cmp))
// 	funcs := make([]C.fdb_custom_cmp_variable, len(cmp))
// 	i := 0
// 	for cmpk, cmpv := range cmp {
// 		kvs_names[i] = C.CString(cmpk)
// 		funcs[i] = C.fdb_custom_cmp_variable(cmpv)
// 		i++
// 	}

// 	if config == nil {
// 		config = DefaultConfig()
// 	}

// 	dbname := C.CString(filename)
// 	defer C.free(unsafe.Pointer(dbname))

// 	rv := File{}
// 	errNo := C.fdb_open_custom_cmp(&rv.dbfile, dbname, config.config, num_functions, &kvs_names[0], &funcs[0])
// 	if errNo != RESULT_SUCCESS {
// 		return nil, Error(errNo)
// 	}
// 	return &rv, nil
// }

// Options to be passed to Commit()
type CommitOpt uint8

const (
	// Perform commit without any options.
	COMMIT_NORMAL CommitOpt = 0x00
	// Manually flush WAL entries even though it doesn't reach the configured threshol
	COMMIT_MANUAL_WAL_FLUSH CommitOpt = 0x01
)

// Commit all pending changes into disk.
func (f *File) Commit(opt CommitOpt) error {
	errNo := C.fdb_commit(f.dbfile, C.fdb_commit_opt_t(opt))
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// Compact the current database file and create a new compacted file
func (f *File) Compact(newfilename string) error {

	fn := C.CString(newfilename)
	defer C.free(unsafe.Pointer(f))

	errNo := C.fdb_compact(f.dbfile, fn)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// EstimateSpaceUsed returns the overall disk space actively used by the current database file
func (f *File) EstimateSpaceUsed() int {
	return int(C.fdb_estimate_space_used(f.dbfile))
}

// DbInfo returns the information about a given database handle
func (f *File) Info() (*FileInfo, error) {
	rv := FileInfo{}
	errNo := C.fdb_get_file_info(f.dbfile, &rv.info)
	if errNo != RESULT_SUCCESS {
		return nil, Error(errNo)
	}
	return &rv, nil
}

// FIXME implement fdb_switch_compaction_mode

// Close the database file
func (f *File) Close() error {
	errNo := C.fdb_close(f.dbfile)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// OpenKVStore opens the named KVStore within the File
// using the provided KVStoreConfig.  If config is
// nil the DefaultKVStoreConfig() will be used.
func (f *File) OpenKVStore(name string, config *KVStoreConfig) (*KVStore, error) {
	if config == nil {
		config = DefaultKVStoreConfig()
	}

	rv := KVStore{
		f: f,
	}
	kvsname := C.CString(name)
	defer C.free(unsafe.Pointer(kvsname))
	errNo := C.fdb_kvs_open(f.dbfile, &rv.db, kvsname, config.config)
	if errNo != RESULT_SUCCESS {
		return nil, Error(errNo)
	}
	return &rv, nil
}

// OpenKVStore opens the default KVStore within the File
// using the provided KVStoreConfig.  If config is
// nil the DefaultKVStoreConfig() will be used.
func (f *File) OpenKVStoreDefault(config *KVStoreConfig) (*KVStore, error) {
	return f.OpenKVStore("default", config)
}
