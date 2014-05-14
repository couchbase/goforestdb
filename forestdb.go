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
//#include <string.h>
//#include <forestdb/forestdb.h>
import "C"

import (
	"unsafe"
)

// Database handle
type Database struct {
	db *C.fdb_handle
}

// Open opens the database with a given file name
func Open(filename string, config *Config) (*Database, error) {

	if config == nil {
		config = DefaultConfig()
	}

	dbname := C.CString(filename)
	defer C.free(unsafe.Pointer(dbname))

	rv := Database{}
	errNo := C.fdb_open(&rv.db, dbname, config.config)
	if errNo != RESULT_SUCCESS {
		return nil, Error(errNo)
	}
	return &rv, nil
}

// Get retrieves the metadata and doc body for a given key
func (d *Database) Get(doc *Doc) error {
	errNo := C.fdb_get(d.db, doc.doc)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// GetMetaOnly retrieves the metadata for a given key
func (d *Database) GetMetaOnly(doc *Doc) error {
	errNo := C.fdb_get_metaonly(d.db, doc.doc)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// GetBySeq retrieves the metadata and doc body for a given sequence number
func (d *Database) GetBySeq(doc *Doc) error {
	errNo := C.fdb_get_byseq(d.db, doc.doc)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// GetMetaOnlyBySeq retrieves the metadata for a given sequence number
func (d *Database) GetMetaOnlyBySeq(doc *Doc) error {
	errNo := C.fdb_get_metaonly_byseq(d.db, doc.doc)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// GetByOffset retrieves a doc's metadata and body with a given doc offset in the database file
func (d *Database) GetByOffset(doc *Doc) error {
	errNo := C.fdb_get_byoffset(d.db, doc.doc)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// Set update the metadata and doc body for a given key
func (d *Database) Set(doc *Doc) error {
	errNo := C.fdb_set(d.db, doc.doc)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// Delete deletes a key, its metadata and value
func (d *Database) Delete(doc *Doc) error {
	errNo := C.fdb_del(d.db, doc.doc)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// Compact the current database file and create a new compacted file
func (d *Database) Compact(newfilename string) error {

	f := C.CString(newfilename)
	defer C.free(unsafe.Pointer(f))

	errNo := C.fdb_compact(d.db, f)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// Close the database file
func (d *Database) Close() error {
	errNo := C.fdb_close(d.db)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// Shutdown destroys all the resources (e.g., buffer cache, in-memory WAL indexes, daemon compaction thread, etc.) and then shutdown the ForestDB engine
func Shutdown() error {
	errNo := C.fdb_shutdown()
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}
