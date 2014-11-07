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

// KVStore handle
type KVStore struct {
	f  *File
	db *C.fdb_kvs_handle
}

// Close the KVStore and release related resources.
func (k *KVStore) Close() error {
	errNo := C.fdb_kvs_close(k.db)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// Info returns the information about a given kvstore
func (k *KVStore) Info() (*KVStoreInfo, error) {
	rv := KVStoreInfo{}
	errNo := C.fdb_get_kvs_info(k.db, &rv.info)
	if errNo != RESULT_SUCCESS {
		return nil, Error(errNo)
	}
	return &rv, nil
}

// Get retrieves the metadata and doc body for a given key
func (k *KVStore) Get(doc *Doc) error {
	errNo := C.fdb_get(k.db, doc.doc)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// GetMetaOnly retrieves the metadata for a given key
func (k *KVStore) GetMetaOnly(doc *Doc) error {
	errNo := C.fdb_get_metaonly(k.db, doc.doc)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// GetBySeq retrieves the metadata and doc body for a given sequence number
func (k *KVStore) GetBySeq(doc *Doc) error {
	errNo := C.fdb_get_byseq(k.db, doc.doc)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// GetMetaOnlyBySeq retrieves the metadata for a given sequence number
func (k *KVStore) GetMetaOnlyBySeq(doc *Doc) error {
	errNo := C.fdb_get_metaonly_byseq(k.db, doc.doc)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// GetByOffset retrieves a doc's metadata and body with a given doc offset in the database file
func (k *KVStore) GetByOffset(doc *Doc) error {
	errNo := C.fdb_get_byoffset(k.db, doc.doc)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// Set update the metadata and doc body for a given key
func (k *KVStore) Set(doc *Doc) error {
	errNo := C.fdb_set(k.db, doc.doc)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// Delete deletes a key, its metadata and value
func (k *KVStore) Delete(doc *Doc) error {
	errNo := C.fdb_del(k.db, doc.doc)
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
