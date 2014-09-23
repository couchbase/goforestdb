package forestdb

//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

//#include <libforestdb/forestdb.h>
import "C"

import (
	"unsafe"
)

// ForestDB iterator options
type IteratorOpt uint8

const (
	// Return both key and value through iterator
	ITR_NONE IteratorOpt = 0x00
	// Return key and its metadata only through iterator
	ITR_META_ONLY IteratorOpt = 0x01
	// Return only non-deleted items through iterator
	ITR_NO_DELETES IteratorOpt = 0x02
)

// Iterator handle
type Iterator struct {
	iter *C.fdb_iterator
}

// Next gets the next item (key, metadata, doc body) from the iterator
func (i *Iterator) Next() (*Doc, error) {
	rv := Doc{}
	errNo := C.fdb_iterator_next(i.iter, &rv.doc)
	if errNo != RESULT_SUCCESS {
		return nil, Error(errNo)
	}
	return &rv, nil
}

// NextMetaOnly gets the next item (key, metadata, offset to doc body) from the iterator
func (i *Iterator) NextMetaOnly() (*Doc, error) {
	rv := Doc{}
	errNo := C.fdb_iterator_next_metaonly(i.iter, &rv.doc)
	if errNo != RESULT_SUCCESS {
		return nil, Error(errNo)
	}
	return &rv, nil
}

// Close the iterator and free its associated resources
func (i *Iterator) Close() error {
	errNo := C.fdb_iterator_close(i.iter)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// IteratorInit creates an iterator to traverse a ForestDB snapshot by key range
func (d *Database) IteratorInit(startKey, endKey []byte, opt IteratorOpt) (*Iterator, error) {
	var sk, ek unsafe.Pointer

	lensk := len(startKey)
	lenek := len(endKey)

	if lensk != 0 {
		sk = unsafe.Pointer(&startKey[0])
	}

	if lenek != 0 {
		ek = unsafe.Pointer(&endKey[0])
	}

	rv := Iterator{}
	errNo := C.fdb_iterator_init(d.db, &rv.iter, sk, C.size_t(lensk), ek, C.size_t(lenek), C.fdb_iterator_opt_t(opt))
	if errNo != RESULT_SUCCESS {
		return nil, Error(errNo)
	}
	return &rv, nil
}

// IteratorSequenceInit create an iterator to traverse a ForestDB snapshot by sequence number range
func (d *Database) IteratorSequenceInit(startSeq, endSeq SeqNum, opt IteratorOpt) (*Iterator, error) {
	rv := Iterator{}
	errNo := C.fdb_iterator_sequence_init(d.db, &rv.iter, C.fdb_seqnum_t(startSeq), C.fdb_seqnum_t(endSeq), C.fdb_iterator_opt_t(opt))
	if errNo != RESULT_SUCCESS {
		return nil, Error(errNo)
	}
	return &rv, nil
}
