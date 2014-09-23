package forestdb

//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

//#include <stdlib.h>
//#include <libforestdb/forestdb.h>
import "C"

import (
	"unsafe"
)

// GetKV simplified API for key/value access to Get()
func (d *Database) GetKV(key []byte) ([]byte, error) {

	var k unsafe.Pointer
	if len(key) != 0 {
		k = unsafe.Pointer(&key[0])
	}
	lenk := len(key)

	var bodyLen C.size_t
	var bodyPointer unsafe.Pointer

	errNo := C.fdb_get_kv(d.db, k, C.size_t(lenk), &bodyPointer, &bodyLen)
	if errNo != RESULT_SUCCESS {
		return nil, Error(errNo)
	}

	body := C.GoBytes(bodyPointer, C.int(bodyLen))
	C.free(bodyPointer)
	return body, nil
}

// SetKV simplified API for key/value access to Set()
func (d *Database) SetKV(key, value []byte) error {

	var k, v unsafe.Pointer

	if len(key) != 0 {
		k = unsafe.Pointer(&key[0])
	}

	if len(value) != 0 {
		v = unsafe.Pointer(&value[0])
	}

	lenk := len(key)
	lenv := len(value)

	errNo := C.fdb_set_kv(d.db, k, C.size_t(lenk), v, C.size_t(lenv))
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// DeleteKV simplified API for key/value access to Delete()
func (d *Database) DeleteKV(key []byte) error {

	var k unsafe.Pointer
	if len(key) != 0 {
		k = unsafe.Pointer(&key[0])
	}

	lenk := len(key)

	errNo := C.fdb_del_kv(d.db, k, C.size_t(lenk))
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}
