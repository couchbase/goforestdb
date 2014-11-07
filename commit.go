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

// SnapshotOpen creates an snapshot of a database file in ForestDB
func (k *KVStore) SnapshotOpen(sn SeqNum) (*KVStore, error) {
	rv := KVStore{}

	errNo := C.fdb_snapshot_open(k.db, &rv.db, C.fdb_seqnum_t(sn))
	if errNo != RESULT_SUCCESS {
		return nil, Error(errNo)
	}
	return &rv, nil
}

// Rollback a database to a specified point represented by the sequence number
func (k *KVStore) Rollback(sn SeqNum) error {
	errNo := C.fdb_rollback(&k.db, C.fdb_seqnum_t(sn))
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}
