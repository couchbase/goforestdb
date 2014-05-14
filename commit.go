package forestdb

//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

//#include <forestdb/forestdb.h>
import "C"

// Options to be passed to Commit()
type CommitOpt uint8

const (
	// Perform commit without any options.
	COMMIT_NORMAL CommitOpt = 0x00
	// Manually flush WAL entries even though it doesn't reach the configured threshol
	COMMIT_MANUAL_WAL_FLUSH CommitOpt = 0x01
)

// Commit all pending changes into disk.
func (d *Database) Commit(opt CommitOpt) error {
	errNo := C.fdb_commit(d.db, C.fdb_commit_opt_t(opt))
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// SnapshotOpen creates an snapshot of a database file in ForestDB
func (d *Database) SnapshotOpen(sn SeqNum) (*Database, error) {
	rv := Database{}

	errNo := C.fdb_snapshot_open(d.db, &rv.db, C.fdb_seqnum_t(sn))
	if errNo != RESULT_SUCCESS {
		return nil, Error(errNo)
	}
	return &rv, nil
}

// Rollback a database to a specified point represented by the sequence number
func (d *Database) Rollback(sn SeqNum) error {
	errNo := C.fdb_rollback(&d.db, C.fdb_seqnum_t(sn))
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}
