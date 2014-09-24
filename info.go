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
	"fmt"
)

// EstimateSpaceUsed returns the overall disk space actively used by the current database file
func (d *Database) EstimateSpaceUsed() int {
	return int(C.fdb_estimate_space_used(d.db))
}

// DatabaseInfo stores information about a given database file
type DatabaseInfo struct {
	info C.fdb_info
}

func (i *DatabaseInfo) Filename() string {
	return C.GoString(i.info.filename)
}

func (i *DatabaseInfo) NewFilename() string {
	return C.GoString(i.info.new_filename)
}

func (i *DatabaseInfo) LastSeqNum() SeqNum {
	return SeqNum(i.info.last_seqnum)
}

func (i *DatabaseInfo) DocCount() uint64 {
	return uint64(i.info.doc_count)
}

func (i *DatabaseInfo) SpaceUsed() uint64 {
	return uint64(i.info.space_used)
}

func (i *DatabaseInfo) FileSize() uint64 {
	return uint64(i.info.file_size)
}

func (i *DatabaseInfo) String() string {
	return fmt.Sprintf("filename: %s new_filename: %s last_seqnum: %d doc_count: %d space_used: %d file_size: %d", i.Filename(), i.NewFilename(), i.LastSeqNum(), i.DocCount(), i.SpaceUsed(), i.FileSize())
}

// DbInfo returns the information about a given database handle
func (d *Database) DbInfo() (*DatabaseInfo, error) {
	rv := DatabaseInfo{}
	errNo := C.fdb_get_dbinfo(d.db, &rv.info)
	if errNo != RESULT_SUCCESS {
		return nil, Error(errNo)
	}
	return &rv, nil
}
