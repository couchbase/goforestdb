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
	return fmt.Sprintf("filename: %s new_filename: %s doc_count: %d space_used: %d file_size: %d", i.Filename(), i.NewFilename(), i.DocCount(), i.SpaceUsed(), i.FileSize())
}

// KVStoreInfo stores information about a given kvstore
type KVStoreInfo struct {
	info C.fdb_kvs_info
}

func (i *KVStoreInfo) Name() string {
	return C.GoString(i.info.name)
}

func (i *KVStoreInfo) LastSeqNum() SeqNum {
	return SeqNum(i.info.last_seqnum)
}

func (i *KVStoreInfo) String() string {
	return fmt.Sprintf("name: %s last_seqnum: %d", i.Name(), i.LastSeqNum())
}
