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
//#include <string.h>
//#include <libforestdb/forestdb.h>
import "C"
import "unsafe"

type batchOp struct {
	k    unsafe.Pointer
	klen C.size_t
	v    unsafe.Pointer
	vlen C.size_t
}

type KVBatch struct {
	ops []*batchOp
}

func NewKVBatch() *KVBatch {
	return &KVBatch{
		ops: make([]*batchOp, 0, 100),
	}
}

func (b *KVBatch) Set(k, v []byte) {
	klen := C.size_t(len(k))
	kc := C.malloc(klen)
	C.memmove(kc, unsafe.Pointer(&k[0]), klen)
	vlen := C.size_t(len(v))
	var vc unsafe.Pointer
	if vlen > 0 {
		vc = C.malloc(vlen)
		C.memmove(vc, unsafe.Pointer(&v[0]), vlen)
	}
	b.ops = append(b.ops, &batchOp{
		k:    unsafe.Pointer(kc),
		klen: klen,
		v:    unsafe.Pointer(vc),
		vlen: vlen,
	})
}

func (b *KVBatch) Delete(k []byte) {
	b.Set(k, nil)
}

func (b *KVBatch) Reset() {
	for _, op := range b.ops {
		if op.klen > 0 {
			C.free(op.k)
		}
		if op.vlen > 0 {
			C.free(op.v)
		}
	}
	b.ops = b.ops[:0]
}

func (k *KVStore) ExecuteBatch(b *KVBatch, opt CommitOpt) error {
	for _, op := range b.ops {
		if op.vlen == 0 {
			errNo := C.fdb_del_kv(k.db, op.k, op.klen)
			if errNo != RESULT_SUCCESS {
				return Error(errNo)
			}
		} else {
			errNo := C.fdb_set_kv(k.db, op.k, op.klen, op.v, op.vlen)
			if errNo != RESULT_SUCCESS {
				return Error(errNo)
			}
		}
	}
	return k.File().Commit(opt)
}
