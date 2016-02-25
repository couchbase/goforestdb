//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package forestdb

import (
	"os"
	"testing"
)

func TestForestDBKVBatch(t *testing.T) {
	defer os.RemoveAll("test")

	dbfile, err := Open("test", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer dbfile.Close()

	kvstore, err := dbfile.OpenKVStoreDefault(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer kvstore.Close()

	batch := NewKVBatch()
	batch.Set([]byte("a"), []byte("a-val"))
	batch.Set([]byte("b"), []byte("b-val"))
	batch.Set([]byte("c"), []byte("c-val"))

	err = kvstore.ExecuteBatch(batch, COMMIT_NORMAL)
	if err != nil {
		t.Fatal(err)
	}

	// lookup these key
	val, err := kvstore.GetKV([]byte("a"))
	if err != nil {
		t.Error(err)
	}
	if string(val) != "a-val" {
		t.Errorf("expected a-val, got %s", val)
	}

	val, err = kvstore.GetKV([]byte("b"))
	if err != nil {
		t.Error(err)
	}
	if string(val) != "b-val" {
		t.Errorf("expected b-val, got %s", val)
	}

	val, err = kvstore.GetKV([]byte("c"))
	if err != nil {
		t.Error(err)
	}
	if string(val) != "c-val" {
		t.Errorf("expected c-val, got %s", val)
	}

	// reset the batch and reuse it
	batch.Reset()

	batch.Delete([]byte("c"))
	batch.Set([]byte("d"), []byte("d-val"))
	batch.Set([]byte("e"), []byte("e-val"))

	err = kvstore.ExecuteBatch(batch, COMMIT_NORMAL)
	if err != nil {
		t.Fatal(err)
	}

	// lookup these key
	val, err = kvstore.GetKV([]byte("a"))
	if err != nil {
		t.Error(err)
	}
	if string(val) != "a-val" {
		t.Errorf("expected a-val, got %s", val)
	}

	val, err = kvstore.GetKV([]byte("b"))
	if err != nil {
		t.Error(err)
	}
	if string(val) != "b-val" {
		t.Errorf("expected b-val, got %s", val)
	}

	val, err = kvstore.GetKV([]byte("c"))
	if err != RESULT_KEY_NOT_FOUND {
		t.Error(err)
	}

	val, err = kvstore.GetKV([]byte("d"))
	if err != nil {
		t.Error(err)
	}
	if string(val) != "d-val" {
		t.Errorf("expected d-val, got %s", val)
	}

	val, err = kvstore.GetKV([]byte("e"))
	if err != nil {
		t.Error(err)
	}
	if string(val) != "e-val" {
		t.Errorf("expected e-val, got %s", val)
	}

	// reset the batch and reuse it
	batch.Reset()
	// set e to value of length 0
	batch.Set([]byte("e"), []byte{})

	err = kvstore.ExecuteBatch(batch, COMMIT_NORMAL)
	if err != nil {
		t.Fatal(err)
	}

	val, err = kvstore.GetKV([]byte("e"))
	if err != nil {
		t.Error(err)
	}
	if string(val) != "" {
		t.Errorf("expected e-val, got %s", val)
	}

}
