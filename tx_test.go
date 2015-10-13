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
	"reflect"
	"testing"
)

func lookupKeyInTest(key []byte) ([]byte, error) {
	dbfile, err := Open("test", nil)
	if err != nil {
		return nil, err
	}
	defer dbfile.Close()

	kvstore, err := dbfile.OpenKVStoreDefault(nil)
	if err != nil {
		return nil, err
	}
	defer kvstore.Close()

	return kvstore.GetKV(key)
}

func TestTx(t *testing.T) {

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

	// first set a value with no tx
	err = kvstore.SetKV([]byte("a"), []byte("a"))
	if err != nil {
		t.Fatal(err)
	}

	// make sure another reader can see it immediately
	val, err := lookupKeyInTest([]byte("a"))
	if err != nil || !reflect.DeepEqual(val, []byte("a")) {
		t.Errorf("expected to see a, got % x - %v", val, err)
	}

	// now start a read-committed tx
	err = dbfile.BeginTransaction(ISOLATION_READ_COMMITTED)
	if err != nil {
		t.Fatal(err)
	}

	err = kvstore.SetKV([]byte("b"), []byte("b"))
	if err != nil {
		t.Fatal(err)
	}

	// reader can't see this, tx in progress
	val, err = lookupKeyInTest([]byte("b"))
	if err != RESULT_KEY_NOT_FOUND {
		t.Errorf("expected not see b, tx in progress")
	}

	err = dbfile.EndTransaction(COMMIT_NORMAL)
	if err != nil {
		t.Fatal(err)
	}

	// expect to see it after commit transaction
	val, err = lookupKeyInTest([]byte("b"))
	if err != nil || !reflect.DeepEqual(val, []byte("b")) {
		t.Errorf("expected after tx to see b, got % x - %v", val, err)
	}

	// now start another read-committed tx
	err = dbfile.BeginTransaction(ISOLATION_READ_COMMITTED)
	if err != nil {
		t.Fatal(err)
	}

	err = kvstore.SetKV([]byte("c"), []byte("c"))
	if err != nil {
		t.Fatal(err)
	}

	// reader can't see this, tx in progress
	val, err = lookupKeyInTest([]byte("c"))
	if err != RESULT_KEY_NOT_FOUND {
		t.Errorf("expected not see c, tx in progress")
	}

	// this time abort the tx
	err = dbfile.AbortTransaction()
	if err != nil {
		t.Fatal(err)
	}

	// reader still can't see this, tx aborted
	val, err = lookupKeyInTest([]byte("c"))
	if err != RESULT_KEY_NOT_FOUND {
		t.Errorf("expected not see c, tx in progress")
	}
}
