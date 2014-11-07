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

func TestForestDBCrud(t *testing.T) {
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

	// check the kvstore info
	kvInfo, err := kvstore.Info()
	if err != nil {
		t.Error(err)
	}
	if kvInfo.LastSeqNum() != 0 {
		t.Errorf("expected last_seqnum to be 0, got %d", kvInfo.LastSeqNum())
	}

	// get a non-existant key
	doc, err := NewDoc([]byte("doesnotexist"), nil, nil)
	if err != nil {
		t.Error(err)
	}
	err = kvstore.Get(doc)
	if err != RESULT_KEY_NOT_FOUND {
		t.Errorf("expected %v, got %v", RESULT_KEY_NOT_FOUND, err)
	}
	doc.Close()

	// put a new key
	doc, err = NewDoc([]byte("key1"), nil, []byte("value1"))
	if err != nil {
		t.Error(err)
	}
	err = kvstore.Set(doc)
	if err != nil {
		t.Error(err)
	}
	doc.Close()

	// lookup that key
	doc, err = NewDoc([]byte("key1"), nil, nil)
	if err != nil {
		t.Error(err)
	}
	err = kvstore.Get(doc)
	if err != nil {
		t.Error(err)
	}
	if string(doc.Body()) != "value1" {
		t.Errorf("expected value1, got %s", doc.Body())
	}
	doc.Close()

	// update it
	doc, err = NewDoc([]byte("key1"), nil, []byte("value1-updated"))
	if err != nil {
		t.Error(err)
	}
	err = kvstore.Set(doc)
	if err != nil {
		t.Error(err)
	}
	doc.Close()

	// look it up again
	doc, err = NewDoc([]byte("key1"), nil, nil)
	if err != nil {
		t.Error(err)
	}
	err = kvstore.Get(doc)
	if err != nil {
		t.Error(err)
	}
	if string(doc.Body()) != "value1-updated" {
		t.Errorf("expected value1-updated, got %s", doc.Body())
	}
	doc.Close()

	// delete it
	doc, err = NewDoc([]byte("key1"), nil, nil)
	if err != nil {
		t.Error(err)
	}
	err = kvstore.Delete(doc)
	if err != nil {
		t.Error(err)
	}
	doc.Close()

	// look it up again
	doc, err = NewDoc([]byte("key1"), nil, nil)
	if err != nil {
		t.Error(err)
	}
	err = kvstore.Get(doc)
	if err != RESULT_KEY_NOT_FOUND {
		t.Error(err)
	}
	doc.Close()

	// delete it again
	doc, err = NewDoc([]byte("key1"), nil, nil)
	if err != nil {
		t.Error(err)
	}
	err = kvstore.Delete(doc)
	if err != nil {
		t.Error(err)
	}
	doc.Close()

	// dete non-existant key
	doc, err = NewDoc([]byte("doesnotext"), nil, nil)
	if err != nil {
		t.Error(err)
	}
	err = kvstore.Delete(doc)
	if err != nil {
		t.Error(err)
	}
	doc.Close()

	// check the db info at the end
	kvInfo, err = kvstore.Info()
	if err != nil {
		t.Error(err)
	}
	if kvInfo.LastSeqNum() != 5 {
		t.Errorf("expected last_seqnum to be 0, got %d", kvInfo.LastSeqNum())
	}
}
