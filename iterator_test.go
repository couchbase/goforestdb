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

func TestForestDBIterator(t *testing.T) {
	defer os.RemoveAll("test")

	db, err := Open("test", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// store a bunch of values to test the iterator

	db.SetKV([]byte("a"), []byte("vala"))
	db.SetKV([]byte("b"), []byte("valb"))
	db.SetKV([]byte("c"), []byte("valc"))
	db.SetKV([]byte("d"), []byte("vald"))
	db.SetKV([]byte("e"), []byte("vale"))
	db.SetKV([]byte("f"), []byte("valf"))
	db.SetKV([]byte("g"), []byte("valg"))
	db.SetKV([]byte("h"), []byte("valh"))
	db.SetKV([]byte("i"), []byte("vali"))
	db.SetKV([]byte("j"), []byte("valj"))

	iter, err := db.IteratorInit([]byte("c"), []byte("g"), ITR_NONE)
	if err != nil {
		t.Fatal(err)
	}

	doc, err := iter.Next()
	count := 0
	var firstKey, lastKey []byte
	for err == nil {
		count++
		if firstKey == nil {
			firstKey = doc.Key()
		}
		lastKey = doc.Key()
		doc, err = iter.Next()
	}
	if count != 5 {
		t.Errorf("exptected to iterate 5, saw %d", count)
	}
	if string(firstKey) != "c" {
		t.Errorf("expected fist key to be c, got %s", firstKey)
	}
	if string(lastKey) != "g" {
		t.Errorf("expected lats key to be g, got %s", lastKey)
	}
	if err != RESULT_ITERATOR_FAIL {
		t.Errorf("expected %#v, got %#v", RESULT_ITERATOR_FAIL, err)
	}

}

func TestForestDBIteratorSeq(t *testing.T) {
	defer os.RemoveAll("test")

	db, err := Open("test", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// store a bunch of values to test the iterator

	db.SetKV([]byte("a"), []byte("vala"))
	db.SetKV([]byte("b"), []byte("valb"))
	db.SetKV([]byte("c"), []byte("valc"))
	db.SetKV([]byte("d"), []byte("vald"))
	db.SetKV([]byte("e"), []byte("vale"))
	db.SetKV([]byte("f"), []byte("valf"))
	db.SetKV([]byte("g"), []byte("valg"))
	db.SetKV([]byte("h"), []byte("valh"))
	db.SetKV([]byte("i"), []byte("vali"))
	db.SetKV([]byte("j"), []byte("valj"))

	iter, err := db.IteratorSequenceInit(3, 7, ITR_NONE)
	if err != nil {
		t.Fatal(err)
	}

	doc, err := iter.Next()
	count := 0
	var firstKey, lastKey []byte
	for err == nil {
		count++
		if firstKey == nil {
			firstKey = doc.Key()
		}
		lastKey = doc.Key()
		doc, err = iter.Next()
	}
	if count != 5 {
		t.Errorf("exptected to iterate 5, saw %d", count)
	}
	if string(firstKey) != "c" {
		t.Errorf("expected fist key to be c, got %s", firstKey)
	}
	if string(lastKey) != "g" {
		t.Errorf("expected lats key to be g, got %s", lastKey)
	}
	if err != RESULT_ITERATOR_FAIL {
		t.Errorf("expected %#v, got %#v", RESULT_ITERATOR_FAIL, err)
	}

}
