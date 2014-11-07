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

	// store a bunch of values to test the iterator

	kvstore.SetKV([]byte("a"), []byte("vala"))
	kvstore.SetKV([]byte("b"), []byte("valb"))
	kvstore.SetKV([]byte("c"), []byte("valc"))
	kvstore.SetKV([]byte("d"), []byte("vald"))
	kvstore.SetKV([]byte("e"), []byte("vale"))
	kvstore.SetKV([]byte("f"), []byte("valf"))
	kvstore.SetKV([]byte("g"), []byte("valg"))
	kvstore.SetKV([]byte("h"), []byte("valh"))
	kvstore.SetKV([]byte("i"), []byte("vali"))
	kvstore.SetKV([]byte("j"), []byte("valj"))

	iter, err := kvstore.IteratorInit([]byte("c"), []byte("g"), ITR_NONE)
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

	// store a bunch of values to test the iterator

	kvstore.SetKV([]byte("a"), []byte("vala"))
	kvstore.SetKV([]byte("b"), []byte("valb"))
	kvstore.SetKV([]byte("c"), []byte("valc"))
	kvstore.SetKV([]byte("d"), []byte("vald"))
	kvstore.SetKV([]byte("e"), []byte("vale"))
	kvstore.SetKV([]byte("f"), []byte("valf"))
	kvstore.SetKV([]byte("g"), []byte("valg"))
	kvstore.SetKV([]byte("h"), []byte("valh"))
	kvstore.SetKV([]byte("i"), []byte("vali"))
	kvstore.SetKV([]byte("j"), []byte("valj"))

	iter, err := kvstore.IteratorSequenceInit(3, 7, ITR_NONE)
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

func TestForestDBIteratorSeek(t *testing.T) {
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

	// store a bunch of values to test the iterator

	kvstore.SetKV([]byte("a"), []byte("vala"))
	kvstore.SetKV([]byte("b"), []byte("valb"))
	kvstore.SetKV([]byte("c"), []byte("valc"))
	kvstore.SetKV([]byte("d"), []byte("vald"))
	kvstore.SetKV([]byte("e"), []byte("vale"))
	kvstore.SetKV([]byte("f"), []byte("valf"))
	kvstore.SetKV([]byte("g"), []byte("valg"))
	kvstore.SetKV([]byte("i"), []byte("vali"))
	kvstore.SetKV([]byte("j"), []byte("valj"))

	iter, err := kvstore.IteratorInit([]byte("c"), []byte("j"), ITR_NONE)
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()

	doc, err := iter.Next()
	if err != nil {
		t.Fatal(err)
	}
	key := doc.Key()
	if string(key) != "c" {
		t.Fatalf("expected first key 'c', got %s", string(key))
	}

	// now seek to e (exists) should skip over d
	err = iter.Seek([]byte("e"))
	if err != nil {
		t.Fatal(err)
	}
	doc, err = iter.Next()
	if err != nil {
		t.Fatal(err)
	}
	key = doc.Key()
	if string(key) != "e" {
		t.Fatalf("expected first key 'e', got %s", string(key))
	}

	// now seek to h (does not exist) should be on i
	err = iter.Seek([]byte("h"))
	if err != nil {
		t.Fatal(err)
	}
	doc, err = iter.Next()
	if err != nil {
		t.Fatal(err)
	}
	key = doc.Key()
	if string(key) != "i" {
		t.Fatalf("expected first key 'i', got %s", string(key))
	}
}
