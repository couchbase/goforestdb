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
	"log"
	"os"
	"testing"
)

func TestKVSOpsInfo(t *testing.T) {
	defer os.RemoveAll("test")
	defer os.RemoveAll("test2")

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

	opsInfo, err := kvstore.OpsInfo()
	if err != nil {
		log.Fatal(err)
	}

	if opsInfo.NumSets() != 0 {
		t.Fatalf("expected 0 sets, got %d", opsInfo.NumSets())
	}
	if opsInfo.NumDels() != 0 {
		t.Fatalf("expected 0 dels, got %d", opsInfo.NumDels())
	}
	if opsInfo.NumCommits() != 0 {
		t.Fatalf("expected 0 commits, got %d", opsInfo.NumCommits())
	}
	if opsInfo.NumCompacts() != 0 {
		t.Fatalf("expected 0 compacts, got %d", opsInfo.NumCompacts())
	}
	if opsInfo.NumGets() != 0 {
		t.Fatalf("expected 0 gets, got %d", opsInfo.NumGets())
	}
	if opsInfo.NumIteratorGets() != 0 {
		t.Fatalf("expected 0 iterator gets, got %d", opsInfo.NumIteratorGets())
	}
	if opsInfo.NumIteratorMoves() != 0 {
		t.Fatalf("expected 0 iterator moves, got %d", opsInfo.NumIteratorMoves())
	}

	err = kvstore.SetKV([]byte("key"), []byte("val"))
	if err != nil {
		log.Fatal(err)
	}

	err = dbfile.Commit(COMMIT_NORMAL)
	if err != nil {
		log.Fatal(err)
	}

	itr, err := kvstore.IteratorInit(nil, nil, IteratorOpt(0))
	if err != nil {
		log.Fatal(err)
	}
	defer itr.Close()

	_, err = itr.Get()
	if err != nil {
		log.Fatal(err)
	}

	_, err = kvstore.GetKV([]byte("key"))
	if err != nil {
		log.Fatal(err)
	}

	err = kvstore.DeleteKV([]byte("key"))
	if err != nil {
		log.Fatal(err)
	}

	err = dbfile.Compact("test2")
	if err != nil {
		log.Fatal(err)
	}

	opsInfo, err = kvstore.OpsInfo()
	if err != nil {
		log.Fatal(err)
	}

	if opsInfo.NumSets() != 1 {
		t.Fatalf("expected 1 sets, got %d", opsInfo.NumSets())
	}
	if opsInfo.NumDels() != 1 {
		t.Fatalf("expected 1 dels, got %d", opsInfo.NumDels())
	}
	if opsInfo.NumCommits() != 1 {
		t.Fatalf("expected 1 commits, got %d", opsInfo.NumCommits())
	}
	if opsInfo.NumCompacts() != 1 {
		t.Fatalf("expected 1 compacts, got %d", opsInfo.NumCompacts())
	}
	if opsInfo.NumGets() != 1 {
		t.Fatalf("expected 1 gets, got %d", opsInfo.NumGets())
	}
	if opsInfo.NumIteratorGets() != 1 {
		t.Fatalf("expected 1 iterator gets, got %d", opsInfo.NumIteratorGets())
	}
	if opsInfo.NumIteratorMoves() != 1 {
		t.Fatalf("expected 1 iterator moves, got %d", opsInfo.NumIteratorMoves())
	}

}
