package forestdb

import (
	"os"
	"testing"
)

func TestSnapshotAndRollback(t *testing.T) {

	defer os.RemoveAll("test")

	db, err := Open("test", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// put a new key
	doc, err := NewDoc([]byte("key1"), nil, []byte("value1"))
	if err != nil {
		t.Error(err)
	}
	err = db.Set(doc)
	if err != nil {
		t.Error(err)
	}
	snapshotSeqNum := doc.SeqNum()
	doc.Close()

	// commit changes
	err = db.Commit(COMMIT_NORMAL)
	if err != nil {
		t.Error(err)
	}

	// get a snapshot
	dbSnapshot, err := db.SnapshotOpen(snapshotSeqNum)
	if err != nil {
		t.Fatal(err)
	}
	defer dbSnapshot.Close()

	// update the original key
	doc, err = NewDoc([]byte("key1"), nil, []byte("value1-updated"))
	if err != nil {
		t.Error(err)
	}
	err = db.Set(doc)
	if err != nil {
		t.Error(err)
	}
	doc.Close()

	// get the document using the regular db handle
	doc, err = NewDoc([]byte("key1"), nil, nil)
	if err != nil {
		t.Error(err)
	}
	err = db.Get(doc)
	if err != nil {
		t.Error(err)
	}
	// verify we get the updated version
	if string(doc.Body()) != "value1-updated" {
		t.Errorf("expected value1-updated, got %s", doc.Body())
	}
	doc.Close()

	// get the document, using the snapshot before the update
	doc, err = NewDoc([]byte("key1"), nil, nil)
	if err != nil {
		t.Error(err)
	}
	err = dbSnapshot.Get(doc)
	if err != nil {
		t.Error(err)
	}
	// verify we get the version before we took the snapshot
	if string(doc.Body()) != "value1" {
		t.Errorf("expected value1, got %s", doc.Body())
	}
	doc.Close()

	// now ask the db to rollback to the snapshot seqnum
	err = db.Rollback(snapshotSeqNum)
	if err != nil {
		t.Error(err)
	}

	// get the document using the regular db handle again
	doc, err = NewDoc([]byte("key1"), nil, nil)
	if err != nil {
		t.Error(err)
	}
	err = db.Get(doc)
	if err != nil {
		t.Error(err)
	}
	// verify we get the non-updated version
	if string(doc.Body()) != "value1" {
		t.Errorf("expected value1, got %s", doc.Body())
	}
	doc.Close()
}
