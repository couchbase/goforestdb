package custom_comparator

import (
	"os"
	"testing"
	"unsafe"

	"github.com/couchbaselabs/goforestdb"
)

func TestForestCustomComparator(t *testing.T) {
	defer os.RemoveAll("test")

	config := forestdb.DefaultConfig()
	config.CustomCompareVariable = unsafe.Pointer(CompareBytesReversedPointer)

	db, err := forestdb.OpenCmpVariable("test", config)
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

	iter, err := db.IteratorInit([]byte("g"), []byte("c"), forestdb.ITR_NONE)
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
	if string(firstKey) != "g" {
		t.Errorf("expected fist key to be c, got %s", firstKey)
	}
	if string(lastKey) != "c" {
		t.Errorf("expected lats key to be g, got %s", lastKey)
	}
	if err != forestdb.RESULT_ITERATOR_FAIL {
		t.Errorf("expected %#v, got %#v", forestdb.RESULT_ITERATOR_FAIL, err)
	}
}
