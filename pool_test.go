package forestdb

import (
	"os"
	"testing"
)

func TestPool(t *testing.T) {
	defer os.RemoveAll("test")

	// create a pool of 10 forestdb clients for file: test kvstore: default
	fdbConfig := DefaultConfig()
	kvConfig := DefaultKVStoreConfig()
	kvpool, err := NewKVPool("test", fdbConfig, "default", kvConfig, 10)
	if err != nil {
		t.Fatal(err)
	}

	// get from the pool
	kvs, err := kvpool.Get()
	if err != nil {
		t.Fatal(err)
	}

	// return to pool
	err = kvpool.Return(kvs)
	if err != nil {
		t.Fatal(err)
	}

	// close the pool
	err = kvpool.Close()
	if err != nil {
		t.Fatal(err)
	}

	// try to get after closing
	_, err = kvpool.Get()
	if err != PoolClosed {
		t.Errorf("expected %v, got %v when calling Get on closed pool", PoolClosed, err)
	}

	// try to return after closing
	err = kvpool.Return(kvs)
	if err != PoolClosed {
		t.Errorf("expected %v, got %v when calling Return on closed pool", PoolClosed, err)
	}

}
