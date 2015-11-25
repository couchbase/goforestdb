package forestdb

import (
	"os"
	"testing"
)

// TestLogCallback intentionally triggers behavior which should invoke the
// logging callback, and verifies that the callback fires as expected.
func TestLogCallback(t *testing.T) {
	defer os.RemoveAll("test")

	// create test file with one k/v pair
	dbfile, err := Open("test", nil)
	if err != nil {
		t.Fatal(err)
	}
	kvstore, err := dbfile.OpenKVStoreDefault(nil)
	if err != nil {
		t.Fatal(err)
	}
	err = kvstore.SetKV([]byte("key"), []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	err = kvstore.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = dbfile.Commit(COMMIT_NORMAL)
	if err != nil {
		t.Fatal(err)
	}
	err = dbfile.Close()
	if err != nil {
		t.Fatal(err)
	}

	// now open it again, this time read only
	dbconfig := DefaultConfig()
	dbconfig.SetOpenFlags(OPEN_FLAG_RDONLY)
	dbfile, err = Open("test", dbconfig)
	if err != nil {
		t.Fatal(err)
	}
	kvstore, err = dbfile.OpenKVStoreDefault(nil)
	if err != nil {
		t.Fatal(err)
	}
	// setup logging
	callbackFired := false
	kvstore.SetLogCallback(func(name string, errCode int, msg string, ctx interface{}) {
		callbackFired = true
		if name != "default" {
			t.Errorf("expected kvstore name to be 'default', got %s", name)
		}
		if errCode != -10 {
			t.Errorf("expected error code -10, got %d", errCode)
		}
		// don't check the message as it could change
	}, nil)
	err = kvstore.SetKV([]byte("key"), []byte("value"))
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !callbackFired {
		t.Errorf("expected log callback to fire, it didn't")
	}
	err = kvstore.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = dbfile.Close()
	if err != nil {
		t.Fatal(err)
	}

}

// if you want to assure yourself that the fatal error
// callback fires:
//
// 1.  add these to the cgo defintions at the top of forestdb.go
//
//extern uint64_t _kvs_stat_get_sum_attr(void *data, uint64_t version, int attr);
//void assertFail() {
//    char *data = malloc(1024);
//    data[0]=1;
//    _kvs_stat_get_sum_attr(data, 2, 25);
//}
//
// 2. add this to forestdb.go
//
// func fdbAssertFail() {
// 	C.assertFail()
// }
//
// 3. uncomment and run the test case below:
//
// func TestFatalErrorCallback(t *testing.T) {
// 	SetFatalErrorCallback(func() {
// 		log.Printf("got fatal error")
// 	})
// 	fdbAssertFail()
// }
