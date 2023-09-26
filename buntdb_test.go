package swmemdb

import (
	"os"
	"testing"
	"time"

	"github.com/tidwall/buntdb"
)

// Test NewConnection
func TestNewConnection(t *testing.T) {
	db := NewBuntDb(WithFile("test_"+getTempFileName()), WithMode("memory"), WithCollection("testtable"))
	if db == nil {
		t.Errorf("NewConnection() = %v, want %v", db, "not nil")
	}

	// close the connection
	err := db.db.Close()
	if err != nil {
		t.Errorf("Close() = %v, want %v", err, "nil")
	}

}

// Test Init
func TestInit(t *testing.T) {
	db := NewBuntDb(WithFile("test_"+getTempFileName()), WithMode("memory"), WithCollection("testtable"))
	err := db.Init()
	if err != nil {
		t.Errorf("Init() = %v, want %v", err, "nil")
	}
}

// Test Set
func TestSet(t *testing.T) {
	db := NewBuntDb(WithFile("test_"+getTempFileName()), WithMode("memory"), WithCollection("testtable"))
	err := db.Init()
	if err != nil {
		t.Errorf("Init() = %v, want %v", err, "nil")
	}

	err = db.Set("testkey", "testvalue", 0)
	if err != nil {
		t.Errorf("Set() = %v, want %v", err, "nil")
	}

	err = nil

	// check that the key exists
	val, err := db.Get("testkey")
	if err != nil {
		t.Errorf("Get() = %v, want %v", err, "nil")
	} else {
		if val != "testvalue" {
			t.Errorf("Get() = %v, want %v", val, "testvalue")
		}
	}

	// close the connection
	err = db.Close()
	if err != nil {
		t.Errorf("Close() = %v, want %v", err, "nil")
	}

	// delete the database file
	os.Remove("./" + db.file)

}

// Test Get
func TestGet(t *testing.T) {
	db := NewBuntDb(WithFile("test_"+getTempFileName()), WithMode("memory"), WithCollection("test332"), WithSyncPolicy(buntdb.Always))
	err := db.Init()
	if err != nil {
		t.Errorf("Init() = %v, want %v", err, "nil")
	}

	err = db.Set("testkey", "testvalue", 5*time.Second)
	if err != nil {
		t.Errorf("Set() = %v, want %v", err, "nil")
	}

	val, err := db.Get("testkey")
	if err != nil {
		t.Errorf("Get() = %v, want %v", err, "nil")
	}

	if val != "testvalue" {
		t.Errorf("Get() = %v, want %v", val, "testvalue")
	}

	// close the connection
	err = db.db.Close()
	if err != nil {
		t.Errorf("Close() = %v, want %v", err, "nil")
	}

}

// Test Get with non existing key
func TestGetWithNonExistingKey(t *testing.T) {
	db := NewBuntDb(WithFile("test_"+getTempFileName()), WithMode("memory"), WithCollection("testtable"))
	err := db.Init()
	if err != nil {
		t.Errorf("Init() = %v, want %v", err, "nil")
	}

	err = nil
	// check that the key does not exist, err should be ErrNotFound (not nil) and val should be ""
	val, err := db.Get("testkeynotfound")
	// check if err is not nil
	if err != nil {
		// err should be ErrNotFound
		if err.Error() != buntdb.ErrNotFound.Error() {
			t.Errorf("Get() = %v, want %v", val, "ErrNotFound")
		}
	}

	if val != "" {
		t.Errorf("Get() = %v, want %v", val, "")
	}

	// close the connection
	err = db.Close()
	if err != nil {
		t.Errorf("Close() = %v, want %v", err, "nil")
	}

}

// Test Close
func TestClose(t *testing.T) {
	db := NewBuntDb(WithFile("test_"+getTempFileName()), WithMode("memory"), WithCollection("testtable"))
	err := db.Init()
	if err != nil {
		t.Errorf("Init() = %v, want %v", err, "nil")
	}

	err = db.Set("testkey", "testvalue", 0)
	if err != nil {
		t.Errorf("Set() = %v, want %v", err, "nil")
	}

	err = db.Close()
	if err != nil {
		t.Errorf("Close() = %v, want %v", err, "nil")
	}

	db.db = nil

	// is db closed?
	if db.db != nil {
		t.Errorf("db.db = %v, want %v", db.db, "nil")
	}

}

// Test Delete
func TestDelete(t *testing.T) {
	db := NewBuntDb(WithFile("test_"+getTempFileName()), WithMode("memory"), WithCollection("testtable"))
	err := db.Init()
	if err != nil {
		t.Errorf("Init() = %v, want %v", err, "nil")
	}

	err = db.Set("testkey", "testvalue", 10*time.Second)
	if err != nil {
		t.Errorf("Set() = %v, want %v", err, "nil")
	}

	// check that the key exists
	val, err := db.Get("testkey")
	if err != nil {
		t.Errorf("Get() = %v, want %v", err, "nil")
	}

	if val != "testvalue" {
		t.Errorf("Get() = %v, want %v", val, "testvalue")
	}

	// delete the key
	err = db.Delete("testkey")
	if err != nil {
		t.Errorf("Delete() = %v, want %v", err, nil)
	}
}

// Test Delete with non existing key
func TestDeleteWithNonExistingKey(t *testing.T) {
	db := NewBuntDb(WithFile("test_"+getTempFileName()), WithMode("memory"), WithCollection("testtable"))
	err := db.Init()
	if err != nil {
		t.Errorf("Init() = %v, want %v", err, "nil")
	}

	err = nil
	// delete the key
	err = db.Delete("testkey")
	if err != nil && err.Error() != "not found" {
		t.Errorf("Delete() = %v, want %v", err.Error(), "not found")
	}
}

// Test Set with expiration
func TestSetWithExpiration(t *testing.T) {
	db := NewBuntDb(WithFile("test_"+getTempFileName()), WithMode("memory"), WithCollection("testtable"))
	err := db.Init()
	if err != nil {
		t.Errorf("Init() = %v, want %v", err, "nil")
	}

	err = db.Set("testkey", "testvalue", 5*time.Second)
	if err != nil {
		t.Errorf("Set() = %v, want %v", err, "nil")
	}

	// check that the key exists
	val, err := db.Get("testkey")
	if err != nil {
		t.Errorf("Get() = %v, want %v", err, "nil")
	}

	if val != "testvalue" {
		t.Errorf("Get() = %v, want %v", val, "testvalue")
	}

	// wait for the key to expire
	time.Sleep(5 * time.Second)

	// check that the key does not exist, err should be ErrNotFound (not nil) and val should be ""
	val, err = db.Get("testkey")
	// check if err is not nil
	if err != nil {
		// err should be ErrNotFound
		if err != buntdb.ErrNotFound {
			t.Errorf("Get() = %v, want %v", val, "ErrNotFound")
		}
	} else {
		t.Errorf("Get() = %v, want %v", err, "nil")
	}

	// if val != "" {
	// 	t.Errorf("Get() = %v, want %v", val, "")
	// }

	// close the connection
	err = db.Close()
	if err != nil {
		t.Errorf("Close() = %v, want %v", err, "nil")
	}

}

// Test creating two collections in the same database
func TestCreateTwoCollectionsInSameDatabase(t *testing.T) {
	db := NewBuntDb(WithFile("test_"+getTempFileName()), WithMode("memory"), WithCollection("testtable1"))
	err := db.Init()
	if err != nil {
		t.Errorf("Init() = %v, want %v", err, "nil")
	}
	// close the connection
	err = db.Close()
	if err != nil {
		t.Errorf("Close() = %v, want %v", err, "nil")
	}

	db = NewBuntDb(WithFile(db.file), WithMode("memory"), WithCollection("testtable2"))
	err = db.Init()
	if err != nil {
		t.Errorf("Init() = %v, want %v", err, "nil")
	}
	// close the connection
	err = db.Close()
	if err != nil {
		t.Errorf("Close() = %v, want %v", err, "nil")
	}
}

// Test creating two collections in the same database and setting data in both
func TestCreateTwoCollectionsInSameDatabaseAndSetDataInBoth(t *testing.T) {
	db := NewBuntDb(WithFile("test_"+getTempFileName()), WithMode("memory"), WithCollection("testtable1"))

	// write a key/value to the first collection
	err := db.SetWithNoExpiration("testkey1", "testvalue1")
	if err != nil {
		t.Errorf("SetWithNoExpiration() = %v, want %v", err, "nil")
	}

	// close the connection
	err = db.Close()
	if err != nil {
		t.Errorf("Close() = %v, want %v", err, "nil")
	}

	db = NewBuntDb(WithFile(db.file), WithMode("memory"), WithCollection("testtable2"))
	// err = db.Init()
	// if err != nil {
	// 	t.Errorf("Init() = %v, want %v", err, "nil")
	// }

	// set some data in the first collection
	str := "testvalue1"

	// set the key/value
	err = db.Set("testkey1", str, 10*time.Second)
	if err != nil {
		t.Errorf("Set() = %v, want %v", err, "nil")
	}

	// set some data in the second collection
	str = "testvalue2"

	// set the key/value
	err = db.Set("testkey2", str, 10*time.Second)
	if err != nil {
		t.Errorf("Set() = %v, want %v", err, "nil")
	}

	// close the connection
	err = db.Close()
	if err != nil {
		t.Errorf("Close() = %v, want %v", err, "nil")
	}

	// open the first database again
	db = NewBuntDb(WithFile(db.file), WithMode("memory"), WithCollection("testtable1"))

	// check that the key exists in the first collection
	val, err := db.Get("testkey1")
	if err != nil {
		t.Errorf("Get() = %v, want %v", err, "nil")
	}

	if val != "testvalue1" {
		t.Errorf("Get() = %v, want %v", val, "testvalue1")
	}

	// check that the key does not exist in the second collection
	val, err = db.Get("testkey2")
	if val != "" {
		t.Errorf("Get() = %v, want %v", err, "nil")
	}

	// close the connection
	err = db.Close()
	if err != nil {
		t.Errorf("Close() = %v, want %v", err, "nil")
	}

	// open the second database again
	db = NewBuntDb(WithFile(db.file), WithMode("memory"), WithCollection("testtable2"))

	// check that the key exists in the second collection
	val, err = db.Get("testkey2")
	if err != nil {
		t.Errorf("Get() = %v, want %v", err, "nil")
	}

	if val != "testvalue2" {
		t.Errorf("Get() = %v, want %v", val, "testvalue2")
	}

	// check that the key does not exist in the first collection
	val, err = db.Get("testkey1")
	if err != nil {
		t.Errorf("Get() = %v, want %v", val, "testvalue1")
	}

	// close the connection
	err = db.Close()
	if err != nil {
		t.Errorf("Close() = %v, want %v", err, "nil")
	}

}

// Test SetWithNoExpiration
func TestSetWithNoExpiration(t *testing.T) {
	db1 := NewBuntDb(WithFile("test_"+getTempFileName()), WithMode("memory"), WithCollection("testtable1"))
	err := db1.Init()
	if err != nil {
		t.Errorf("Init() = %v, want %v", err, "nil")
	}

	// set some data in the first collection
	str := "testvalue1"

	// set the key/value
	err = db1.SetWithNoExpiration("testkey1", str)
	if err != nil {
		t.Errorf("SetWithNoExpiration() = %v, want %v", err, "nil")
	}

	// close the connection
	err = db1.Close()
	if err != nil {
		t.Errorf("Close() = %v, want %v", err, "nil")
	}

	db2 := NewBuntDb(WithFile(db1.file), WithMode("memory"), WithCollection("testtable2"))
	err = db2.Init()
	if err != nil {
		t.Errorf("Init() = %v, want %v", err, "nil")
	}

	// set some data in the second collection
	str = "testvalue2"

	// set the key/value
	err = db2.SetWithNoExpiration("testkey2", str)
	if err != nil {
		t.Errorf("SetWithNoExpiration() = %v, want %v", err, "nil")
	}

	// close the connection
	err = db2.Close()
	if err != nil {

		t.Errorf("Close() = %v, want %v", err, "nil")
	}

	// open the first database again
	db1 = NewBuntDb(WithFile(db1.file), WithMode("memory"), WithCollection("testtable1"))
	err = db1.Init()
	if err != nil {
		t.Errorf("Init() = %v, want %v", err, "nil")
	}

	// check that the key exists in the first collection
	val, err := db1.Get("testkey1")
	if err != nil {
		t.Errorf("Get() = %v, want %v", err, "nil")
	}

	if val != "testvalue1" {
		t.Errorf("Get() = %v, want %v", val, "testvalue1")
	}

	// check that the key does not exist in the second collection
	val, err = db1.Get("testkey2")
	if err != nil {
		t.Errorf("Get() = %v, want %v", err, "nil")
	}

	// close the connection
	err = db1.Close()
	if err != nil {
		t.Errorf("Close() = %v, want %v", err, "nil")
	}

	// open the second database again
	db2 = NewBuntDb(WithFile(db1.file), WithMode("memory"), WithCollection("testtable2"))
	err = db2.Init()
	if err != nil {
		t.Errorf("Init() = %v, want %v", err, "nil")
	}

	// check that the key exists in the second collection
	val, err = db2.Get("testkey2")
	if err != nil {
		t.Errorf("Get() = %v, want %v", err, "nil")
	}

	if val != "testvalue2" {
		t.Errorf("Get() = %v, want %v", val, "testvalue2")
	}

	// check that the key does not exist in the first collection
	val, err = db2.Get("testkey1")
	if err != nil {
		t.Errorf("Get() = %v, want %v", err, "nil")
	}

	// close the connection
	err = db2.Close()
	if err != nil {
		t.Errorf("Close() = %v, want %v", err, "nil")
	}

}

// Test Setting same key in two collections in the same database to different values and then getting the values back from the database
func TestSettingSameKeyInTwoCollectionsInSameDatabaseToDifferentValuesAndGetValuesBack(t *testing.T) {
	// open the first database
	db1 := NewBuntDb(WithFile("test_"+getTempFileName()), WithMode("memory"), WithCollection("testtable1"))

	// set some data in the first collection
	str := "testvalue1"

	// set the key/value
	err := db1.Set("testkey", str, 10*time.Second)
	if err != nil {
		t.Errorf("Set() = %v, want %v", err, "nil")
	}

	// close the connection
	err = db1.Close()
	if err != nil {
		t.Errorf("Close() = %v, want %v", err, "nil")
	}

	// init the second collection
	db2 := NewBuntDb(WithFile(db1.file), WithMode("memory"), WithCollection("testtable2"))

	// set some data in the second collection
	str = "testvalue2"

	// set the key/value
	err = db2.Set("testkey", str, 10*time.Second)
	if err != nil {
		t.Errorf("Set() = %v, want %v", err, "nil")
	}

	// close the connection
	err = db2.Close()
	if err != nil {

		t.Errorf("Close() = %v, want %v", err, "nil")
	}

	// open the first database again
	db1 = NewBuntDb(WithFile(db1.file), WithMode("memory"), WithCollection("testtable1"))

	// check that the key exists in the first collection
	val, err := db1.Get("testkey")
	if err != nil {
		t.Errorf("Get() = %v, want %v", err, "nil")
	}

	if val != "testvalue1" {
		t.Errorf("Get() = %v, want %v", val, "testvalue1")
	}

	// open the second collection again
	db1.Init(WithCollection("testtable2"))

	// check that the key exists in the second collection
	val, err = db1.Get("testkey")
	if err != nil {
		t.Errorf("Get() = %v, want %v", err, "nil")
	}

	if val != "testvalue2" {
		t.Errorf("Get() = %v, want %v", val, "testvalue2")
	}

}

// Test GetKeys
func TestGetKeys(t *testing.T) {
	// open the first database
	db1 := NewBuntDb(WithFile("test_"+getTempFileName()), WithMode("memory"), WithCollection("testtable1"))

	// set some data in the first collection
	str := "testvalue1"

	// set the key/value
	err := db1.Set("testkey1", str, 10*time.Second)
	if err != nil {
		t.Errorf("Set() = %v, want %v", err, "nil")
	}

	// set some data in the first collection
	str = "testvalue2"

	// set the key/value
	err = db1.Set("testkey2", str, 10*time.Second)
	if err != nil {
		t.Errorf("Set() = %v, want %v", err, "nil")
	}

	// set some data in the first collection
	str = "testvalue3"

	// set the key/value
	err = db1.Set("testkey3", str, 10*time.Second)
	if err != nil {
		t.Errorf("Set() = %v, want %v", err, "nil")
	}

	keys, err := db1.GetKeys()
	if err != nil {
		t.Errorf("GetKeys() = %v, want %v", err, "nil")
	}

	if len(keys) != 3 {
		t.Errorf("GetKeys() = %v, want %v", len(keys), 3)
	}

	t.Log(keys)

	// close the connection
	err = db1.Close()
	if err != nil {
		t.Errorf("Close() = %v, want %v", err, "nil")
	}

}
