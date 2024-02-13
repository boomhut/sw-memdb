package swmemdb

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/tidwall/buntdb"
)

var (
	tempfile1, tempfile2, tempfile3, tempfile4, tempfile5, tempfile6, tempfile7, tempfile8, tempfile9, tempfile10, tempfile11, tempfile12, tempfile13, tempfile14, tempfile15, tempfile16, tempfile17, tempfile18, tempfile19, tempfile20, tempfile21, tempfile22 string
)

// Test NewConnection
func TestNewConnection(t *testing.T) {
	tempfile1 = getTempFileName("TestNewConnection")
	db := NewBuntDb(WithFile("test_"+tempfile1), WithMode("memory"), WithCollection("testtable"))
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
	tempfile2 = getTempFileName("TestInit")
	db := NewBuntDb(WithFile("test_"+tempfile2), WithMode("memory"), WithCollection("testtable"))
	err := db.Init()
	if err != nil {
		t.Errorf("Init() = %v, want %v", err, "nil")
	}

	// set some data in the first collection
	str := "testvalue1"

	// set the key/value
	err = db.Set("testkey1", str, 10*time.Second)
	if err != nil {
		t.Errorf("Set() = %v, want %v", err, "nil")
	}

	// close the connection
	err = db.db.Close()
	if err != nil {
		t.Errorf("Close() = %v, want %v", err, "nil")
	}
}

// Test Set
func TestSet(t *testing.T) {
	tempfile3 = getTempFileName("TestSet")
	db := NewBuntDb(WithFile("test_"+tempfile3), WithMode("memory"), WithCollection("testtable"))
	err := db.Init()
	if err != nil {
		t.Errorf("Init() = %v, want %v", err, "nil")
	}

	err = db.Set("testkey", "testvalue", 5*time.Second)
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

}

// Test Get
func TestGet(t *testing.T) {
	tempfile4 = getTempFileName("TestGet")
	db := NewBuntDb(WithFile("test_"+tempfile4), WithMode("memory"), WithCollection("test332"), WithSyncPolicy(buntdb.Always))
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
	tempfile5 = getTempFileName("TestGetWithNonExistingKey")
	db := NewBuntDb(WithFile("test_"+tempfile5), WithMode("memory"), WithCollection("testtable"))
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
	tempfile6 = getTempFileName("TestClose")
	db := NewBuntDb(WithFile("test_"+tempfile6), WithMode("memory"), WithCollection("testtable"))
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
	tempfile7 = getTempFileName("TestDelete")
	db := NewBuntDb(WithFile("test_"+tempfile7), WithMode("memory"), WithCollection("testtable"))
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

	// check that the key does not exist, err should be ErrNotFound (not nil) and val should be ""
	val, err = db.Get("testkey")
	// check if err is not nil
	if err != nil {
		// err should be ErrNotFound
		if err.Error() != buntdb.ErrNotFound.Error() {
			t.Errorf("Get() = %v, want %v", val, "ErrNotFound")
		}
	}

	// delete the key again, err should be ErrNotFound (not nil)
	err = db.Delete("testkey")
	if err != nil {
		if err.Error() != buntdb.ErrNotFound.Error() {
			t.Errorf("Delete() = %v, want %v", err.Error(), "ErrNotFound")
		}
	}

	// close the connection
	err = db.Close()
	if err != nil {
		t.Errorf("Close() = %v, want %v", err, "nil")
	}

}

// Test Delete with non existing key
func TestDeleteWithNonExistingKey(t *testing.T) {
	tempfile8 = getTempFileName("TestDeleteWithNonExistingKey")
	db := NewBuntDb(WithFile("test_"+tempfile8), WithMode("memory"), WithCollection("testtable"))
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

	// close the connection
	err = db.Close()
	if err != nil {
		t.Errorf("Close() = %v, want %v", err, "nil")
	}

}

// Test Set with expiration
func TestSetWithExpiration(t *testing.T) {
	tempfile9 = getTempFileName("TestSetWithExpiration")
	db := NewBuntDb(WithFile("test_"+tempfile9), WithMode("memory"), WithCollection("testtable"))
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
	tempfile10 = getTempFileName("TestCreateTwoCollectionsInSameDatabase")
	db := NewBuntDb(WithFile("test_"+tempfile10), WithMode("memory"), WithCollection("testtable1"))
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
	tempfile11 = getTempFileName("TestCreateTwoCollectionsInSameDatabaseAndSetDataInBoth")
	db := NewBuntDb(WithFile("test_"+tempfile11), WithMode("file"), WithCollection("testtable1"))

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

	db = NewBuntDb(WithFile(db.file), WithMode("file"), WithCollection("testtable2"))
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
	db = NewBuntDb(WithFile(db.file), WithMode("file"), WithCollection("testtable1"))

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
	db = NewBuntDb(WithFile(db.file), WithMode("file"), WithCollection("testtable2"))

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
	tempfile12 = getTempFileName("TestSetWithNoExpiration")
	db1 := NewBuntDb(WithFile("test_"+tempfile12), WithMode("file"), WithCollection("testtable1"))
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

	db2 := NewBuntDb(WithFile(db1.file), WithMode("file"), WithCollection("testtable2"))
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
	db1 = NewBuntDb(WithFile(db1.file), WithMode("file"), WithCollection("testtable1"))
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
	if err == nil {
		t.Errorf("Get() = %v, want %v", val, "nil")
	}

	// close the connection
	err = db1.Close()
	if err != nil {
		t.Errorf("Close() = %v, want %v", err, "nil")
	}

	// open the second database again
	db2 = NewBuntDb(WithFile(db1.file), WithMode("file"), WithCollection("testtable2"))
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
	val, _ = db2.Get("testkey1")
	if val != "" {
		t.Errorf("Get() = %v, want %v", val, "nil")
	}

	// close the connection
	err = db2.Close()
	if err != nil {
		t.Errorf("Close() = %v, want %v", err, "nil")
	}

}

// Test Setting same key in two collections in the same database to different values and then getting the values back from the database
func TestSettingSameKeyInTwoCollectionsInSameDatabaseToDifferentValuesAndGetValuesBack(t *testing.T) {
	tempfile13 = getTempFileName("TestSettingSameKeyInTwoCollectionsInSameDatabaseToDifferentValuesAndGetValuesBack")
	// open the first database
	db1 := NewBuntDb(WithFile("test_"+tempfile13), WithMode("file"), WithCollection("testtable1"))

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
	db2 := NewBuntDb(WithFile(db1.file), WithMode("file"), WithCollection("testtable2"))

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
	db1 = NewBuntDb(WithFile(db1.file), WithMode("file"), WithCollection("testtable1"))

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

	// close the connection
	err = db1.Close()
	if err != nil {
		t.Errorf("Close() = %v, want %v", err, "nil")
	}

}

// Test GetKeys
func TestGetKeys(t *testing.T) {
	tempfile14 = getTempFileName("TestGetKeys")
	// open the first database
	db1 := NewBuntDb(WithFile("test_"+tempfile14), WithMode("file"), WithCollection("testtable5"))

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

// Test GetKeys with no keys
func TestGetKeysWithNoKeys(t *testing.T) {
	tempfile15 = getTempFileName("TestGetKeysWithNoKeys")
	// open the first database
	db1 := NewBuntDb(WithFile("test_a"+tempfile15), WithMode("file"), WithCollection("testtable5"))

	keys, err := db1.GetKeys()
	if err != nil {
		t.Errorf("GetKeys() = %v, want %v", err, "nil")
	}

	if len(keys) != 0 {
		t.Errorf("GetKeys() = %v, want %v", len(keys), 0)
	}

	// close the connection
	err = db1.Close()
	if err != nil {
		t.Errorf("Close() = %v, want %v", err, "nil")
	}

}

// Test DeleteWhere
func TestDeleteWhere(t *testing.T) {
	tempfile16 = getTempFileName("TestDeleteWhere")
	// open the first database
	db1 := NewBuntDb(WithFile("test_"+tempfile16), WithMode("file"), WithCollection("testtable5"))

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

	// delete the keys
	err = db1.DeleteWhere(func(key, value string) bool {
		return value == "testvalue1" || value == "testvalue2"
	})
	if err != nil {
		t.Errorf("DeleteWhere() = %v, want %v", err, "nil")
	}

	keys, err := db1.GetKeys()
	if err != nil {
		t.Errorf("GetKeys() = %v, want %v", err, "nil")
	}

	if len(keys) != 1 {
		t.Errorf("GetKeys() = %v, want %v", len(keys), 1)
	}

	// close the connection
	err = db1.Close()
	if err != nil {
		t.Errorf("Close() = %v, want %v", err, "nil")
	}

}

// Test GetKeys
func TestGetKeysAfterDeleteWhere(t *testing.T) {
	tempfile17 = getTempFileName("TestGetKeysAfterDeleteWhere")
	// open the first database
	db1 := NewBuntDb(WithFile("test_"+tempfile17), WithMode("file"), WithCollection("testtable5"))

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

	// delete the keys
	err = db1.DeleteWhere(func(key, value string) bool {
		return value == "testvalue1" || value == "testvalue2"
	})
	if err != nil {
		t.Errorf("DeleteWhere() = %v, want %v", err, "nil")
	}

	keys, err := db1.GetKeys()
	if err != nil {
		t.Errorf("GetKeys() = %v, want %v", err, "nil")
	}

	if len(keys) != 1 {
		t.Errorf("GetKeys() = %v, want %v", len(keys), 1)
	}

	// close the connection
	err = db1.Close()
	if err != nil {
		t.Errorf("Close() = %v, want %v", err, "nil")
	}

}

// Test if collections work with the same file
func TestCollectionsWithSameFile(t *testing.T) {
	tempfile18 = getTempFileName("TestCollectionsWithSameFile")
	db1 := NewBuntDb(WithFile("test_"+tempfile18), WithMode("file"), WithCollection("testtable1"))
	err := db1.Init()
	if err != nil {
		t.Errorf("Init() = %v, want %v", err, "nil")
	}

	// set some data in the first collection
	str := "testvalue1"

	// set the key/value
	err = db1.Set("testkey1", str, 10*time.Second)
	if err != nil {
		t.Errorf("Set() = %v, want %v", err, "nil")
	}

	// close the connection
	err = db1.Close()
	if err != nil {
		t.Errorf("Close() = %v, want %v", err, "nil")
	}

	db2 := NewBuntDb(WithFile(db1.file), WithMode("file"), WithCollection("testtable2"))
	err = db2.Init()
	if err != nil {
		t.Errorf("Init() = %v, want %v", err, "nil")
	}

	// set some data in the second collection
	str = "testvalue2"

	// set the key/value
	err = db2.Set("testkey2", str, 10*time.Second)
	if err != nil {
		t.Errorf("Set() = %v, want %v", err, "nil")
	}

	// close the connection
	err = db2.Close()
	if err != nil {

		t.Errorf("Close() = %v, want %v", err, "nil")
	}

	// open the first database again
	db1 = NewBuntDb(WithFile(db1.file), WithMode("file"), WithCollection("testtable1"))
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

	// open the second collection again
	db1.Init(WithCollection("testtable2"))

	// check that the key exists in the second collection
	val, err = db1.Get("testkey2")
	if err != nil {
		t.Errorf("Get() = %v, want %v", err, "nil")
	}

	if val != "testvalue2" {
		t.Errorf("Get() = %v, want %v", val, "testvalue2")
	}

	// close the connection
	err = db1.Close()
	if err != nil {
		t.Errorf("Close() = %v, want %v", err, "nil")
	}

}

// Test if collections work with the same file and setting data in both collections
func TestCollectionsWithSameFileAndSetDataInBoth(t *testing.T) {
	tempfile19 = getTempFileName("TestCollectionsWithSameFileAndSetDataInBoth")
	db1 := NewBuntDb(WithFile("test_"+tempfile19), WithMode("file"), WithCollection("domains"))
	err := db1.Init()
	if err != nil {
		t.Errorf("Init() = %v, want %v", err, "nil")
	}

	// set some data in the first collection
	str := "example.com"
	err = db1.Set("example.com", "testvalue1", 10*time.Second)
	if err != nil {
		t.Errorf("Set() = %v, want %v", err, "nil")
	}

	// close the connection
	err = db1.Close()
	if err != nil {
		t.Errorf("Close() = %v, want %v", err, "nil")
	}

	db2 := NewBuntDb(WithFile(db1.file), WithMode("file"), WithCollection("users"))
	err = db2.Init()
	if err != nil {
		t.Errorf("Init() = %v, want %v", err, "nil")
	}

	// set some data in the second collection
	str = "testvalue2"
	err = db2.Set("testkey2", str, 10*time.Second)
	if err != nil {
		t.Errorf("Set() = %v, want %v", err, "nil")
	}

	// verify that the key exists in the second collection
	val, err := db2.Get("testkey2")
	if err != nil {
		t.Errorf("Get() = %v, want %v", err, "nil")
	}

	if val != "testvalue2" {
		t.Errorf("Get() = %v, want %v", val, "testvalue2")
	}

	// verify that the first key does not exist in the second collection
	_, err = db2.Get("example.com")
	if err == nil {
		t.Errorf("Get() = %v, want %v", err, "nil")
	}

	err = db2.Init(WithCollection("domains"), WithAutoShrinkDisabled(false), WithAutoShrinkMinSize(100), WithAutoShrinkPercentage(30), WithOnExpired(func(keys []string) { t.Log("expired") }), WithOnExpiredSync(func(key, value string, tx *buntdb.Tx) error { return nil }))
	if err != nil {
		t.Errorf("Init() = %v, want %v", err, "nil")
	}

	// verify that the first key does exist in the first collection

	val, err = db2.Get("example.com")
	if err != nil {
		t.Errorf("Get() = %v, want %v", err, "nil")
	}

	if val != "testvalue1" {
		t.Errorf("Get() = %v, want %v", val, "testvalue1")
	}

	// close the connection
	err = db2.Close()
	if err != nil {

		t.Errorf("Close() = %v, want %v", err, "nil")
	}

}

// TestGetFromCollection
func TestGetFromCollection(t *testing.T) {
	tempfile20 = getTempFileName("TestGetFromCollection")
	db := NewBuntDb(WithFile("test_"+tempfile20), WithMode("file"), WithCollection("testtable"))
	err := db.Init()
	if err != nil {
		t.Errorf("Init() = %v, want %v", err, "nil")
	}

	err = db.Set("testkey", "testvalue", 5*time.Second)
	if err != nil {
		t.Errorf("Set() = %v, want %v", err, "nil")
	}

	val, err := db.GetFromCollection("testtable", "testkey")
	if err != nil {
		t.Errorf("GetFromCollection() = %v, want %v", err, "nil")
	}

	if val != "testvalue" {
		t.Errorf("GetFromCollection() = %v, want %v", val, "testvalue")
	}

	// close the connection
	err = db.Close()
	if err != nil {
		t.Errorf("Close() = %v, want %v", err, "nil")
	}

}

// TestDeleteFromCollection
func TestDeleteFromCollection(t *testing.T) {
	tempfile21 = getTempFileName("TestDeleteFromCollection")
	db := NewBuntDb(WithFile("test_"+tempfile21), WithMode("file"), WithCollection("testtable"))
	err := db.Init()
	if err != nil {
		t.Errorf("Init() = %v, want %v", err, "nil")
	}

	err = db.Set("testkey", "testvalue", 0)
	if err != nil {
		t.Errorf("Set() = %v, want %v", err, "nil")
	}

	err = nil
	// delete the key
	err = db.DeleteFromCollection("testtable", "testkey")
	if err != nil {
		t.Errorf("DeleteFromCollection() = %v, want %v", err, "nil")
	}

	err = nil
	// check that the key does not exist, err should be ErrNotFound (not nil) and val should be ""
	val, err := db.GetFromCollection("testtable", "testkey")
	// check if err is not nil
	if err != nil {
		// err should be ErrNotFound
		if err.Error() != buntdb.ErrNotFound.Error() {
			t.Errorf("GetFromCollection() = %v, want %v", val, "ErrNotFound")
		}
	}

	if val != "" {
		t.Errorf("GetFromCollection() = %v, want %v", val, "")
	}

	// close the connection
	err = db.Close()
	if err != nil {
		t.Errorf("Close() = %v, want %v", err, "nil")
	}

}

// TestSetToCollection
func TestSetToCollection(t *testing.T) {
	tempfile22 = getTempFileName("TestSetToCollection")
	db := NewBuntDb(WithFile("test_"+tempfile22), WithMode("file"), WithCollection("testtable"))
	err := db.Init()
	if err != nil {
		t.Errorf("Init() = %v, want %v", err, "nil")
	}

	err = db.SetToCollection("testtable", "testkey", "testvalue", 5*time.Second)
	if err != nil {
		t.Errorf("SetToCollection() = %v, want %v", err, "nil")
	}

	val, err := db.GetFromCollection("testtable", "testkey")
	if err != nil {
		t.Errorf("GetFromCollection() = %v, want %v", err, "nil")
	}

	if val != "testvalue" {
		t.Errorf("GetFromCollection() = %v, want %v", val, "testvalue")
	}

	// get non existing key from the collection
	val, err = db.GetFromCollection("testtable", "testkeynotfound")
	if err == nil || val != "" {
		t.Errorf("GetFromCollection() = %v, want %v", err, "nil")
	}

	t.Log(val)

	// close the connection
	err = db.Close()
	if err != nil {
		t.Errorf("Close() = %v, want %v", err, "nil")
	}

}

// Test DeleteFromCollection with non existing key
func TestDeleteFromCollectionWithNonExistingKey(t *testing.T) {
	db := NewBuntDb(WithMode("memory"), WithCollection("testtable"))
	err := db.Init()
	if err != nil {
		t.Errorf("Init() = %v, want %v", err, "nil")
	}

	err = nil
	// delete the key
	err = db.DeleteFromCollection("testtable", "testkey")
	if err != nil && err.Error() != "not found" {
		t.Errorf("DeleteFromCollection() = %v, want %v", err.Error(), "not found")
	}

	// close the connection
	err = db.Close()
	if err != nil {
		t.Errorf("Close() = %v, want %v", err, "nil")
	}

}

// Test GetKeysFromCollection
func TestGetKeysFromCollection(t *testing.T) {
	db := NewBuntDb(WithMode("mem"), WithCollection("testtable"))
	err := db.Init()
	if err != nil {
		t.Errorf("Init() = %v, want %v", err, "nil")
	}

	err = db.SetToCollection("testtable", "testkey1", "testvalue1", 5*time.Second)
	if err != nil {
		t.Errorf("SetToCollection() = %v, want %v", err, "nil")
	}

	err = db.SetToCollection("testtable", "testkey2", "testvalue2", 5*time.Second)
	if err != nil {
		t.Errorf("SetToCollection() = %v, want %v", err, "nil")
	}

	keys, err := db.GetKeysFromCollection("testtable")
	if err != nil {
		t.Errorf("GetKeysFromCollection() = %v, want %v", err, "nil")
	}

	if len(keys) != 2 {
		t.Errorf("GetKeysFromCollection() = %v, want %v", len(keys), 2)
	}

	t.Log(keys)

	// close the connection
	err = db.Close()
	if err != nil {
		t.Errorf("Close() = %v, want %v", err, "nil")
	}

}

// getTempFileName returns a temporary file name. [unix timestamp].db
func getTempFileName(n ...string) string {
	var label string
	if len(n) > 0 {
		label = n[0]
	} else {
		label = ""
	}

	return time.Now().Format(label+"20060102150405") + ".db"
}

// Clean up
func TestCleanup(t *testing.T) {
	// delete the database files
	os.Remove("./test_" + tempfile1)
	os.Remove("./test_" + tempfile2)
	os.Remove("./test_" + tempfile3)
	os.Remove("./test_" + tempfile4)
	os.Remove("./test_" + tempfile5)
	os.Remove("./test_" + tempfile6)
	os.Remove("./test_" + tempfile7)
	os.Remove("./test_" + tempfile8)
	os.Remove("./test_" + tempfile9)
	os.Remove("./test_" + tempfile10)
	os.Remove("./test_" + tempfile11)
	os.Remove("./test_" + tempfile12)
	os.Remove("./test_" + tempfile13)
	os.Remove("./test_" + tempfile14)
	os.Remove("./test_a" + tempfile15)
	os.Remove("./test_" + tempfile16)
	os.Remove("./test_" + tempfile17)
	os.Remove("./test_" + tempfile18)
	os.Remove("./test_" + tempfile19)
	os.Remove("./test_" + tempfile20)
	os.Remove("./test_" + tempfile21)
	os.Remove("./test_" + tempfile22)

	// verify that the files are deleted

}

// // mustReturn is a helper that wraps a call returning (interface{}, error) and panics if the
// // error is non-nil.
// func mustReturn(value interface{}, err error) interface{} {

// 	if err != nil {
// 		panic(err)
// 	}

// 	return value
// }

// // onError run the callback if the error is not nil.
// func onError(err error, callback func(err error)) {
// 	// if err is not nil, run the callback
// 	if err != nil {
// 		// run the callback
// 		callback(err)
// 	}
// }

// Test must function
func TestMust(t *testing.T) {

	// test Must function
	// test that it panics
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic")
		}
	}()

	// call must panic on purpose
	errFunc := func() error {
		return errors.New("test error")
	}
	must(errFunc())

}

// Test mustReturn function
func TestMustReturn(t *testing.T) {

	// test Must function
	// test that it panics
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic")
		}
	}()

	// call mustReturn panic on purpose
	errFunc := func() error {
		return errors.New("test error")
	}
	mustReturn(nil, errFunc())

}

// Test mustReturn function with nil error and non nil value should return the value
func TestMustReturnWithNilError(t *testing.T) {

	// call mustReturn with nil error and non nil value
	errFunc := func() error {
		return nil
	}
	val := mustReturn("test", errFunc())
	if val != "test" {
		t.Errorf("mustReturn() = %v, want %v", val, "test")
	}

}

// Test onError function
func TestOnError(t *testing.T) {

	// test onError function
	// test that it runs the callback
	errFunc := func() error {
		return errors.New("test error")
	}
	onError(errFunc(), func(err error) {
		if err == nil {
			t.Errorf("onError() = %v, want %v", err, "nil")
		}
	})

}
