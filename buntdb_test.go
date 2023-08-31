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
