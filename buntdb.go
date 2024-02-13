package swmemdb

import (
	"time"

	bunt "github.com/tidwall/buntdb"
)

// DB is a database handle representing a pool of zero or more underlying
// connections. It's safe for concurrent use by multiple goroutines.
type DB struct {
	db         *bunt.DB
	file       string
	collection string
	mode       string
}

// buntDbOptions provides options for configuring a BuntDb.
type buntDbOptions struct {
	file                 string
	collection           string
	mode                 string
	SyncPolicy           bunt.SyncPolicy
	AutoShrinkDisabled   bool
	AutoShrinkPercentage int
	AutoShrinkMinSize    int
	OnExpired            func(keys []string)
	OnExpiredSync        func(key, value string, tx *bunt.Tx) error
}

// defaultBuntDbOptions provides default options for configuring a BuntDb.
func defaultBuntDbOptions() buntDbOptions {
	return buntDbOptions{
		file:       "data.db",
		collection: "data",
		mode:       "file",
		SyncPolicy: bunt.EverySecond,
	}
}

// NewBuntDb creates a new BuntDb.
func NewBuntDb(options ...BuntDbOptionsFn) *DB {
	// default options
	opts := defaultBuntDbOptions()

	// apply user options
	for _, option := range options {
		option(&opts)
	}

	// create the database handle
	db := &DB{
		file:       opts.file, // getTempFileName(),
		collection: opts.collection,
		mode:       opts.mode,
	}

	// options to bunt
	buntOptions := &bunt.Config{
		SyncPolicy:           opts.SyncPolicy,
		AutoShrinkDisabled:   opts.AutoShrinkDisabled,
		AutoShrinkPercentage: opts.AutoShrinkPercentage,
		AutoShrinkMinSize:    opts.AutoShrinkMinSize,
		OnExpired:            opts.OnExpired,
		OnExpiredSync:        opts.OnExpiredSync,
	}

	// set persistence mode
	if db.mode == "memory" {
		// in memory
		db.file = ":memory:"
	}

	// Open the data.db file. It will be created if it doesn't exist.
	db.db = mustReturn(bunt.Open(db.file)).(*bunt.DB)
	// read config
	must(db.db.ReadConfig(buntOptions))
	// set config
	must(db.db.SetConfig(*buntOptions))

	return db
}

//

// BuntDbOptionsFn is a function that configures a BuntDb.
type BuntDbOptionsFn func(*buntDbOptions)

// WithFile sets the file name.
func WithFile(file string) BuntDbOptionsFn {
	return func(o *buntDbOptions) {
		o.file = file
	}
}

// WithCollection sets the collection name.
func WithCollection(collection string) BuntDbOptionsFn {
	return func(o *buntDbOptions) {
		o.collection = collection
	}
}

// WithMode sets the mode.
func WithMode(mode string) BuntDbOptionsFn {
	return func(o *buntDbOptions) {
		o.mode = mode
	}
}

// WithSyncPolicy sets the sync policy.
func WithSyncPolicy(syncPolicy bunt.SyncPolicy) BuntDbOptionsFn {
	return func(o *buntDbOptions) {
		o.SyncPolicy = syncPolicy
	}
}

// WithAutoShrinkDisabled sets the auto shrink disabled flag.
func WithAutoShrinkDisabled(autoShrinkDisabled bool) BuntDbOptionsFn {
	return func(o *buntDbOptions) {
		o.AutoShrinkDisabled = autoShrinkDisabled
	}
}

// WithAutoShrinkPercentage sets the auto shrink percentage.
func WithAutoShrinkPercentage(autoShrinkPercentage int) BuntDbOptionsFn {
	return func(o *buntDbOptions) {
		o.AutoShrinkPercentage = autoShrinkPercentage
	}
}

// WithAutoShrinkMinSize sets the auto shrink min size.
func WithAutoShrinkMinSize(autoShrinkMinSize int) BuntDbOptionsFn {
	return func(o *buntDbOptions) {
		o.AutoShrinkMinSize = autoShrinkMinSize
	}
}

// WithOnExpired sets the on expired callback.
func WithOnExpired(onExpired func(keys []string)) BuntDbOptionsFn {
	return func(o *buntDbOptions) {
		o.OnExpired = onExpired
	}
}

// WithOnExpiredSync sets the on expired sync callback.
func WithOnExpiredSync(onExpiredSync func(key, value string, tx *bunt.Tx) error) BuntDbOptionsFn {
	return func(o *buntDbOptions) {
		o.OnExpiredSync = onExpiredSync
	}
}

// Init initializes the database.
func (db *DB) Init(options ...BuntDbOptionsFn) error {

	// buntdb config
	config := buntDbOptions{}

	// apply user options
	for _, option := range options {
		option(&config)
	}

	// options to bunt
	buntOptions := &bunt.Config{
		SyncPolicy:           config.SyncPolicy,
		AutoShrinkDisabled:   config.AutoShrinkDisabled,
		AutoShrinkPercentage: config.AutoShrinkPercentage,
		AutoShrinkMinSize:    config.AutoShrinkMinSize,
		OnExpired:            config.OnExpired,
		OnExpiredSync:        config.OnExpiredSync,
	}

	// if set to config, use it
	if config.AutoShrinkPercentage != 0 {
		buntOptions.AutoShrinkPercentage = config.AutoShrinkPercentage
	} else {
		buntOptions.AutoShrinkPercentage = 100
	}

	// if set to config, use it
	if config.AutoShrinkMinSize != 0 {
		buntOptions.AutoShrinkMinSize = config.AutoShrinkMinSize
	} else {
		buntOptions.AutoShrinkMinSize = 32
	}

	// if set to config, use it
	if config.OnExpired != nil {
		buntOptions.OnExpired = config.OnExpired
	}

	// if set to config, use it
	if config.OnExpiredSync != nil {
		buntOptions.OnExpiredSync = config.OnExpiredSync
	}

	// if collection is set, use it
	if config.collection != "" {
		db.collection = config.collection
	}

	// read config
	must(db.db.ReadConfig(buntOptions))

	// set config
	must(db.db.SetConfig(*buntOptions))

	// open the database
	db.db = mustReturn(bunt.Open(db.file)).(*bunt.DB)

	return nil
}

// Close closes the database.
func (db *DB) Close() error {

	// close the database
	return db.db.Close()
}

// Set sets the value for a key.
func (db *DB) Set(key string, value string, exp time.Duration) error {

	return db.db.Update(func(tx *bunt.Tx) error {

		// set the key/value
		_, _, err := tx.Set(db.collection+":"+key, value, &bunt.SetOptions{Expires: true, TTL: exp})
		if err != nil {
			return err
		}

		return nil
	})

}

// SetWithNoExpiration sets the value for a key with no expiration.
func (db *DB) SetWithNoExpiration(key string, value string) error {

	return db.db.Update(func(tx *bunt.Tx) error {

		// set the key/value
		_, _, err := tx.Set(db.collection+":"+key, value, &bunt.SetOptions{Expires: false})
		if err != nil {
			return err
		}

		return nil
	})

}

// Get gets the value for a key.
func (db *DB) Get(key string) (interface{}, error) {
	var value interface{}
	err := db.db.View(func(tx *bunt.Tx) error {
		val, err := tx.Get(db.collection + ":" + key)
		if err != nil {
			if err.Error() == bunt.ErrNotFound.Error() {

				value = ""
				return err
			}
		}

		value = val

		return nil
	})

	return value, err
}

// GetFromCollection gets the value for a key from a collection.
func (db *DB) GetFromCollection(collection string, key string) (interface{}, error) {
	var value interface{}
	err := db.db.View(func(tx *bunt.Tx) error {
		val, err := tx.Get(collection + ":" + key)
		if err != nil {
			if err.Error() == bunt.ErrNotFound.Error() {

				value = ""
				return err
			}
		}

		value = val

		return nil
	})

	return value, err
}

// DeleteFromCollection deletes a key/value pair from a collection.
func (db *DB) DeleteFromCollection(collection string, key string) error {
	return db.db.Update(func(tx *bunt.Tx) error {
		_, err := tx.Delete(collection + ":" + key)
		if err != nil {
			if err.Error() == bunt.ErrNotFound.Error() {
				return err
			} else {
				return nil
			}
		}

		return nil

	})
}

// Delete deletes a key/value pair.
func (db *DB) Delete(key string) error {
	return db.db.Update(func(tx *bunt.Tx) error {
		_, err := tx.Delete(db.collection + ":" + key)
		if err != nil {
			if err.Error() == bunt.ErrNotFound.Error() {
				return err
			} else {
				return nil
			}
		}

		return nil

	})
}

// var delkeys []string
// tx.AscendKeys("object:*", func(k, v string) bool {
// 	if someCondition(k) == true {
// 		delkeys = append(delkeys, k)
// 	}
// 	return true // continue
// })
// for _, k := range delkeys {
// 	if _, err = tx.Delete(k); err != nil {
// 		return err
// 	}
// }

// DeleteWhere deletes all key/value pairs that match the condition.
func (db *DB) DeleteWhere(condition func(key string, value string) bool) error {
	// return db.db.Update(func(tx *bunt.Tx) error {

	// 	var delkeys []string
	// 	tx.AscendKeys("*", func(k, v string) bool {
	// 		if condition(k, v) {
	// 			delkeys = append(delkeys, k)
	// 		}
	// 		return true // continue
	// 	})

	// 	for _, k := range delkeys {
	// 		if _, err := tx.Delete(k); err != nil {
	// 			return err
	// 		}
	// 	}

	// 	return nil

	// })
	return db.db.Update(func(tx *bunt.Tx) error {

		var delkeys []string
		tx.AscendKeys(db.collection+":*", func(k, v string) bool {
			if condition(k, v) {
				delkeys = append(delkeys, k)
			}
			return true // continue
		})

		for _, k := range delkeys {
			if _, err := tx.Delete(k); err != nil {
				return err
			}
		}

		return nil

	})
}

// GetKeys returns all keys from the database collection.
func (db *DB) GetKeys() ([]string, error) {

	var keys []string

	err := db.db.View(func(tx *bunt.Tx) error {

		// get all keys
		err := tx.AscendKeys(db.collection+":*", func(key, value string) bool {
			// strip the collection name
			key = key[len(db.collection)+1:]
			// append the key
			keys = append(keys, key)
			return true
		})

		return err
	})

	return keys, err
}

// must is a helper that wraps a call returning (_, error) and panics if the
// error is non-nil.
func must(err error) {
	if err != nil {
		panic(err)
	}
}

// mustReturn is a helper that wraps a call returning (interface{}, error) and panics if the
// error is non-nil.
func mustReturn(value interface{}, err error) interface{} {

	if err != nil {
		panic(err)
	}

	return value
}

// onError run the callback if the error is not nil.
func onError(err error, callback func(err error)) {
	// if err is not nil, run the callback
	if err != nil {
		// run the callback
		callback(err)
	}
}

var TempFileName string

// getTempFileName returns a temporary file name. [unix timestamp].db
func getTempFileName(n ...string) string {
	var label string
	if len(n) > 0 {
		label = n[0]
	} else {
		label = ""
	}
	// if TempFileName != "" && n == false {
	// 	return TempFileName
	// } else {
	// 	TempFileName = time.Now().Format("test_20060102150405") + ".db"
	// 	return TempFileName
	// }
	return time.Now().Format(label+"20060102150405") + ".db"
}
