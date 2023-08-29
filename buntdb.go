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
		mode:       "memory",
		SyncPolicy: bunt.EverySecond,
	}
}

// NewBuntDb creates a new BuntDb.
func NewBuntDb(options ...BuntDbOptionsFn) *DB {
	// create the database handle
	db := &DB{
		db:         nil,
		file:       getTempFileName(),
		collection: "",
		mode:       "",
	}
	// default options
	opts := defaultBuntDbOptions()

	// apply user options
	for _, option := range options {
		option(&opts)
	}

	// Open the data.db file. It will be created if it doesn't exist.
	db.db = mustReturn(bunt.Open(db.file)).(*bunt.DB)

	// initialize the database
	must(db.Init(options...))

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

// TODO: Implement WithCollection?
// WithCollection sets the collection name. NOT IMPLEMENTED YET.
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
	config := defaultBuntDbOptions()

	// apply user options
	for _, option := range options {
		option(&config)
	}

	// options to bunt
	buntOptions := &bunt.Config{
		SyncPolicy:         config.SyncPolicy,
		AutoShrinkDisabled: config.AutoShrinkDisabled,
		// AutoShrinkPercentage: config.AutoShrinkPercentage,
		// AutoShrinkMinSize:    config.AutoShrinkMinSize,
		// OnExpired:            config.OnExpired,
		// OnExpiredSync:        config.OnExpiredSync,
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
	return db.db.Close()
}

// Set sets the value for a key.
func (db *DB) Set(key string, value string, exp time.Duration) error {

	return db.db.Update(func(tx *bunt.Tx) error {

		// set the key/value
		_, _, err := tx.Set(key, value, &bunt.SetOptions{Expires: true, TTL: exp})
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
		val, err := tx.Get(key)
		if err != nil {
			if err == bunt.ErrNotFound {
				val = ""
				return err
			}
		}

		value = val

		return nil
	})

	return value, err
}

// Delete deletes a key/value pair.
func (db *DB) Delete(key string) error {
	return db.db.Update(func(tx *bunt.Tx) error {
		_, err := tx.Delete(key)
		return err
	})
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

// getTempFileName returns a temporary file name. [unix timestamp].db
func getTempFileName() string {
	return time.Now().Format("20060102150405") + ".db"
}
