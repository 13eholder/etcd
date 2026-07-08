// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package bitcask implements a small append-only log-structured key/value
// engine (in the style of the Bitcask paper / Riak's bitcask backend),
// purpose-built for etcd's Event store:
//
//   - Values live on disk in sequentially-written, immutable-once-sealed
//     data files; only a small index (key -> file/offset/size) is kept in
//     memory, unlike a pure in-memory store.
//   - The data directory is wiped on every Open: this engine never recovers
//     its keydir from the log after a restart, so there is no hint-file /
//     log-replay machinery. Callers that need durability across restarts
//     should not use this package.
//   - Writes are never fsynced; they only need to reach the OS page cache.
//     Losing unflushed writes on a crash is acceptable given the no-recovery
//     design above.
//   - The in-memory index is a B-tree (not a hash map) so Range/DeleteRange
//     can do ordered prefix/interval scans directly against the index.
package bitcask

import (
	"os"
	"sync"
	"time"
)

const (
	defaultMaxFileSize   = 64 << 20 // 64MiB
	defaultMergeInterval = 10 * time.Minute
)

// Options configures a DB. Zero values fall back to the defaults documented
// on each field.
type Options struct {
	// MaxFileSize is the size threshold at which the active data file is
	// sealed and a new one is rolled. Default: 64MiB.
	MaxFileSize int64
	// MergeInterval is how often the background goroutine purges expired
	// keys and compacts sealed data files. Default: 10 minutes.
	MergeInterval time.Duration
	// TTL is a single store-wide expiry applied to every key, measured from
	// each record's write time (its on-disk tstamp), independent of any
	// per-key deadline. This mirrors Riak bitcask's bucket-level expiry
	// rather than a per-record absolute deadline. Zero (the default) means
	// keys never expire.
	TTL time.Duration
}

func (o Options) withDefaults() Options {
	if o.MaxFileSize <= 0 {
		o.MaxFileSize = defaultMaxFileSize
	}
	if o.MergeInterval <= 0 {
		o.MergeInterval = defaultMergeInterval
	}
	return o
}

// Entry is a single key/value pair returned by Range or DeleteRange.
type Entry struct {
	Key   []byte
	Value []byte
}

// DB is a single Bitcask-style key/value store rooted at one directory.
type DB struct {
	dir string
	opt Options

	kd *keydir

	// writeMu serializes appends to the active file (Bitcask's classic
	// single-writer model) and protects activeFile/activeID/activeSize.
	writeMu    sync.Mutex
	activeFile *os.File
	activeID   uint32
	activeSize int64
	nextFileID uint32

	// filesMu guards the read-handle cache used by Get/Range/merge.
	filesMu sync.RWMutex
	files   map[uint32]*os.File

	stopc chan struct{}
	wg    sync.WaitGroup
}

// Open (re)creates a fresh Bitcask store at dir: any existing contents are
// removed since this engine never recovers state from a previous run.
func Open(dir string, opt Options) (*DB, error) {
	opt = opt.withDefaults()

	if err := os.RemoveAll(dir); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}

	db := &DB{
		dir:   dir,
		opt:   opt,
		kd:    newKeydir(),
		files: make(map[uint32]*os.File),
		stopc: make(chan struct{}),
	}
	if err := db.rollActiveFileLocked(1); err != nil {
		return nil, err
	}

	db.wg.Add(1)
	go db.backgroundLoop()
	return db, nil
}

// rollActiveFileLocked seals the current active file (if any) and opens a
// new one as id. Callers must hold writeMu.
func (db *DB) rollActiveFileLocked(id uint32) error {
	f, err := os.OpenFile(dataFileName(db.dir, id), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	db.activeFile = f
	db.activeID = id
	db.activeSize = 0
	if id > db.nextFileID {
		db.nextFileID = id
	}

	rf, err := os.Open(dataFileName(db.dir, id))
	if err != nil {
		f.Close()
		return err
	}
	db.filesMu.Lock()
	db.files[id] = rf
	db.filesMu.Unlock()
	return nil
}

func (db *DB) allocFileID() uint32 {
	db.nextFileID++
	return db.nextFileID
}

// Put writes key/value, overwriting any previous value for key. The record
// is timestamped with the current write time, which is what DB.opt.TTL (if
// set) measures expiry from.
func (db *DB) Put(key, value []byte) error {
	tstamp := time.Now().UnixNano()
	rec, valueOff := encodeRecord(tstamp, key, value)

	db.writeMu.Lock()
	if db.activeSize+int64(len(rec)) > db.opt.MaxFileSize {
		if err := db.rollActiveFileLocked(db.allocFileID()); err != nil {
			db.writeMu.Unlock()
			return err
		}
	}
	pos := db.activeSize
	n, err := db.activeFile.Write(rec)
	if err != nil {
		db.writeMu.Unlock()
		return err
	}
	fileID := db.activeID
	db.activeSize += int64(n)
	db.writeMu.Unlock()

	db.kd.set(&keydirEntry{
		key:       string(key),
		fileID:    fileID,
		valuePos:  pos + int64(valueOff),
		valueSize: uint32(len(value)),
		tstamp:    tstamp,
	})
	return nil
}

// Get returns the current value for key, if present and not expired.
func (db *DB) Get(key []byte) (value []byte, ok bool) {
	e, found := db.kd.get(string(key))
	if !found {
		return nil, false
	}
	if db.expired(e) {
		db.kd.delete(e.key)
		return nil, false
	}
	val, err := db.readValue(e)
	if err != nil {
		return nil, false
	}
	return val, true
}

func (db *DB) expired(e *keydirEntry) bool {
	return db.opt.TTL > 0 && time.Now().UnixNano()-e.tstamp >= int64(db.opt.TTL)
}

func (db *DB) readValue(e *keydirEntry) ([]byte, error) {
	f, err := db.getReadFile(e.fileID)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, e.valueSize)
	if e.valueSize > 0 {
		if _, err := f.ReadAt(buf, e.valuePos); err != nil {
			return nil, err
		}
	}
	return buf, nil
}

func (db *DB) getReadFile(fileID uint32) (*os.File, error) {
	db.filesMu.RLock()
	f, ok := db.files[fileID]
	db.filesMu.RUnlock()
	if ok {
		return f, nil
	}

	db.filesMu.Lock()
	defer db.filesMu.Unlock()
	if f, ok := db.files[fileID]; ok {
		return f, nil
	}
	f, err := os.Open(dataFileName(db.dir, fileID))
	if err != nil {
		return nil, err
	}
	db.files[fileID] = f
	return f, nil
}

// Delete removes a single key.
func (db *DB) Delete(key []byte) {
	db.kd.delete(string(key))
}

// Range returns entries with keys in [startKey, endKey), honoring limit (<=0
// means unlimited), following the etcd RangeEnd convention:
//   - endKey == nil:            only startKey itself
//   - endKey != nil, len == 0:  startKey to the end of the keyspace
//   - otherwise:                [startKey, endKey)
//
// Expired entries encountered during the scan are dropped lazily.
func (db *DB) Range(startKey, endKey []byte, limit int) []Entry {
	var out []Entry
	var expiredKeys []string

	db.kd.ascend(string(startKey), endKey, func(e *keydirEntry) bool {
		if db.expired(e) {
			expiredKeys = append(expiredKeys, e.key)
			return true
		}
		val, err := db.readValue(e)
		if err != nil {
			return true
		}
		out = append(out, Entry{Key: []byte(e.key), Value: val})
		return limit <= 0 || len(out) < limit
	})

	for _, k := range expiredKeys {
		db.kd.delete(k)
	}
	return out
}

// DeleteRange removes all keys in [startKey, endKey) (same RangeEnd
// convention as Range) and returns the entries that were deleted.
func (db *DB) DeleteRange(startKey, endKey []byte) []Entry {
	var matched []string
	db.kd.ascend(string(startKey), endKey, func(e *keydirEntry) bool {
		matched = append(matched, e.key)
		return true
	})

	deleted := make([]Entry, 0, len(matched))
	for _, k := range matched {
		e, ok := db.kd.delete(k)
		if !ok || db.expired(e) {
			continue
		}
		val, err := db.readValue(e)
		if err != nil {
			continue
		}
		deleted = append(deleted, Entry{Key: []byte(e.key), Value: val})
	}
	return deleted
}

// Len reports the number of live (not-yet-expired-and-purged) keys.
func (db *DB) Len() int {
	return db.kd.len()
}

// Close stops the background merge loop and closes all open file handles.
func (db *DB) Close() error {
	close(db.stopc)
	db.wg.Wait()

	db.writeMu.Lock()
	err := db.activeFile.Close()
	db.writeMu.Unlock()

	db.filesMu.Lock()
	for id, f := range db.files {
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		delete(db.files, id)
	}
	db.filesMu.Unlock()
	return err
}

func (db *DB) backgroundLoop() {
	defer db.wg.Done()
	ticker := time.NewTicker(db.opt.MergeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			db.purgeExpired()
			db.merge()
		case <-db.stopc:
			return
		}
	}
}

func (db *DB) purgeExpired() {
	var expiredKeys []string
	db.kd.ascendAll(func(e *keydirEntry) bool {
		if db.expired(e) {
			expiredKeys = append(expiredKeys, e.key)
		}
		return true
	})
	for _, k := range expiredKeys {
		db.kd.delete(k)
	}
}
