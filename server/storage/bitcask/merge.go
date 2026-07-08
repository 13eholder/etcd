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

package bitcask

import (
	"os"
)

// merge reclaims disk space held by sealed (non-active) data files: it
// rewrites every keydir entry that still points at a sealed file into one
// new compacted file, then deletes sealed files no longer referenced by any
// keydir entry.
//
// Because this engine never recovers from the log (Open always starts
// empty) and keeps no version history, merge does not need to preserve
// tombstones or anything for a "previous replica" — a key that was deleted
// or overwritten during the scan is simply absent from the rewrite.
func (db *DB) merge() error {
	db.writeMu.Lock()
	mergeBeforeID := db.activeID // files with id < mergeBeforeID are sealed and eligible
	newID := db.allocFileID()
	db.writeMu.Unlock()

	if mergeBeforeID <= 1 {
		// nothing sealed yet
		return nil
	}

	outPath := dataFileName(db.dir, newID)
	out, err := os.OpenFile(outPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}

	var keys []string
	db.kd.ascendAll(func(e *keydirEntry) bool {
		if e.fileID < mergeBeforeID {
			keys = append(keys, e.key)
		}
		return true
	})

	var offset int64
	wrote := false
	for _, k := range keys {
		e, ok := db.kd.get(k)
		if !ok || e.fileID >= mergeBeforeID {
			continue // deleted or already rewritten by a newer Put since the scan
		}
		if db.expired(e) {
			db.kd.delete(k)
			continue
		}
		val, err := db.readValue(e)
		if err != nil {
			continue
		}
		// Preserve the original tstamp: since expiry is now measured as
		// TTL-since-tstamp, restamping to time.Now() here would reset every
		// record's expiry clock on each merge and keys would never expire.
		rec, valueOff := encodeRecord(e.tstamp, []byte(k), val)
		if _, err := out.Write(rec); err != nil {
			out.Close()
			return err
		}
		newEntry := &keydirEntry{
			key:       k,
			fileID:    newID,
			valuePos:  offset + int64(valueOff),
			valueSize: e.valueSize,
			tstamp:    e.tstamp,
		}
		db.kd.compareAndSwap(e, newEntry)
		offset += int64(len(rec))
		wrote = true
	}
	out.Close()

	if !wrote {
		os.Remove(outPath)
	} else {
		rf, err := os.Open(outPath)
		if err == nil {
			db.filesMu.Lock()
			db.files[newID] = rf
			db.filesMu.Unlock()
		}
	}

	db.reclaimSealedFiles(mergeBeforeID)
	return nil
}

// reclaimSealedFiles deletes any file with id < before that no keydir entry
// references any more.
func (db *DB) reclaimSealedFiles(before uint32) {
	refCount := make(map[uint32]int)
	db.kd.ascendAll(func(e *keydirEntry) bool {
		refCount[e.fileID]++
		return true
	})

	db.filesMu.Lock()
	defer db.filesMu.Unlock()
	for id, f := range db.files {
		if id >= before {
			continue
		}
		if refCount[id] > 0 {
			continue
		}
		f.Close()
		delete(db.files, id)
		os.Remove(dataFileName(db.dir, id))
	}
}
