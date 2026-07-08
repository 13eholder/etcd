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
	"fmt"
	"testing"
	"time"
)

func openTestDB(t *testing.T, opt Options) *DB {
	t.Helper()
	db, err := Open(t.TempDir(), opt)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestPutGet(t *testing.T) {
	db := openTestDB(t, Options{})

	if err := db.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	val, ok := db.Get([]byte("k1"))
	if !ok || string(val) != "v1" {
		t.Fatalf("Get = %q, %v; want v1, true", val, ok)
	}

	// overwrite
	if err := db.Put([]byte("k1"), []byte("v2")); err != nil {
		t.Fatalf("Put overwrite: %v", err)
	}
	val, ok = db.Get([]byte("k1"))
	if !ok || string(val) != "v2" {
		t.Fatalf("Get after overwrite = %q, %v; want v2, true", val, ok)
	}

	if _, ok := db.Get([]byte("missing")); ok {
		t.Fatalf("Get(missing) = true; want false")
	}
}

func TestDelete(t *testing.T) {
	db := openTestDB(t, Options{})
	db.Put([]byte("k1"), []byte("v1"))
	db.Delete([]byte("k1"))
	if _, ok := db.Get([]byte("k1")); ok {
		t.Fatalf("Get after Delete = true; want false")
	}
}

func TestExpiry(t *testing.T) {
	db := openTestDB(t, Options{TTL: 50 * time.Millisecond})

	db.Put([]byte("k1"), []byte("v"))
	if _, ok := db.Get([]byte("k1")); !ok {
		t.Fatalf("Get(k1) right after Put = false; want true")
	}

	time.Sleep(80 * time.Millisecond)
	if _, ok := db.Get([]byte("k1")); ok {
		t.Fatalf("Get(k1) after TTL elapsed = true; want false")
	}
}

func TestNoExpiryByDefault(t *testing.T) {
	db := openTestDB(t, Options{}) // TTL: 0 (unset) means never expire
	db.Put([]byte("k1"), []byte("v"))
	time.Sleep(20 * time.Millisecond)
	if _, ok := db.Get([]byte("k1")); !ok {
		t.Fatalf("Get(k1) with TTL=0 = false; want true (never expires)")
	}
}

func TestRange(t *testing.T) {
	db := openTestDB(t, Options{})
	keys := []string{"a", "b", "c", "d", "e"}
	for _, k := range keys {
		db.Put([]byte(k), []byte("v-"+k))
	}

	// single key
	entries := db.Range([]byte("b"), nil, 0)
	if len(entries) != 1 || string(entries[0].Key) != "b" {
		t.Fatalf("single-key Range = %v; want [b]", entries)
	}

	// [b, d)
	entries = db.Range([]byte("b"), []byte("d"), 0)
	if len(entries) != 2 || string(entries[0].Key) != "b" || string(entries[1].Key) != "c" {
		t.Fatalf("[b,d) Range = %v; want [b c]", entries)
	}

	// b to end
	entries = db.Range([]byte("c"), []byte{}, 0)
	if len(entries) != 3 {
		t.Fatalf("[c,end) Range len = %d; want 3", len(entries))
	}

	// limit
	entries = db.Range([]byte("a"), []byte{}, 2)
	if len(entries) != 2 {
		t.Fatalf("limited Range len = %d; want 2", len(entries))
	}
}

func TestDeleteRange(t *testing.T) {
	db := openTestDB(t, Options{})
	for _, k := range []string{"a", "b", "c"} {
		db.Put([]byte(k), []byte("v-"+k))
	}

	deleted := db.DeleteRange([]byte("a"), []byte("c"))
	if len(deleted) != 2 {
		t.Fatalf("DeleteRange len = %d; want 2", len(deleted))
	}
	if _, ok := db.Get([]byte("a")); ok {
		t.Fatalf("Get(a) after DeleteRange = true; want false")
	}
	if _, ok := db.Get([]byte("c")); !ok {
		t.Fatalf("Get(c) after DeleteRange = false; want true")
	}
}

func TestFileRotationAndMerge(t *testing.T) {
	// tiny max file size to force rotation after a couple of writes
	db := openTestDB(t, Options{MaxFileSize: 128})

	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("key-%03d", i)
		if err := db.Put([]byte(k), []byte("some-value-padding")); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	if db.activeID <= 1 {
		t.Fatalf("activeID = %d; want > 1 (expected rotation)", db.activeID)
	}

	// overwrite every key so all older files become fully dead
	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("key-%03d", i)
		if err := db.Put([]byte(k), []byte("updated")); err != nil {
			t.Fatalf("Put(update) %d: %v", i, err)
		}
	}

	if err := db.merge(); err != nil {
		t.Fatalf("merge: %v", err)
	}

	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("key-%03d", i)
		val, ok := db.Get([]byte(k))
		if !ok || string(val) != "updated" {
			t.Fatalf("Get(%s) after merge = %q, %v; want updated, true", k, val, ok)
		}
	}
}

// TestMergePreservesTstamp guards against restamping a record's write time
// during merge: since expiry is TTL-since-tstamp, restamping to the merge
// time would reset every surviving record's expiry clock and keys would
// never expire as long as merge keeps running.
func TestMergePreservesTstamp(t *testing.T) {
	db := openTestDB(t, Options{MaxFileSize: 64, TTL: 150 * time.Millisecond})

	db.Put([]byte("k1"), []byte("v1"))
	// force a rotation so k1's file becomes sealed and eligible for merge
	db.Put([]byte("k2"), []byte(make([]byte, 128)))

	if db.activeID <= 1 {
		t.Fatalf("activeID = %d; want > 1 (expected rotation)", db.activeID)
	}
	if err := db.merge(); err != nil {
		t.Fatalf("merge: %v", err)
	}
	if _, ok := db.Get([]byte("k1")); !ok {
		t.Fatalf("Get(k1) right after merge = false; want true (not yet expired)")
	}

	time.Sleep(200 * time.Millisecond)
	if _, ok := db.Get([]byte("k1")); ok {
		t.Fatalf("Get(k1) after TTL elapsed post-merge = true; want false (merge must not reset tstamp)")
	}
}
