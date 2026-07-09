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
	"sync"

	"github.com/google/btree"
)

// keydirEntry is the in-memory index entry for one key: it points at the
// on-disk location of the value in a sealed or active data file. There is no
// version history here — a Put overwrites the previous entry in place.
type keydirEntry struct {
	key       string
	fileID    uint32
	valuePos  int64
	valueSize uint32
	tstamp    int64 // unix nano write time; expiry is DB.opt.TTL after this
}

// keydir is the ordered in-memory index (key -> keydirEntry) backing a DB.
// It is implemented as a B-tree, rather than the hash map used by textbook
// Bitcask, so that Range/DeleteRange can do ordered prefix/interval scans.
type keydir struct {
	mu   sync.RWMutex
	tree *btree.BTreeG[*keydirEntry]
}

func newKeydir() *keydir {
	return &keydir{
		tree: btree.NewG(32, func(a, b *keydirEntry) bool {
			return a.key < b.key
		}),
	}
}

func (kd *keydir) set(e *keydirEntry) {
	kd.mu.Lock()
	defer kd.mu.Unlock()
	kd.tree.ReplaceOrInsert(e)
}

func (kd *keydir) get(key string) (*keydirEntry, bool) {
	kd.mu.RLock()
	defer kd.mu.RUnlock()
	return kd.tree.Get(&keydirEntry{key: key})
}

func (kd *keydir) delete(key string) (*keydirEntry, bool) {
	kd.mu.Lock()
	defer kd.mu.Unlock()
	return kd.tree.Delete(&keydirEntry{key: key})
}

// ascend visits entries in [start, end) in ascending key order.
//   - end == nil:            visit only the start key itself
//   - end != nil && len == 0: visit from start to the end of the keyspace
//   - otherwise:              visit start <= key < end
func (kd *keydir) ascend(start string, end []byte, visit func(*keydirEntry) bool) {
	kd.mu.RLock()
	defer kd.mu.RUnlock()

	if end == nil {
		if e, ok := kd.tree.Get(&keydirEntry{key: start}); ok {
			visit(e)
		}
		return
	}

	endStr := string(end)
	kd.tree.AscendGreaterOrEqual(&keydirEntry{key: start}, func(item *keydirEntry) bool {
		if len(endStr) > 0 && item.key >= endStr {
			return false
		}
		return visit(item)
	})
}

func (kd *keydir) len() int {
	kd.mu.RLock()
	defer kd.mu.RUnlock()
	return kd.tree.Len()
}
