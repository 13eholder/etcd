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

// Package eventstore implements a bypass storage path for high-churn,
// disposable keys (K8s Events): Put/Range/DeleteRange/Watch for keys under
// KeyPrefix never go through Raft, MVCC or BoltDB. Data is kept in a local
// Bitcask-style disk engine (see server/storage/bitcask) instead, so it is
// not replicated across cluster members and does not survive a restart.
//
// See docs/event-bitcask.md for the full design.
package eventstore

import (
	"bytes"
	"sync/atomic"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/server/v3/storage/bitcask"
)

// KeyPrefix identifies keys handled by the EventStore instead of the normal
// Raft/MVCC path. It matches the key prefix Kubernetes' apiserver uses for
// Event objects.
const KeyPrefix = "/registry/events/"

// DefaultTTL is the store-wide expiry applied to every event key, measured
// from each record's write time. It matches the TTL Kubernetes' apiserver
// uses for Event objects (LeaseGrant(ttl=3600) before every Event Put), and
// is applied uniformly instead of looking up each Put's actual lease: once a
// key is written, its expiry no longer depends on that lease's state, so
// LeaseGrant/Revoke for event keys are pure bookkeeping that this store
// never has to consult.
const DefaultTTL = time.Hour

// IsEventKey reports whether key should be routed to the EventStore.
func IsEventKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte(KeyPrefix))
}

// EventStore serves Put/Range/DeleteRange/Watch for event keys against a
// local bitcask.DB, bypassing Raft/MVCC/BoltDB entirely.
type EventStore struct {
	db *bitcask.DB

	rev int64 // monotonically increasing local revision, atomic

	watchStreams watchStreamSet
}

// New opens an EventStore rooted at dir (any previous contents are wiped,
// see bitcask.Open). Every key expires DefaultTTL after it is written.
func New(dir string) (*EventStore, error) {
	db, err := bitcask.Open(dir, bitcask.Options{TTL: DefaultTTL})
	if err != nil {
		return nil, err
	}
	return &EventStore{
		db:           db,
		watchStreams: newWatchStreamSet(),
	}, nil
}

// Close releases the underlying bitcask.DB.
func (es *EventStore) Close() error {
	return es.db.Close()
}

func (es *EventStore) nextRevision() int64 {
	return atomic.AddInt64(&es.rev, 1)
}

// Revision returns the current local revision.
func (es *EventStore) Revision() int64 {
	return atomic.LoadInt64(&es.rev)
}

// Put stores an event key. Its expiry is DefaultTTL after this write,
// regardless of r.Lease: LeaseGrant/Revoke for event keys keep going through
// the normal Raft/BoltDB path unaffected, but this store never attaches to
// or looks up that lease, so its actual TTL/remaining time is irrelevant
// here. r.Lease is stored as-is, purely as response/read-back metadata.
func (es *EventStore) Put(r *pb.PutRequest) (*pb.PutResponse, error) {
	var oldKV *mvccpb.KeyValue
	if old, found := es.db.Get(r.Key); found {
		oldKV = &mvccpb.KeyValue{}
		if err := oldKV.Unmarshal(old); err != nil {
			oldKV = nil
		}
	}

	value := r.Value
	if r.IgnoreValue && oldKV != nil {
		value = oldKV.Value
	}
	leaseVal := r.Lease
	if r.IgnoreLease && oldKV != nil {
		leaseVal = oldKV.Lease
	}

	rev := es.nextRevision()
	createRev := rev
	version := int64(1)
	if oldKV != nil {
		createRev = oldKV.CreateRevision
		version = oldKV.Version + 1
	}

	kv := &mvccpb.KeyValue{
		Key:            r.Key,
		Value:          value,
		CreateRevision: createRev,
		ModRevision:    rev,
		Version:        version,
		Lease:          leaseVal,
	}
	data, err := kv.Marshal()
	if err != nil {
		return nil, err
	}
	if err := es.db.Put(r.Key, data); err != nil {
		return nil, err
	}

	es.watchStreams.notify(mvccpb.Event{Type: mvccpb.PUT, Kv: kv, PrevKv: oldKV})

	resp := &pb.PutResponse{Header: &pb.ResponseHeader{Revision: rev}}
	if r.PrevKv && oldKV != nil {
		resp.PrevKv = oldKV
	}
	return resp, nil
}

// Range serves a Range request against the local bitcask store.
func (es *EventStore) Range(r *pb.RangeRequest) (*pb.RangeResponse, error) {
	startKey, endKey := rangeBounds(r.Key, r.RangeEnd)

	// Fetch the full match set (unlimited) so Count reflects all matches,
	// then truncate Kvs to r.Limit, mirroring mvcc's Range semantics.
	entries := es.db.Range(startKey, endKey, 0)

	kvs := make([]*mvccpb.KeyValue, 0, len(entries))
	for _, e := range entries {
		kv := &mvccpb.KeyValue{}
		if err := kv.Unmarshal(e.Value); err != nil {
			continue
		}
		if r.KeysOnly {
			kv.Value = nil
		}
		kvs = append(kvs, kv)
	}

	resp := &pb.RangeResponse{
		Header: &pb.ResponseHeader{Revision: es.Revision()},
		Count:  int64(len(kvs)),
	}
	if !r.CountOnly {
		if r.Limit > 0 && int64(len(kvs)) > r.Limit {
			resp.More = true
			kvs = kvs[:r.Limit]
		}
		resp.Kvs = kvs
	}
	return resp, nil
}

// DeleteRange serves a DeleteRange request against the local bitcask store.
func (es *EventStore) DeleteRange(r *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error) {
	startKey, endKey := rangeBounds(r.Key, r.RangeEnd)
	deleted := es.db.DeleteRange(startKey, endKey)

	rev := es.nextRevision()
	var prevKvs []*mvccpb.KeyValue
	for _, d := range deleted {
		oldKV := &mvccpb.KeyValue{}
		if err := oldKV.Unmarshal(d.Value); err != nil {
			continue
		}
		es.watchStreams.notify(mvccpb.Event{
			Type:   mvccpb.DELETE,
			Kv:     &mvccpb.KeyValue{Key: d.Key, ModRevision: rev},
			PrevKv: oldKV,
		})
		if r.PrevKv {
			prevKvs = append(prevKvs, oldKV)
		}
	}

	resp := &pb.DeleteRangeResponse{
		Header:  &pb.ResponseHeader{Revision: rev},
		Deleted: int64(len(deleted)),
	}
	if r.PrevKv {
		resp.PrevKvs = prevKvs
	}
	return resp, nil
}

// NewWatchStream returns a new WatchStream fed by this EventStore's writes.
func (es *EventStore) NewWatchStream() WatchStream {
	return es.watchStreams.newStream(es)
}

// rangeBounds translates the etcd wire convention for (key, rangeEnd) into
// the (start, end) pair understood by bitcask.DB.Range/DeleteRange:
//   - len(rangeEnd) == 0:                 single key (end == nil)
//   - len(rangeEnd) == 1 && rangeEnd[0]==0: from key to the end of the keyspace (end == []byte{})
//   - otherwise:                          [key, rangeEnd)
func rangeBounds(key, rangeEnd []byte) (start, end []byte) {
	switch {
	case len(rangeEnd) == 0:
		return key, nil
	case len(rangeEnd) == 1 && rangeEnd[0] == 0:
		return key, []byte{}
	default:
		return key, rangeEnd
	}
}
