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

// IsEventTxn reports whether every key referenced anywhere in r — every
// Compare and every op in Success/Failure, recursively through nested
// transactions — is an event key, so the whole request can be served
// against the local bitcask store instead of Raft/MVCC.
//
// kube-apiserver's etcd3 storage driver issues Create/GuaranteedUpdate/
// conditionalDelete as a Txn (If <revision unchanged> Then <Put/Delete>
// Else <Range>) rather than a bare Put/DeleteRange, and it never mixes
// keys from two different resource types (i.e. two different key
// prefixes) in one Txn: each resource type owns its own etcd3.Store bound
// to one prefix. So in practice this only ever needs to recognize a Txn
// entirely scoped to /registry/events/ keys.
//
// A Compare with a non-empty RangeEnd (a multi-key range comparison) is
// treated as not a pure event Txn and falls through to the normal path:
// kube-apiserver never generates one for Event keys, and this bypass
// isn't equipped to evaluate a range comparison against the bitcask
// store.
func IsEventTxn(r *pb.TxnRequest) bool {
	seenKey := false
	return isEventTxn(r, &seenKey) && seenKey
}

func isEventTxn(r *pb.TxnRequest, seenKey *bool) bool {
	for _, c := range r.Compare {
		if len(c.RangeEnd) > 0 {
			return false
		}
		*seenKey = true
		if !IsEventKey(c.Key) {
			return false
		}
	}
	for _, ops := range [2][]*pb.RequestOp{r.Success, r.Failure} {
		for _, op := range ops {
			switch tv := op.Request.(type) {
			case *pb.RequestOp_RequestRange:
				*seenKey = true
				if !IsEventKey(tv.RequestRange.Key) {
					return false
				}
			case *pb.RequestOp_RequestPut:
				*seenKey = true
				if !IsEventKey(tv.RequestPut.Key) {
					return false
				}
			case *pb.RequestOp_RequestDeleteRange:
				*seenKey = true
				if !IsEventKey(tv.RequestDeleteRange.Key) {
					return false
				}
			case *pb.RequestOp_RequestTxn:
				if !isEventTxn(tv.RequestTxn, seenKey) {
					return false
				}
			}
		}
	}
	return true
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

// Txn serves a Txn request against the local bitcask store: r must satisfy
// IsEventTxn (every Compare/op key is an event key). Compare evaluates
// against the current bitcask contents, then the matching op list
// (Success or Failure) executes in order via Put/Range/DeleteRange, so
// each op's normal side effects (revision bump, watch notification) apply
// exactly as if it had been issued standalone.
func (es *EventStore) Txn(r *pb.TxnRequest) (*pb.TxnResponse, error) {
	succeeded := es.applyCompares(r.Compare)
	ops := r.Success
	if !succeeded {
		ops = r.Failure
	}

	responses := make([]*pb.ResponseOp, len(ops))
	for i, op := range ops {
		resp, err := es.applyOp(op)
		if err != nil {
			return nil, err
		}
		responses[i] = resp
	}

	return &pb.TxnResponse{
		Header:    &pb.ResponseHeader{Revision: es.Revision()},
		Succeeded: succeeded,
		Responses: responses,
	}, nil
}

func (es *EventStore) applyCompares(cmps []*pb.Compare) bool {
	for _, c := range cmps {
		if !es.applyCompare(c) {
			return false
		}
	}
	return true
}

// applyCompare mirrors mvcc's applyCompare/compareKV (see
// server/etcdserver/txn/txn.go): a missing key compares as a zero-value
// KeyValue, except Compare_VALUE, which always fails against a missing
// key since there is no byte string that means "absent".
func (es *EventStore) applyCompare(c *pb.Compare) bool {
	startKey, endKey := rangeBounds(c.Key, c.RangeEnd)
	entries := es.db.Range(startKey, endKey, 0)
	if len(entries) == 0 {
		if c.Target == pb.Compare_VALUE {
			return false
		}
		return compareKV(c, mvccpb.KeyValue{})
	}
	for _, e := range entries {
		kv := &mvccpb.KeyValue{}
		if err := kv.Unmarshal(e.Value); err != nil {
			return false
		}
		if !compareKV(c, *kv) {
			return false
		}
	}
	return true
}

func compareKV(c *pb.Compare, kv mvccpb.KeyValue) bool {
	var result int
	switch c.Target {
	case pb.Compare_VALUE:
		var v []byte
		if tv, _ := c.TargetUnion.(*pb.Compare_Value); tv != nil {
			v = tv.Value
		}
		result = bytes.Compare(kv.Value, v)
	case pb.Compare_CREATE:
		var rev int64
		if tv, _ := c.TargetUnion.(*pb.Compare_CreateRevision); tv != nil {
			rev = tv.CreateRevision
		}
		result = compareInt64(kv.CreateRevision, rev)
	case pb.Compare_MOD:
		var rev int64
		if tv, _ := c.TargetUnion.(*pb.Compare_ModRevision); tv != nil {
			rev = tv.ModRevision
		}
		result = compareInt64(kv.ModRevision, rev)
	case pb.Compare_VERSION:
		var rev int64
		if tv, _ := c.TargetUnion.(*pb.Compare_Version); tv != nil {
			rev = tv.Version
		}
		result = compareInt64(kv.Version, rev)
	case pb.Compare_LEASE:
		var rev int64
		if tv, _ := c.TargetUnion.(*pb.Compare_Lease); tv != nil {
			rev = tv.Lease
		}
		result = compareInt64(kv.Lease, rev)
	}
	switch c.Result {
	case pb.Compare_EQUAL:
		return result == 0
	case pb.Compare_NOT_EQUAL:
		return result != 0
	case pb.Compare_GREATER:
		return result > 0
	case pb.Compare_LESS:
		return result < 0
	}
	return true
}

func compareInt64(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// applyOp dispatches a single Success/Failure op to the same Put/Range/
// DeleteRange methods a standalone request would use, so it gets the same
// revision bump and watch notification, then wraps the result in the
// ResponseOp shape Txn expects.
func (es *EventStore) applyOp(op *pb.RequestOp) (*pb.ResponseOp, error) {
	switch tv := op.Request.(type) {
	case *pb.RequestOp_RequestRange:
		resp, err := es.Range(tv.RequestRange)
		if err != nil {
			return nil, err
		}
		return &pb.ResponseOp{Response: &pb.ResponseOp_ResponseRange{ResponseRange: resp}}, nil
	case *pb.RequestOp_RequestPut:
		resp, err := es.Put(tv.RequestPut)
		if err != nil {
			return nil, err
		}
		return &pb.ResponseOp{Response: &pb.ResponseOp_ResponsePut{ResponsePut: resp}}, nil
	case *pb.RequestOp_RequestDeleteRange:
		resp, err := es.DeleteRange(tv.RequestDeleteRange)
		if err != nil {
			return nil, err
		}
		return &pb.ResponseOp{Response: &pb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: resp}}, nil
	case *pb.RequestOp_RequestTxn:
		resp, err := es.Txn(tv.RequestTxn)
		if err != nil {
			return nil, err
		}
		return &pb.ResponseOp{Response: &pb.ResponseOp_ResponseTxn{ResponseTxn: resp}}, nil
	default:
		return &pb.ResponseOp{}, nil
	}
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
