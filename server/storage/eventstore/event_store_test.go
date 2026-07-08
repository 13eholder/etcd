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

package eventstore

import (
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

func newTestStore(t *testing.T) *EventStore {
	t.Helper()
	es, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { es.Close() })
	return es
}

func TestIsEventKey(t *testing.T) {
	if !IsEventKey([]byte("/registry/events/default/foo")) {
		t.Fatalf("expected event key to match")
	}
	if IsEventKey([]byte("/registry/pods/default/foo")) {
		t.Fatalf("expected non-event key not to match")
	}
}

func TestPutGetRoundTrip(t *testing.T) {
	es := newTestStore(t)
	key := []byte("/registry/events/default/foo")

	putResp, err := es.Put(&pb.PutRequest{Key: key, Value: []byte("v1")})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if putResp.Header.Revision == 0 {
		t.Fatalf("Put response revision = 0; want nonzero")
	}

	rangeResp, err := es.Range(&pb.RangeRequest{Key: key})
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	if len(rangeResp.Kvs) != 1 || string(rangeResp.Kvs[0].Value) != "v1" {
		t.Fatalf("Range = %+v; want one kv with value v1", rangeResp.Kvs)
	}
	if rangeResp.Kvs[0].CreateRevision != rangeResp.Kvs[0].ModRevision {
		t.Fatalf("first Put: CreateRevision(%d) != ModRevision(%d)", rangeResp.Kvs[0].CreateRevision, rangeResp.Kvs[0].ModRevision)
	}

	// second Put on the same key should bump ModRevision/Version but keep CreateRevision
	putResp2, err := es.Put(&pb.PutRequest{Key: key, Value: []byte("v2"), PrevKv: true})
	if err != nil {
		t.Fatalf("Put 2: %v", err)
	}
	if putResp2.PrevKv == nil || string(putResp2.PrevKv.Value) != "v1" {
		t.Fatalf("PrevKv = %v; want v1", putResp2.PrevKv)
	}

	rangeResp, err = es.Range(&pb.RangeRequest{Key: key})
	if err != nil {
		t.Fatalf("Range 2: %v", err)
	}
	kv := rangeResp.Kvs[0]
	if kv.Version != 2 {
		t.Fatalf("Version = %d; want 2", kv.Version)
	}
	if kv.CreateRevision == kv.ModRevision {
		t.Fatalf("CreateRevision should stay fixed across updates, got Create=%d Mod=%d", kv.CreateRevision, kv.ModRevision)
	}
}

// TestPutStoresLeaseAsMetadataOnly documents that Lease is no longer
// validated or used to compute expiry (see DefaultTTL): any value, including
// one that doesn't correspond to a real lease, is accepted and stored
// verbatim as read-back metadata on the KeyValue.
func TestPutStoresLeaseAsMetadataOnly(t *testing.T) {
	es := newTestStore(t)
	key := []byte("/registry/events/x")
	if _, err := es.Put(&pb.PutRequest{Key: key, Value: []byte("v"), Lease: 12345}); err != nil {
		t.Fatalf("Put with unknown lease id: %v", err)
	}

	rangeResp, err := es.Range(&pb.RangeRequest{Key: key})
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	if len(rangeResp.Kvs) != 1 || rangeResp.Kvs[0].Lease != 12345 {
		t.Fatalf("Kvs = %+v; want one kv with Lease=12345", rangeResp.Kvs)
	}
}

func TestDeleteRange(t *testing.T) {
	es := newTestStore(t)
	prefix := "/registry/events/ns1/"
	for _, k := range []string{"a", "b", "c"} {
		if _, err := es.Put(&pb.PutRequest{Key: []byte(prefix + k), Value: []byte("v")}); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	delResp, err := es.DeleteRange(&pb.DeleteRangeRequest{Key: []byte(prefix), RangeEnd: []byte{0}, PrevKv: true})
	if err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	if delResp.Deleted != 3 || len(delResp.PrevKvs) != 3 {
		t.Fatalf("Deleted=%d PrevKvs=%d; want 3, 3", delResp.Deleted, len(delResp.PrevKvs))
	}

	rangeResp, err := es.Range(&pb.RangeRequest{Key: []byte(prefix), RangeEnd: []byte{0}})
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	if len(rangeResp.Kvs) != 0 {
		t.Fatalf("Range after DeleteRange = %d kvs; want 0", len(rangeResp.Kvs))
	}
}

func TestWatch(t *testing.T) {
	es := newTestStore(t)
	key := []byte("/registry/events/default/watched")

	ws := es.NewWatchStream()
	defer ws.Close()

	id, err := ws.Watch(0, key, nil)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	if id < autoWatchIDBase {
		t.Fatalf("auto-assigned id %d below reserved base %d", id, autoWatchIDBase)
	}

	if _, err := es.Put(&pb.PutRequest{Key: key, Value: []byte("v1")}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	select {
	case wr := <-ws.Chan():
		if wr.WatchID != id {
			t.Fatalf("WatchID = %d; want %d", wr.WatchID, id)
		}
		if len(wr.Events) != 1 || wr.Events[0].Type != mvccpb.PUT {
			t.Fatalf("Events = %+v; want one PUT event", wr.Events)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for watch event")
	}

	// unrelated key must not be delivered
	if _, err := es.Put(&pb.PutRequest{Key: []byte("/registry/events/default/other"), Value: []byte("v")}); err != nil {
		t.Fatalf("Put unrelated: %v", err)
	}
	select {
	case wr := <-ws.Chan():
		t.Fatalf("unexpected event for unrelated key: %+v", wr)
	case <-time.After(100 * time.Millisecond):
	}

	if err := ws.Cancel(id); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
}
