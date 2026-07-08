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
	"bytes"
	"errors"
	"sync"

	"go.etcd.io/etcd/api/v3/mvccpb"
)

// WatchID identifies one watcher within a WatchStream.
type WatchID int64

// autoWatchIDBase is the first ID handed out for auto-assigned (WatchID==0
// requested) event watches. It is offset far above any ID the mvcc watch
// store would hand out for a normal (non-event) watch on the same gRPC
// stream, so the two ID spaces can never collide even though each is
// generated independently and both start counting from a small value
// internally.
const autoWatchIDBase WatchID = 1 << 32

// ErrWatchStreamClosed is returned by Watch after the stream has been closed.
var ErrWatchStreamClosed = errors.New("eventstore: watch stream closed")

// ErrWatcherDuplicateID is returned by Watch when id is already registered
// on this stream.
var ErrWatcherDuplicateID = errors.New("eventstore: duplicate watch id")

// WatchResponse carries events for delivery to one WatchID on a stream.
type WatchResponse struct {
	WatchID WatchID
	Events  []mvccpb.Event
}

// WatchStream is a single client-facing subscription session against an
// EventStore, mirroring the shape of mvcc.WatchStream closely enough that
// server/etcdserver/api/v3rpc/watch.go can drive both side by side. There is
// no history: a Watch call only ever observes events written after it is
// registered (StartRevision is not honored).
type WatchStream interface {
	Watch(id WatchID, key, rangeEnd []byte) (WatchID, error)
	Cancel(id WatchID) error
	Chan() <-chan WatchResponse
	Rev() int64
	Close()
}

type eventWatcher struct {
	key      []byte
	rangeEnd []byte // nil: single key; empty non-nil: from key to end; else [key,rangeEnd)
}

func (w *eventWatcher) matches(key []byte) bool {
	switch {
	case w.rangeEnd == nil:
		return bytes.Equal(w.key, key)
	case len(w.rangeEnd) == 0:
		return bytes.Compare(key, w.key) >= 0
	default:
		return bytes.Compare(key, w.key) >= 0 && bytes.Compare(key, w.rangeEnd) < 0
	}
}

type watchStream struct {
	es *EventStore
	ch chan WatchResponse

	mu       sync.Mutex
	closed   bool
	watchers map[WatchID]*eventWatcher
	nextAuto WatchID
}

const watchChanBufLen = 128

func (ws *watchStream) Watch(id WatchID, key, rangeEnd []byte) (WatchID, error) {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	if ws.closed {
		return -1, ErrWatchStreamClosed
	}
	if id == 0 {
		for ws.watchers[ws.nextAuto] != nil {
			ws.nextAuto++
		}
		id = ws.nextAuto
		ws.nextAuto++
	} else if _, ok := ws.watchers[id]; ok {
		return -1, ErrWatcherDuplicateID
	}

	w := &eventWatcher{key: append([]byte(nil), key...)}
	if rangeEnd != nil {
		w.rangeEnd = append([]byte{}, rangeEnd...)
	}
	ws.watchers[id] = w
	return id, nil
}

func (ws *watchStream) Cancel(id WatchID) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	delete(ws.watchers, id)
	return nil
}

func (ws *watchStream) Chan() <-chan WatchResponse { return ws.ch }

func (ws *watchStream) Rev() int64 { return ws.es.Revision() }

// Close marks the stream closed and closes Chan(). It removes the stream
// from the fan-out set before closing anything, so no call to dispatch can
// be in flight for this stream once Chan() is closed (dispatch and remove
// both serialize on watchStreamSet.mu).
func (ws *watchStream) Close() {
	ws.es.watchStreams.remove(ws)

	ws.mu.Lock()
	if ws.closed {
		ws.mu.Unlock()
		return
	}
	ws.closed = true
	ws.mu.Unlock()
	close(ws.ch)
}

// dispatch delivers ev to every watcher on this stream whose key range
// matches. Delivery is best-effort: a full channel drops the event rather
// than blocking the writer that produced it.
func (ws *watchStream) dispatch(ev mvccpb.Event) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	for id, w := range ws.watchers {
		if !w.matches(ev.Kv.Key) {
			continue
		}
		select {
		case ws.ch <- WatchResponse{WatchID: id, Events: []mvccpb.Event{ev}}:
		default:
		}
	}
}

// watchStreamSet tracks every live watchStream for an EventStore so writes
// can fan out to all of them.
type watchStreamSet struct {
	mu      sync.RWMutex
	streams map[*watchStream]struct{}
}

func newWatchStreamSet() watchStreamSet {
	return watchStreamSet{streams: make(map[*watchStream]struct{})}
}

func (s *watchStreamSet) newStream(es *EventStore) *watchStream {
	ws := &watchStream{
		es:       es,
		ch:       make(chan WatchResponse, watchChanBufLen),
		watchers: make(map[WatchID]*eventWatcher),
		nextAuto: autoWatchIDBase,
	}
	s.mu.Lock()
	s.streams[ws] = struct{}{}
	s.mu.Unlock()
	return ws
}

func (s *watchStreamSet) remove(ws *watchStream) {
	s.mu.Lock()
	delete(s.streams, ws)
	s.mu.Unlock()
}

func (s *watchStreamSet) notify(ev mvccpb.Event) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for ws := range s.streams {
		ws.dispatch(ev)
	}
}
