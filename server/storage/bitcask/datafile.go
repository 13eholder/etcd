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
	"encoding/binary"
	"fmt"
	"path/filepath"
)

// On-disk record layout (all integers big-endian):
//
//	tstamp(8) | keySize(4) | valueSize(4) | key | value
//
// tstamp is the record's original write time (preserved verbatim across
// merges, see merge.go) and is the sole input to expiry: a record is
// treated as absent once DB.opt.TTL has elapsed since tstamp, mirroring
// Riak bitcask's bucket-level expiry rather than storing a per-record
// absolute deadline.
//
// There is no checksum and no tombstone marker: this engine never rebuilds
// its keydir from the log (data directory is wiped on every Open), so
// nothing ever reads the log for anything other than the value bytes a live
// keydir entry already points at.
const headerSize = 16

func dataFileName(dir string, fileID uint32) string {
	return filepath.Join(dir, fmt.Sprintf("%09d.data", fileID))
}

// encodeRecord serializes one record and reports the offset within it where
// the value bytes begin (== headerSize+len(key)).
func encodeRecord(tstamp int64, key, value []byte) (buf []byte, valueOff int) {
	buf = make([]byte, headerSize+len(key)+len(value))
	binary.BigEndian.PutUint64(buf[0:8], uint64(tstamp))
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(key)))
	binary.BigEndian.PutUint32(buf[12:16], uint32(len(value)))
	copy(buf[headerSize:], key)
	copy(buf[headerSize+len(key):], value)
	return buf, headerSize + len(key)
}
