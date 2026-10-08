// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package binlogstream

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/binlog/event"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"
)

func archiveFile(t *testing.T, dir, name, next string) (uint32, uint32) {
	t.Helper()
	h := &replication.EventHeader{ServerID: 1, Timestamp: 1700000000}
	fde, err := event.GenFormatDescriptionEvent(h, 4)
	require.NoError(t, err)
	query, err := event.GenQueryEvent(h, fde.Header.LogPos, 0, 0, 0, nil, []byte("test"), []byte("CREATE DATABASE test"))
	require.NoError(t, err)
	data := append(append([]byte{}, replication.BinLogFileHeader...), fde.RawData...)
	data = append(data, query.RawData...)
	if next != "" {
		rotate, err := event.GenRotateEvent(h, query.Header.LogPos, []byte(next), 4)
		require.NoError(t, err)
		data = append(data, rotate.RawData...)
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), data, 0o600))
	return fde.Header.LogPos, query.Header.LogPos
}

func TestArchiveEventCacheCount(t *testing.T) {
	for _, size := range []int{0, -1, 1, 7} {
		r := &archiveReader{
			end: mysql.Position{Name: "mysql-bin.000001", Pos: 4},
			cfg: replication.BinlogSyncerConfig{EventCacheCount: size},
		}
		loc := binlog.MustZeroLocation("mysql")
		loc.Position = r.end
		_, err := r.GenerateStreamFrom(loc)
		require.NoError(t, err)
		want := size
		if want <= 0 {
			want = 10240 // go-mysql BinlogStreamer's default event cache
		}
		require.Equal(t, want, cap(r.results))
		_, err = r.GetEvent(context.Background())
		require.ErrorIs(t, err, ErrArchiveComplete)
		r.Close()
	}
}

func TestBufferedArchiveMatchesParseFile(t *testing.T) {
	h := &replication.EventHeader{ServerID: 1, Timestamp: 1700000000}
	fde, err := event.GenFormatDescriptionEvent(h, 4)
	require.NoError(t, err)
	large, err := event.GenQueryEvent(h, fde.Header.LogPos, 0, 0, 0, nil, []byte("test"), []byte(strings.Repeat("x", 2*archiveReadBufferSize+17)))
	require.NoError(t, err)
	last, err := event.GenQueryEvent(h, large.Header.LogPos, 0, 0, 0, nil, []byte("test"), []byte("CREATE TABLE t (id INT)"))
	require.NoError(t, err)
	data := append([]byte{}, replication.BinLogFileHeader...)
	for _, e := range []*replication.BinlogEvent{fde, large, last} {
		data = append(data, e.RawData...)
	}
	name := filepath.Join(t.TempDir(), "mysql-bin.000001")
	require.NoError(t, os.WriteFile(name, data, 0o600))
	newParser := func() *replication.BinlogParser {
		p := replication.NewBinlogParser()
		p.SetVerifyChecksum(true)
		return p
	}
	// Resume before and after a >64 KiB event, and at EOF. The initial FDE
	// prefetch must not leak into the post-seek stream or lose decoding state.
	for _, offset := range []uint32{4, fde.Header.LogPos, large.Header.LogPos, last.Header.LogPos} {
		var expected, actual []*replication.BinlogEvent
		require.NoError(t, newParser().ParseFile(name, int64(offset), func(e *replication.BinlogEvent) error {
			expected = append(expected, e)
			return nil
		}))
		require.NoError(t, parseArchiveFile(newParser(), name, int64(offset), func(e *replication.BinlogEvent) error {
			actual = append(actual, e)
			return nil
		}))
		require.Equal(t, expected, actual, "offset %d", offset)
	}
	stop := errors.New("stop consuming")
	for _, offset := range []int64{4, int64(large.Header.LogPos)} {
		err = parseArchiveFile(newParser(), name, offset, func(*replication.BinlogEvent) error { return stop })
		require.ErrorIs(t, err, stop)
	}
	for _, length := range []int{0, 3, len(data) - 1} {
		require.NoError(t, os.WriteFile(name, data[:length], 0o600))
		err = parseArchiveFile(newParser(), name, 4, func(*replication.BinlogEvent) error { return nil })
		require.Error(t, err, "length %d", length)
	}
	// go-mysql treats an incomplete event header as EOF; the archive boundary
	// check must still reject this as an incomplete workload, never success.
	require.NoError(t, os.WriteFile(name, data[:5], 0o600))
	r := &archiveReader{directory: filepath.Dir(name), end: mysql.Position{Name: filepath.Base(name), Pos: last.Header.LogPos}}
	loc := binlog.MustZeroLocation("mysql")
	loc.Position = mysql.Position{Name: filepath.Base(name), Pos: 4}
	_, err = r.GenerateStreamFrom(loc)
	require.NoError(t, err)
	_, err = r.GetEvent(context.Background())
	require.ErrorContains(t, err, "ended before configured end")
	r.Close()
	corrupt := append([]byte{}, data...)
	corrupt[len(corrupt)-5] ^= 1 // complete event with invalid CRC32
	require.NoError(t, os.WriteFile(name, corrupt, 0o600))
	err = parseArchiveFile(newParser(), name, 4, func(*replication.BinlogEvent) error { return nil })
	require.Error(t, err)
	corrupt = append([]byte{}, data...)
	corrupt[0] ^= 1
	require.NoError(t, os.WriteFile(name, corrupt, 0o600))
	err = parseArchiveFile(newParser(), name, 4, func(*replication.BinlogEvent) error { return nil })
	require.ErrorContains(t, err, "invalid binlog file header")
}

func TestArchiveRotationAndRestart(t *testing.T) {
	dir := t.TempDir()
	start, _ := archiveFile(t, dir, "mysql-bin.000001", "mysql-bin.000002")
	_, end := archiveFile(t, dir, "mysql-bin.000002", "")
	c := NewArchiveController(dir, mysql.Position{Name: "mysql-bin.000002", Pos: end}, replication.BinlogSyncerConfig{Flavor: "mysql"}, log.L())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	tctx := tcontext.NewContext(ctx, log.L())
	loc := binlog.MustZeroLocation("mysql")
	loc.Position = mysql.Position{Name: "mysql-bin.000001", Pos: start}
	for range 2 {
		require.NoError(t, c.Start(tctx, loc))
		var queries int
		for {
			ev, _, err := c.GetEvent(tctx)
			if err != nil {
				require.ErrorIs(t, err, ErrArchiveComplete)
				break
			}
			if ev.Header.EventType == replication.QUERY_EVENT {
				queries++
			}
		}
		require.Equal(t, 2, queries)
		require.Equal(t, mysql.Position{Name: "mysql-bin.000002", Pos: end}, c.GetTxnEndLocation().Position)
		require.False(t, c.CanRetry(ErrArchiveComplete))
		c.Close()
	}
}

func TestArchiveFailures(t *testing.T) {
	for _, tc := range []struct {
		name, next string
		delta      int
		message    string
	}{
		{"early EOF", "", 100, "ended before"},
		{"split event", "", -1, "splits an event"},
		{"missing file", "mysql-bin.000002", 100, "no such file"},
		{"rotation traversal", "../mysql-bin.000002", 100, "invalid local binlog rotation"},
		{"rotation gap", "mysql-bin.000003", 100, "invalid local binlog rotation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			_, end := archiveFile(t, dir, "mysql-bin.000001", tc.next)
			target := mysql.Position{Name: "mysql-bin.000001", Pos: uint32(int(end) + tc.delta)}
			if tc.next != "" {
				target = mysql.Position{Name: "mysql-bin.000002", Pos: 1000}
			}
			r := &archiveReader{directory: dir, end: target, cfg: replication.BinlogSyncerConfig{Flavor: "mysql"}}
			loc := binlog.MustZeroLocation("mysql")
			loc.Position = mysql.Position{Name: "mysql-bin.000001", Pos: 4}
			_, err := r.GenerateStreamFrom(loc)
			require.NoError(t, err)
			defer r.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			for {
				_, err = r.GetEvent(ctx)
				if err != nil {
					require.ErrorContains(t, err, tc.message)
					break
				}
			}
		})
	}
}

func TestArchiveCloseAndCancellation(t *testing.T) {
	dir := t.TempDir()
	_, end := archiveFile(t, dir, "mysql-bin.000001", "")
	r := &archiveReader{directory: dir, end: mysql.Position{Name: "mysql-bin.000001", Pos: end}, cfg: replication.BinlogSyncerConfig{Flavor: "mysql"}}
	loc := binlog.MustZeroLocation("mysql")
	loc.Position = mysql.Position{Name: "../mysql-bin.000001", Pos: 4}
	_, err := r.GenerateStreamFrom(loc)
	require.Error(t, err)
	loc.Position.Name = "mysql-bin.000001"
	_, err = r.GenerateStreamFrom(loc)
	require.NoError(t, err)
	r.Close()
	r.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// A canceled receiver is guaranteed to stop even if the producer has no data.
	r.results = make(chan archiveResult)
	_, err = r.GetEvent(ctx)
	require.ErrorIs(t, err, context.Canceled)
}
