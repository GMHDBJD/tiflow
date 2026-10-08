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
	"os"
	"path/filepath"
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
