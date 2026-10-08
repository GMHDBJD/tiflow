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

package syncer

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestBlackholeCheckpointLifecycle(t *testing.T) {
	cfg := localTestConfig(t, true)
	ctx := tcontext.Background()
	cp := NewRemoteCheckPoint(ctx, cfg, nil, cfg.SourceID).(*RemoteCheckPoint)
	require.NotNil(t, cp.blackhole)
	require.NoError(t, cp.Init(ctx))
	defer cp.Close()
	require.NoError(t, cp.LoadMeta(context.Background()))
	require.NoError(t, cp.Load(ctx))
	require.Equal(t, uint64(2), cp.blackhole.Stats().Statements) // CREATE schema/table
	table := &filter.Table{Schema: "test", Name: "t"}
	ti := &model.TableInfo{ID: 42, Name: ast.NewCIStr("t")}
	require.NoError(t, cp.FlushPointsWithTableInfos(ctx, []*filter.Table{table}, []*model.TableInfo{ti}))
	start := cp.GlobalPoint()
	a := start.Clone()
	a.Position.Pos += 100
	cp.SaveTablePoint(table, a, ti)
	cp.SaveGlobalPoint(a)
	snapshot := cp.Snapshot(true)
	require.NotNil(t, snapshot)
	// An invalid/out-of-order request must not consume a snapshot or retain its lock.
	require.ErrorContains(t, cp.FlushPointsExcept(ctx, snapshot.id+1, nil, nil, nil), "not found")
	b := a.Clone()
	b.Position.Pos += 100
	cp.SaveTablePoint(table, b, ti)
	cp.SaveGlobalPoint(b)
	require.NoError(t, cp.FlushPointsExcept(ctx, snapshot.id, nil, nil, nil))
	require.Equal(t, a.Position, cp.FlushedGlobalPoint().Position)
	require.Equal(t, uint64(5), cp.blackhole.Stats().Statements) // init + schema seed + table/global flush
	require.Equal(t, uint64(33), cp.blackhole.Stats().Parameters)
	cp.Rollback()
	require.Equal(t, a.Position, cp.GlobalPoint().Position)
	require.Equal(t, a.Position, cp.TablePoint()["test"]["t"].Position)
	cp.SaveSafeModeExitPoint(&b)
	require.NoError(t, cp.FlushSafeModeExitPoint(ctx))
	require.False(t, cp.needFlushSafeModeExitPoint.Load())
	require.Equal(t, a.Position, cp.FlushedGlobalPoint().Position)
	require.NoError(t, cp.ResetConn(ctx))
	require.NoError(t, cp.Load(ctx))
	require.Equal(t, a.Position, cp.GlobalPoint().Position)
	require.NoError(t, cp.DeleteTablePoint(ctx, table))
	require.Nil(t, cp.GetTableInfo("test", "t"))
	require.NoError(t, cp.Clear(ctx))
	require.Empty(t, cp.TablePoint())
	// No persistence across new instances, even with identical source/task names.
	fresh := NewRemoteCheckPoint(ctx, cfg, nil, cfg.SourceID).(*RemoteCheckPoint)
	require.NoError(t, fresh.Init(ctx))
	defer fresh.Close()
	require.NoError(t, fresh.LoadMeta(context.Background()))
	require.NoError(t, fresh.Load(ctx))
	require.Equal(t, start.Position, fresh.GlobalPoint().Position)
}

// Assert both modes execute identical SQL and serialized snapshot contents at
// the driver boundary. A later Save must not leak into the pending snapshot.
func TestBlackholeCheckpointSQLParity(t *testing.T) {
	for _, offline := range []bool{false, true} {
		t.Run(fmt.Sprint(offline), func(t *testing.T) {
			cfg := localTestConfig(t, true)
			cfg.Blackhole = offline
			ctx := tcontext.Background()
			cp := NewRemoteCheckPoint(ctx, cfg, nil, cfg.SourceID).(*RemoteCheckPoint)
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			raw, err := db.Conn(context.Background())
			require.NoError(t, err)
			defer raw.Close()
			cp.dbConn = dbconn.NewDBConn(cfg, conn.NewBaseConnForTest(raw, nil))
			require.NoError(t, cp.LoadMeta(context.Background()))
			table := &filter.Table{Schema: "test", Name: "t"}
			ti := &model.TableInfo{ID: 42, Name: ast.NewCIStr("t")}
			serialized, err := json.Marshal(ti)
			require.NoError(t, err)
			first := cp.GlobalPoint()
			first.Position.Pos += 100
			cp.SaveTablePoint(table, first, ti)
			cp.SaveGlobalPoint(first)
			snapshot := cp.Snapshot(true)
			next := first.Clone()
			next.Position.Pos += 100
			cp.SaveTablePoint(table, next, &model.TableInfo{ID: 43})
			cp.SaveGlobalPoint(next)
			mock.ExpectBegin()
			mock.ExpectExec("INSERT INTO").WithArgs(cfg.SourceID, "test", "t", first.Position.Name, first.Position.Pos, "", "", 0, "", string(serialized), false).WillReturnResult(sqlmock.NewResult(0, 1))
			// Global point must be LAST, after every table point.
			mock.ExpectExec("INSERT INTO").WithArgs(cfg.SourceID, "", "", first.Position.Name, first.Position.Pos, "", "", 0, "", "null", true).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectCommit()
			require.NoError(t, cp.FlushPointsExcept(ctx, snapshot.id, nil, nil, nil))
			require.Equal(t, first.Position, cp.FlushedGlobalPoint().Position)
			require.Equal(t, next.Position, cp.GlobalPoint().Position)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestBlackholeCheckpointWaitsForDMLBarrier(t *testing.T) {
	cfg := localTestConfig(t, true)
	ctx := tcontext.Background()
	cp := NewRemoteCheckPoint(ctx, cfg, nil, cfg.SourceID).(*RemoteCheckPoint)
	require.NoError(t, cp.Init(ctx))
	defer cp.Close()
	require.NoError(t, cp.LoadMeta(context.Background()))
	next := cp.GlobalPoint()
	next.Position.Pos += 100
	cp.SaveGlobalPoint(next)
	before := cp.blackhole.Stats()
	snapshot := cp.Snapshot(false)
	flushJob := newAsyncFlushJob(2, 1)
	done := make(chan struct{}, 1)
	worker := &checkpointFlushWorker{
		input: make(chan *checkpointFlushTask), cp: cp, execError: &atomic.Error{},
		afterFlushFn:       func(*checkpointFlushTask) error { done <- struct{}{}; return nil },
		updateJobMetricsFn: func(bool, string, *job) {},
	}
	stopped := make(chan struct{})
	go func() { defer close(stopped); worker.Run(ctx) }()
	worker.Add(&checkpointFlushTask{snapshotInfo: snapshot, asyncflushJob: flushJob})
	require.Equal(t, before, cp.blackhole.Stats())
	flushJob.flushWg.Done()
	select {
	case <-done:
		t.Fatal("checkpoint crossed an incomplete DML barrier")
	case <-time.After(20 * time.Millisecond):
	}
	require.Equal(t, before, cp.blackhole.Stats())
	flushJob.flushWg.Done()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("checkpoint did not flush after DML barrier")
	}
	worker.Close()
	<-stopped
	require.Equal(t, before.Statements+1, cp.blackhole.Stats().Statements)
	require.Equal(t, next.Position, cp.FlushedGlobalPoint().Position)
}
