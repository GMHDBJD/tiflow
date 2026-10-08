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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/binlog/event"
	"github.com/stretchr/testify/require"
)

func localTestConfig(t *testing.T, initialSchema bool) *config.SubTaskConfig {
	t.Helper()
	g, err := event.NewGeneratorV2("mysql", "8.0.46", "3ccc475b-2343-11e7-be21-6c0b84d59f30:1", true)
	require.NoError(t, err)
	_, data, err := g.GenFileHeader(1700000000)
	require.NoError(t, err)
	dir := t.TempDir()
	ddl := "CREATE TABLE t(id INT PRIMARY KEY, v INT NOT NULL)"
	var schemaFiles []string
	if initialSchema {
		path := filepath.Join(dir, "schema.sql")
		require.NoError(t, os.WriteFile(path, []byte("CREATE DATABASE test; USE test; "+ddl+";"), 0o600))
		schemaFiles = []string{path}
	} else {
		_, buf, err := g.GenCreateDatabaseEvents("test")
		require.NoError(t, err)
		data = append(data, buf...)
		_, buf, err = g.GenCreateTableEvents("test", ddl)
		require.NoError(t, err)
		data = append(data, buf...)
	}
	for _, tc := range []struct {
		tp   replication.EventType
		rows [][]interface{}
	}{
		{replication.WRITE_ROWS_EVENTv2, [][]interface{}{{int32(1), int32(2)}}},
		{replication.UPDATE_ROWS_EVENTv2, [][]interface{}{{int32(1), int32(2)}, {int32(1), int32(3)}}},
		{replication.DELETE_ROWS_EVENTv2, [][]interface{}{{int32(1), int32(3)}}},
	} {
		_, buf, err := g.GenDMLEvents(tc.tp, []*event.DMLData{{TableID: 1, Schema: "test", Table: "t", ColumnType: []byte{mysql.TypeLong, mysql.TypeLong}, Rows: tc.rows}}, 1700000000)
		require.NoError(t, err)
		data = append(data, buf...)
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, "mysql-bin.000001"), data, 0o600))
	cfg := config.NewSubTaskConfig()
	cfg.Name = "local-test"
	cfg.SourceID = "local-source"
	cfg.Mode = config.ModeIncrement
	cfg.Flavor = "mysql"
	cfg.Blackhole = true
	cfg.OnlineDDL = false
	cfg.CaseSensitive = true
	cfg.SyncerConfig = config.DefaultSyncerConfig()
	cfg.SafeModeDuration = "0s"
	cfg.Meta = &config.Meta{BinLogName: "mysql-bin.000001", BinLogPos: 4}
	cfg.LocalBinlog = &config.LocalBinlogConfig{Directory: dir, SchemaFiles: schemaFiles, End: config.Meta{BinLogName: "mysql-bin.000001", BinLogPos: g.LatestPos}, Timezone: "UTC"}
	require.NoError(t, cfg.Adjust(false))
	return cfg
}

func TestLocalBinlogProcess(t *testing.T) {
	for _, initialSchema := range []bool{false, true} {
		t.Run(map[bool]string{false: "schema-in-binlog", true: "initial-schema"}[initialSchema], func(t *testing.T) {
			cfg := localTestConfig(t, initialSchema)
			s := NewSyncer(cfg, nil, nil)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
			defer cancel()
			require.NoError(t, s.Init(ctx))
			defer s.Close()
			result := make(chan pb.ProcessResult, 1)
			go s.Process(ctx, result)
			select {
			case r := <-result:
				require.Empty(t, r.Errors)
				require.False(t, r.IsCanceled)
			case <-ctx.Done():
				t.Fatal("finite replay did not complete")
			}
			require.Equal(t, cfg.LocalBinlog.EndPosition(), s.checkpoint.FlushedGlobalPoint().Position)
			statementsBeforeResume := s.blackhole.Stats().Statements
			require.GreaterOrEqual(t, s.blackhole.Stats().Statements, uint64(3))
			require.GreaterOrEqual(t, s.blackhole.Stats().Parameters, uint64(6))
			// A second Process on the same instance resumes at end, not from meta.
			s.Resume(ctx, result)
			r := <-result
			require.Empty(t, r.Errors)
			require.False(t, r.IsCanceled)
			require.Equal(t, statementsBeforeResume, s.blackhole.Stats().Statements)
		})
	}
}

func TestLocalBinlogMissingSchema(t *testing.T) {
	cfg := localTestConfig(t, true)
	cfg.LocalBinlog.SchemaFiles = nil
	s := NewSyncer(cfg, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	require.NoError(t, s.Init(ctx))
	defer s.Close()
	result := make(chan pb.ProcessResult, 1)
	s.Process(ctx, result)
	r := <-result
	require.NotEmpty(t, r.Errors)
	require.Contains(t, r.Errors[0].Message, "local schema missing")
}

func TestLocalBinlogEndInsideTransaction(t *testing.T) {
	cfg := localTestConfig(t, true)
	parser := replication.NewBinlogParser()
	var end uint32
	require.NoError(t, parser.ParseFile(filepath.Join(cfg.LocalBinlog.Directory, cfg.Meta.BinLogName), 4, func(ev *replication.BinlogEvent) error {
		if ev.Header.EventType == replication.WRITE_ROWS_EVENTv2 {
			end = ev.Header.LogPos
		}
		return nil
	}))
	require.NotZero(t, end)
	cfg.LocalBinlog.End.BinLogPos = end
	s := NewSyncer(cfg, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, s.Init(ctx))
	defer s.Close()
	result := make(chan pb.ProcessResult, 1)
	s.Process(ctx, result)
	r := <-result
	require.NotEmpty(t, r.Errors)
	require.Contains(t, r.Errors[0].Message, "transaction boundary")
}
