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

package conn

import (
	"context"
	"database/sql/driver"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/stretchr/testify/require"
)

func TestBlackholeDriver(t *testing.T) {
	sink := &Blackhole{}
	db := NewBlackholeDB(sink)
	defer db.Close()
	ctx := context.Background()
	base, err := db.GetBaseConn(ctx)
	require.NoError(t, err)
	// Exercise the real BaseConn transaction and auto-split entry points, not a
	// special syncer shortcut. Include types used in wide/type-rich binlogs.
	args := []interface{}{uint64(math.MaxUint64), int32(3), []byte("bytes"), nil, time.Unix(1, 0), "text"}
	n, err := base.ExecuteSQL(tcontext.Background(), nil, "test", []string{"INSERT INTO t VALUES (?,?,?,?,?,?)", "ALTER TABLE t ADD COLUMN v INT"}, args)
	require.NoError(t, err)
	require.Zero(t, n) // blackhole cannot know the number of affected rows
	require.Equal(t, BlackholeStats{Statements: 2, Parameters: 6, Commits: 1}, sink.Stats())
	require.NoError(t, base.ExecuteSQLsAutoSplit(tcontext.Background(), nil, "test", []string{"UPDATE t SET v=?"}, []interface{}{1}))
	require.Equal(t, uint64(2), sink.Stats().Commits)
	rows, err := base.QuerySQL(tcontext.Background(), "SELECT VERSION()")
	require.ErrorContains(t, err, "cannot read database state")
	require.Nil(t, rows)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = db.DB.ExecContext(canceled, "INSERT INTO t VALUES (1)")
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, uint64(3), sink.Stats().Statements)
	tx, err := db.DB.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, "DELETE FROM t")
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	require.Equal(t, uint64(1), sink.Stats().Rollbacks)
	require.NoError(t, db.ForceCloseConn(base))
	base, err = db.GetBaseConn(ctx)
	require.NoError(t, err)
	_, err = base.ExecuteSQL(tcontext.Background(), nil, "test", []string{"SET SESSION foreign_key_checks=0"})
	require.NoError(t, err)
	require.NoError(t, db.Close())
	require.Equal(t, uint64(5), sink.Stats().Statements)
}

func TestBlackholeConcurrentConnections(t *testing.T) {
	sink := &Blackhole{}
	db := NewBlackholeDB(sink)
	defer db.Close()
	const workers, batches = 4, 20
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			base, err := db.GetBaseConn(context.Background())
			if err != nil {
				errs <- err
				return
			}
			defer db.CloseConnWithoutErr(base)
			for j := 0; j < batches; j++ {
				if _, err := base.ExecuteSQL(tcontext.Background(), nil, "test", []string{"INSERT INTO t VALUES (?)"}, []interface{}{j}); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, BlackholeStats{Statements: workers * batches, Parameters: workers * batches, Commits: workers * batches}, sink.Stats())
}

func TestBlackholeLatency(t *testing.T) {
	const latency = time.Millisecond
	sink := NewBlackhole(latency)
	db := NewBlackholeDB(sink)
	defer db.Close()
	ctx := context.Background()
	// Force connection reuse to verify transaction state is cleared on both paths.
	db.DB.SetMaxOpenConns(1)
	tx, err := db.DB.BeginTx(ctx, nil)
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		_, err = tx.ExecContext(ctx, "INSERT INTO t VALUES (?)", i)
		require.NoError(t, err)
	}
	require.Zero(t, sink.Stats().DelayCount)
	require.NoError(t, tx.Commit())
	require.Equal(t, uint64(1), sink.Stats().DelayCount)
	require.GreaterOrEqual(t, sink.Stats().DelayNanoseconds, uint64(latency))
	_, err = db.DB.ExecContext(ctx, "ALTER TABLE t ADD COLUMN v INT")
	require.NoError(t, err)
	require.Equal(t, uint64(2), sink.Stats().DelayCount)
	tx, err = db.DB.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	require.Equal(t, uint64(2), sink.Stats().DelayCount)
	_, err = db.DB.ExecContext(ctx, "DELETE FROM t")
	require.NoError(t, err)
	stats := sink.Stats()
	require.Equal(t, uint64(3), stats.DelayCount)
	require.Equal(t, uint64(1), stats.Commits)
	require.Equal(t, uint64(1), stats.Rollbacks)
	require.Equal(t, uint64(5), stats.Statements)
}

func TestBlackholeLatencyCancellation(t *testing.T) {
	// Test the driver directly so database/sql cannot short-circuit cancellation.
	for _, transaction := range []bool{false, true} {
		t.Run(fmt.Sprint(transaction), func(t *testing.T) {
			sink := NewBlackhole(time.Hour)
			c := &blackholeConn{sink: sink}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()
			if transaction {
				tx, err := c.BeginTx(ctx, driver.TxOptions{})
				require.NoError(t, err)
				_, err = c.ExecContext(ctx, "INSERT INTO t VALUES (1)", nil)
				require.NoError(t, err)
				require.ErrorIs(t, tx.Commit(), context.DeadlineExceeded)
				require.False(t, c.inTransaction)
			} else {
				_, err := c.ExecContext(ctx, "INSERT INTO t VALUES (1)", nil)
				require.ErrorIs(t, err, context.DeadlineExceeded)
			}
			require.Zero(t, sink.Stats().Commits)
			require.Equal(t, uint64(1), sink.Stats().DelayCount)
		})
	}
}

func TestBlackholeLatencyConnectionsAreIndependent(t *testing.T) {
	// A blocked connection must not hold a shared sink lock while waiting.
	sink := NewBlackhole(time.Hour)
	slow := &blackholeConn{sink: sink}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tx, err := slow.BeginTx(ctx, driver.TxOptions{})
	require.NoError(t, err)
	finished := make(chan error, 1)
	go func() { finished <- tx.Commit() }()
	require.Eventually(t, func() bool { return sink.Stats().DelayCount == 1 }, time.Second, time.Millisecond)
	other := &blackholeConn{sink: sink}
	otherTx, err := other.BeginTx(context.Background(), driver.TxOptions{})
	require.NoError(t, err)
	_, err = other.ExecContext(context.Background(), "INSERT INTO t VALUES (1)", nil)
	require.NoError(t, err)
	require.NoError(t, otherTx.Rollback())
	cancel()
	require.ErrorIs(t, <-finished, context.Canceled)
}
