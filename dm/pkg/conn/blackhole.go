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
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync/atomic"
	"time"

	"github.com/pingcap/tiflow/dm/pkg/terror"
)

// Blackhole is a write-only SQL execution target for offline performance runs.
// It retains no SQL or data, and performs no network or disk I/O. Using it below
// database/sql preserves DM's connection, transaction, retry and metrics paths.
// It does not simulate server execution, affected rows, constraints or durability.
type Blackhole struct {
	latency          time.Duration // immutable; one wait per transaction or autocommit statement
	delayCount       atomic.Uint64
	delayNanoseconds atomic.Uint64
	statements       atomic.Uint64
	parameters       atomic.Uint64
	commits          atomic.Uint64
	rollbacks        atomic.Uint64
}

// BlackholeStats counts submitted statements/parameters and transaction outcomes,
// not source rows or source transactions. Statements may belong to rolled-back
// transactions. Counters remain available after closing connections.
type BlackholeStats struct {
	DelayCount       uint64
	DelayNanoseconds uint64
	Statements       uint64
	Parameters       uint64
	Commits          uint64
	Rollbacks        uint64
}

// NewBlackhole creates a write-only target with an optional acknowledgement
// latency. Callers must validate that latency is nonnegative. Zero preserves the
// no-delay behavior. This models batch response time, not per-statement round
// trips or a database's CPU, bandwidth, locking or durability limits.
func NewBlackhole(latency time.Duration) *Blackhole {
	return &Blackhole{latency: latency}
}

// wait applies latency only at the execution boundary. Connections wait
// independently; there is no global lock or shared throughput limiter. The
// measured duration includes timer scheduling and cancellation, not SQL work.
func (b *Blackhole) wait(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.latency <= 0 {
		return nil
	}
	started := time.Now()
	b.delayCount.Add(1)
	defer func() { b.delayNanoseconds.Add(uint64(time.Since(started))) }()
	timer := time.NewTimer(b.latency)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}

// Stats returns independently sampled counters. DelayCount includes canceled
// waits; DelayNanoseconds is their actual aggregate duration across connections.
// These are not wall time or source transaction counts.
func (b *Blackhole) Stats() BlackholeStats {
	return BlackholeStats{Statements: b.statements.Load(), Parameters: b.parameters.Load(), Commits: b.commits.Load(), Rollbacks: b.rollbacks.Load(), DelayCount: b.delayCount.Load(), DelayNanoseconds: b.delayNanoseconds.Load()}
}

// NewBlackholeDB opens a process-local, write-only target. No global SQL driver
// registration or default DB provider is changed. Reads fail explicitly rather
// than inventing metadata; offline callers must supply metadata themselves.
func NewBlackholeDB(b *Blackhole) *BaseDB {
	return NewBaseDB(sql.OpenDB(&blackholeConnector{sink: b}), terror.ScopeDownstream, "")
}

type blackholeConnector struct{ sink *Blackhole }

func (c *blackholeConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &blackholeConn{sink: c.sink}, nil
}
func (c *blackholeConnector) Driver() driver.Driver { return blackholeDriver{} }

type blackholeDriver struct{}

func (blackholeDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("blackhole requires an explicit connector")
}

type blackholeConn struct {
	sink          *Blackhole
	inTransaction bool // database/sql serializes calls on a driver connection
}

func (*blackholeConn) Close() error { return nil }
func (*blackholeConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("blackhole does not support prepared statements")
}

func (c *blackholeConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *blackholeConn) BeginTx(ctx context.Context, _ driver.TxOptions) (driver.Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.inTransaction {
		return nil, errors.New("blackhole transaction already active")
	}
	c.inTransaction = true
	return &blackholeTx{conn: c, ctx: ctx}, nil
}
func (*blackholeConn) Ping(ctx context.Context) error { return ctx.Err() }

// MySQL accepts uint64 even with its high bit set; database/sql's default
// converter does not. All other values use database/sql's normal conversion.
func (*blackholeConn) CheckNamedValue(v *driver.NamedValue) error {
	if _, ok := v.Value.(uint64); ok {
		return nil
	}
	return driver.ErrSkip
}

func (c *blackholeConn) ExecContext(ctx context.Context, _ string, args []driver.NamedValue) (driver.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !c.inTransaction {
		if err := c.sink.wait(ctx); err != nil {
			return nil, err
		}
	}
	c.sink.statements.Add(1)
	c.sink.parameters.Add(uint64(len(args)))
	return driver.RowsAffected(0), nil // unknown, never pretend these are source rows
}

func (*blackholeConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return nil, errors.New("blackhole cannot read database state; supply offline metadata")
}

type blackholeTx struct {
	conn *blackholeConn
	ctx  context.Context
}

func (t *blackholeTx) Commit() error {
	defer func() { t.conn.inTransaction = false }()
	if err := t.conn.sink.wait(t.ctx); err != nil {
		return err
	}
	t.conn.sink.commits.Add(1)
	return nil
}
func (t *blackholeTx) Rollback() error {
	t.conn.inTransaction = false
	t.conn.sink.rollbacks.Add(1)
	return nil
}

var (
	_ driver.Connector         = (*blackholeConnector)(nil)
	_ driver.Conn              = (*blackholeConn)(nil)
	_ driver.ConnBeginTx       = (*blackholeConn)(nil)
	_ driver.ExecerContext     = (*blackholeConn)(nil)
	_ driver.QueryerContext    = (*blackholeConn)(nil)
	_ driver.NamedValueChecker = (*blackholeConn)(nil)
	_ driver.Pinger            = (*blackholeConn)(nil)
)
