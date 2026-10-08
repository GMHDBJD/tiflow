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

	"github.com/pingcap/tiflow/dm/pkg/terror"
)

// Blackhole is a write-only SQL execution target for offline performance runs.
// It retains no SQL or data, and performs no network or disk I/O. Using it below
// database/sql preserves DM's connection, transaction, retry and metrics paths.
// It does not simulate server execution, affected rows, constraints or durability.
type Blackhole struct {
	statements atomic.Uint64
	parameters atomic.Uint64
	commits    atomic.Uint64
	rollbacks  atomic.Uint64
}

// BlackholeStats counts submitted statements/parameters and transaction outcomes,
// not source rows or source transactions. Statements may belong to rolled-back
// transactions. Counters remain available after closing connections.
type BlackholeStats struct {
	Statements uint64
	Parameters uint64
	Commits    uint64
	Rollbacks  uint64
}

// Stats returns independently sampled counters.
func (b *Blackhole) Stats() BlackholeStats {
	return BlackholeStats{b.statements.Load(), b.parameters.Load(), b.commits.Load(), b.rollbacks.Load()}
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

type blackholeConn struct{ sink *Blackhole }

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
	return &blackholeTx{sink: c.sink}, nil
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
	c.sink.statements.Add(1)
	c.sink.parameters.Add(uint64(len(args)))
	return driver.RowsAffected(0), nil // unknown, never pretend these are source rows
}

func (*blackholeConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return nil, errors.New("blackhole cannot read database state; supply offline metadata")
}

type blackholeTx struct{ sink *Blackhole }

func (t *blackholeTx) Commit() error   { t.sink.commits.Add(1); return nil }
func (t *blackholeTx) Rollback() error { t.sink.rollbacks.Add(1); return nil }

var (
	_ driver.Connector         = (*blackholeConnector)(nil)
	_ driver.Conn              = (*blackholeConn)(nil)
	_ driver.ConnBeginTx       = (*blackholeConn)(nil)
	_ driver.ExecerContext     = (*blackholeConn)(nil)
	_ driver.QueryerContext    = (*blackholeConn)(nil)
	_ driver.NamedValueChecker = (*blackholeConn)(nil)
	_ driver.Pinger            = (*blackholeConn)(nil)
)
