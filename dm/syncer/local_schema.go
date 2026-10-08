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
	"fmt"
	"os"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/filter"
)

// loadLocalSchema imports only schema declarations, never seed rows. All table
// infos are checkpointed, including tables untouched by the first transaction,
// so pause/resume restores the schema at the flushed position rather than
// resetting changed tables to their initial definitions.
func (s *Syncer) loadLocalSchema(ctx context.Context) error {
	p := parser.New()
	mode, err := mysql.GetSQLMode(s.cfg.LocalBinlog.SQLMode)
	if err != nil {
		return err
	}
	p.SetSQLMode(mode)
	var tables []*filter.Table
	var infos []*model.TableInfo
	for _, path := range s.cfg.LocalBinlog.SchemaFiles {
		contents, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read local schema %s: %w", path, err)
		}
		nodes, _, err := p.ParseSQL(string(contents))
		if err != nil {
			return fmt.Errorf("parse local schema %s: %w", path, err)
		}
		database := ""
		for _, node := range nodes {
			if err = ctx.Err(); err != nil {
				return err
			}
			switch stmt := node.(type) {
			case *ast.UseStmt:
				database = stmt.DBName
				continue
			case *ast.CreateDatabaseStmt:
			case *ast.CreateTableStmt:
				if stmt.Table.Schema.O == "" && database == "" {
					return fmt.Errorf("unqualified table in local schema %s", path)
				}
			default:
				return fmt.Errorf("local schema %s contains non-schema statement %T; supply CREATE DATABASE, USE and CREATE TABLE only", path, node)
			}
			if err = s.schemaTracker.Exec(ctx, database, node); err != nil {
				return fmt.Errorf("load local schema %s: %w", path, err)
			}
			if stmt, ok := node.(*ast.CreateTableStmt); ok {
				db := stmt.Table.Schema.O
				if db == "" {
					db = database
				}
				table := &filter.Table{Schema: db, Name: stmt.Table.Name.O}
				info, err := s.schemaTracker.GetTableInfo(table)
				if err != nil {
					return err
				}
				tables = append(tables, table)
				infos = append(infos, info)
			}
		}
	}
	return s.checkpoint.FlushPointsWithTableInfos(s.tctx, tables, infos)
}
