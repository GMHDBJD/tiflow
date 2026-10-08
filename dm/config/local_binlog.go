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

package config

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	pmysql "github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

// LocalBinlogConfig describes an immutable, finite MySQL binlog archive.
// Paths are resolved on the worker, not on the master. This source is only
// usable with an explicitly selected blackhole target.
type LocalBinlogConfig struct {
	Directory           string   `yaml:"directory" toml:"directory" json:"directory"`
	SchemaFiles         []string `yaml:"schema-files" toml:"schema-files" json:"schema-files"`
	End                 Meta     `yaml:"end" toml:"end" json:"end"`
	Timezone            string   `yaml:"timezone" toml:"timezone" json:"timezone"`
	SQLMode             string   `yaml:"sql-mode" toml:"sql-mode" json:"sql-mode"`
	LowerCaseTableNames int      `yaml:"lower-case-table-names" toml:"lower-case-table-names" json:"lower-case-table-names"`
}

// Verify checks portable metadata without accessing worker-local files.
func (c *LocalBinlogConfig) Verify() error {
	if !filepath.IsAbs(c.Directory) {
		return fmt.Errorf("local-binlog.directory must be absolute")
	}
	for _, path := range c.SchemaFiles {
		if !filepath.IsAbs(path) {
			return fmt.Errorf("local-binlog.schema-files must contain absolute paths")
		}
	}
	if !utils.VerifyFilename(c.End.BinLogName) || filepath.Base(c.End.BinLogName) != c.End.BinLogName || c.End.BinLogPos < 4 || c.End.BinLogGTID != "" {
		return fmt.Errorf("local-binlog.end requires a binlog filename and position >= 4, without GTID")
	}
	if c.Timezone == "" || c.Timezone == "Local" {
		return fmt.Errorf("local-binlog.timezone must be explicit, for example UTC")
	}
	if _, err := time.LoadLocation(c.Timezone); err != nil {
		return fmt.Errorf("local-binlog.timezone: %w", err)
	}
	if _, err := pmysql.GetSQLMode(c.SQLMode); err != nil {
		return fmt.Errorf("local-binlog.sql-mode: %w", err)
	}
	if c.LowerCaseTableNames < 0 || c.LowerCaseTableNames > 2 {
		return fmt.Errorf("local-binlog.lower-case-table-names must be 0, 1 or 2")
	}
	return nil
}

// EndPosition is the exclusive archive end, at a transaction boundary.
func (c *LocalBinlogConfig) EndPosition() mysql.Position {
	return mysql.Position{Name: c.End.BinLogName, Pos: c.End.BinLogPos}
}

// VerifyLocalBinlog checks the source/target pair after source settings have
// been copied into a subtask. Keep this check on both master and worker.
func (c *SubTaskConfig) VerifyLocalBinlog() error {
	if c.LocalBinlog == nil && !c.Blackhole {
		return nil
	}
	if c.LocalBinlog == nil || !c.Blackhole {
		return fmt.Errorf("local-binlog and blackhole must be enabled together")
	}
	if err := c.LocalBinlog.Verify(); err != nil {
		return err
	}
	if c.Mode != ModeIncrement || c.ShardMode != "" || c.IsSharding || c.OnlineDDL || c.EnableGTID || c.UseRelay || c.ValidatorCfg.Mode != ValidationNone {
		return fmt.Errorf("local-binlog requires incremental, non-sharded replication without online DDL, GTID positioning, relay or validator")
	}
	if c.CollationCompatible == StrictCollationCompatible {
		return fmt.Errorf("local-binlog does not support strict collation discovery")
	}
	if c.Meta == nil || !utils.VerifyFilename(c.Meta.BinLogName) || filepath.Base(c.Meta.BinLogName) != c.Meta.BinLogName || c.Meta.BinLogPos < 4 || c.Meta.BinLogGTID != "" {
		return fmt.Errorf("local-binlog requires an explicit start filename/position in meta")
	}
	start := mysql.Position{Name: c.Meta.BinLogName, Pos: c.Meta.BinLogPos}
	startFile, err := utils.ParseFilename(start.Name)
	if err != nil {
		return err
	}
	endFile, err := utils.ParseFilename(c.LocalBinlog.End.BinLogName)
	if err != nil {
		return err
	}
	if startFile.BaseName != endFile.BaseName {
		return fmt.Errorf("local-binlog start and end must use the same filename prefix")
	}
	if c.Flavor != "" && c.Flavor != "mysql" {
		return fmt.Errorf("local-binlog supports MySQL archives only")
	}
	if start.Compare(c.LocalBinlog.EndPosition()) >= 0 {
		return fmt.Errorf("local-binlog start must precede end")
	}
	return nil
}
