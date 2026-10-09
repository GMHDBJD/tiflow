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
// Paths are resolved on the worker, not on the master. The target is selected
// independently: a real database or an explicitly enabled blackhole.
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

// BinlogReplayConfig bounds a replication run independently of its input and output.
// SchemaFiles describe the schema at the task's start position, not today's
// upstream schema. Paths are resolved on the worker.
type BinlogReplayConfig struct {
	SchemaFiles []string `yaml:"schema-files,omitempty" toml:"schema-files,omitempty" json:"schema-files,omitempty"`
	End         Meta     `yaml:"end" toml:"end" json:"end"`
}

// Verify checks a finite replay's portable metadata.
func (c *BinlogReplayConfig) Verify() error {
	for _, path := range c.SchemaFiles {
		if !filepath.IsAbs(path) {
			return fmt.Errorf("binlog-replay.schema-files must contain absolute paths")
		}
	}
	return verifyReplayPosition(&c.End)
}

func verifyReplayPosition(m *Meta) error {
	if m == nil || !utils.VerifyFilename(m.BinLogName) || filepath.Base(m.BinLogName) != m.BinLogName || m.BinLogPos < 4 || m.BinLogGTID != "" {
		return fmt.Errorf("binlog replay requires an explicit filename/position >= 4, without GTID")
	}
	return nil
}

// ReplayEnd returns the exclusive finite endpoint, or nil for live replication.
func (c *SubTaskConfig) ReplayEnd() *mysql.Position {
	if c.LocalBinlog != nil {
		p := c.LocalBinlog.EndPosition()
		return &p
	}
	if c.BinlogReplay != nil {
		return &mysql.Position{Name: c.BinlogReplay.End.BinLogName, Pos: c.BinlogReplay.End.BinLogPos}
	}
	return nil
}

// ReplaySchemaFiles returns initial schema declarations for either transport.
func (c *SubTaskConfig) ReplaySchemaFiles() []string {
	if c.LocalBinlog != nil {
		return c.LocalBinlog.SchemaFiles
	}
	if c.BinlogReplay != nil {
		return c.BinlogReplay.SchemaFiles
	}
	return nil
}

// VerifySyncerMode checks opt-in input, replay and output settings after source
// configuration is copied into a subtask. Neither transport implies blackhole.
func (c *SubTaskConfig) VerifySyncerMode() error {
	if c.LocalBinlog == nil && c.BinlogReplay == nil && !c.Blackhole {
		return nil
	}
	if c.Mode != ModeIncrement || c.ShardMode != "" || c.IsSharding || c.OnlineDDL || c.ValidatorCfg.Mode != ValidationNone {
		return fmt.Errorf("binlog replay and blackhole require incremental, non-sharded replication without online DDL or validator")
	}
	if c.LocalBinlog != nil && c.BinlogReplay != nil {
		return fmt.Errorf("specify replay metadata in either local-binlog or binlog-replay, not both")
	}
	if c.LocalBinlog != nil {
		if err := c.LocalBinlog.Verify(); err != nil {
			return err
		}
		if c.CollationCompatible == StrictCollationCompatible {
			return fmt.Errorf("local-binlog does not support strict collation discovery")
		}
	}
	if c.BinlogReplay != nil {
		if err := c.BinlogReplay.Verify(); err != nil {
			return err
		}
	}
	if err := verifyReplayPosition(c.Meta); err != nil {
		return err
	}
	if c.EnableGTID || c.UseRelay || (c.Flavor != "" && c.Flavor != "mysql") {
		return fmt.Errorf("binlog replay and blackhole currently require MySQL filename/position replication without relay")
	}
	if end := c.ReplayEnd(); end != nil {
		start := mysql.Position{Name: c.Meta.BinLogName, Pos: c.Meta.BinLogPos}
		startFile, err := utils.ParseFilename(start.Name)
		if err != nil {
			return err
		}
		endFile, err := utils.ParseFilename(end.Name)
		if err != nil {
			return err
		}
		if startFile.BaseName != endFile.BaseName {
			return fmt.Errorf("binlog replay start and end must use the same filename prefix")
		}
		if start.Compare(*end) >= 0 {
			return fmt.Errorf("binlog replay start must precede end")
		}
	}
	return nil
}
