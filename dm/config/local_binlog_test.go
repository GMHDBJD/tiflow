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
	"testing"

	"github.com/pingcap/tiflow/dm/config/dbconfig"
	"github.com/stretchr/testify/require"
)

func localConfigForTest() *SubTaskConfig {
	c := NewSubTaskConfig()
	c.Name = "archive"
	c.SourceID = "source"
	c.Mode = ModeIncrement
	c.Flavor = "mysql"
	c.Blackhole = true
	c.Meta = &Meta{BinLogName: "mysql-bin.000001", BinLogPos: 4}
	c.LocalBinlog = &LocalBinlogConfig{Directory: "/tmp/archive", SchemaFiles: []string{"/tmp/schema.sql"}, End: Meta{BinLogName: "mysql-bin.000002", BinLogPos: 100}, Timezone: "UTC"}
	c.ValidatorCfg.Mode = ValidationNone
	return c
}

func TestLocalBinlogConfig(t *testing.T) {
	require.NoError(t, localConfigForTest().VerifyLocalBinlog())
	for _, tc := range []struct {
		name   string
		mutate func(*SubTaskConfig)
	}{
		{"not blackhole", func(c *SubTaskConfig) { c.Blackhole = false }},
		{"not local", func(c *SubTaskConfig) { c.LocalBinlog = nil }},
		{"full", func(c *SubTaskConfig) { c.Mode = ModeAll }},
		{"sharding", func(c *SubTaskConfig) { c.ShardMode = ShardOptimistic }},
		{"online ddl", func(c *SubTaskConfig) { c.OnlineDDL = true }},
		{"gtid", func(c *SubTaskConfig) { c.EnableGTID = true }},
		{"relay", func(c *SubTaskConfig) { c.UseRelay = true }},
		{"validator", func(c *SubTaskConfig) { c.ValidatorCfg.Mode = ValidationFull }},
		{"relative directory", func(c *SubTaskConfig) { c.LocalBinlog.Directory = "archive" }},
		{"relative schema", func(c *SubTaskConfig) { c.LocalBinlog.SchemaFiles = []string{"schema.sql"} }},
		{"local timezone", func(c *SubTaskConfig) { c.LocalBinlog.Timezone = "Local" }},
		{"missing meta", func(c *SubTaskConfig) { c.Meta = nil }},
		{"prefix", func(c *SubTaskConfig) { c.Meta.BinLogName = "other-bin.000001" }},
		{"traversal", func(c *SubTaskConfig) { c.LocalBinlog.End.BinLogName = "../mysql-bin.000002" }},
		{"empty range", func(c *SubTaskConfig) { c.Meta = &c.LocalBinlog.End }},
	} {
		t.Run(tc.name, func(t *testing.T) { c := localConfigForTest(); tc.mutate(c); require.Error(t, c.VerifyLocalBinlog()) })
	}
}

func TestLocalBinlogConfigRoundTrip(t *testing.T) {
	c := localConfigForTest()
	encoded, err := c.Toml()
	require.NoError(t, err)
	decoded := NewSubTaskConfig()
	require.NoError(t, decoded.Decode(encoded, false))
	require.Equal(t, c.LocalBinlog, decoded.LocalBinlog)
	require.True(t, decoded.Blackhole)
	source := NewSourceConfig()
	source.SourceID = c.SourceID
	source.LocalBinlog = c.LocalBinlog
	yaml, err := source.YamlForDowngrade()
	require.NoError(t, err)
	copy, err := SourceCfgFromYamlAndVerify(yaml)
	require.NoError(t, err)
	require.Equal(t, source.LocalBinlog, copy.LocalBinlog)
	clone := source.Clone()
	clone.LocalBinlog.SchemaFiles[0] = "/changed"
	require.Equal(t, "/tmp/schema.sql", source.LocalBinlog.SchemaFiles[0])
}

func TestBlackholeTaskConfigExport(t *testing.T) {
	c := NewTaskConfig()
	require.NoError(t, c.FromYaml(`name: replay
task-mode: incremental
blackhole: true
online-ddl: false
mysql-instances:
   - source-id: local
     meta:
       binlog-name: mysql-bin.000001
       binlog-pos: 4
 `))
	exported, err := c.YamlForDowngrade()
	require.NoError(t, err)
	d := NewTaskConfig()
	require.NoError(t, d.FromYaml(exported))
	require.True(t, d.Blackhole)
	require.Nil(t, d.TargetDB)
	require.True(t, NewTaskConfigForDowngrade(c).Blackhole)
	// Opt-in target is never inferred and cannot silently discard a real target.
	c.TargetDB = &dbconfig.DBConfig{Host: "127.0.0.1"}
	require.Error(t, c.Adjust())
	normal := NewTaskConfig()
	require.False(t, normal.Blackhole)
	require.Error(t, normal.Adjust()) // normal mode still requires target config
}
