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
	"time"

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
	require.NoError(t, localConfigForTest().VerifySyncerMode())
	for _, tc := range []struct {
		name   string
		mutate func(*SubTaskConfig)
	}{
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
		t.Run(tc.name, func(t *testing.T) { c := localConfigForTest(); tc.mutate(c); require.Error(t, c.VerifySyncerMode()) })
	}
}

func TestLocalBinlogConfigRoundTrip(t *testing.T) {
	c := localConfigForTest()
	c.BlackholeLatency = Duration{Duration: 5 * time.Millisecond}
	encoded, err := c.Toml()
	require.NoError(t, err)
	decoded := NewSubTaskConfig()
	require.NoError(t, decoded.Decode(encoded, false))
	require.Equal(t, c.LocalBinlog, decoded.LocalBinlog)
	require.True(t, decoded.Blackhole)
	require.Equal(t, c.BlackholeLatency, decoded.BlackholeLatency)
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
blackhole-latency: 5ms
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
	require.Equal(t, 5*time.Millisecond, d.BlackholeLatency.Duration)
	subtasks, err := TaskConfigToSubTaskConfigs(c, map[string]dbconfig.DBConfig{"local": {Host: "127.0.0.1"}})
	require.NoError(t, err)
	require.Equal(t, c.BlackholeLatency, subtasks[0].BlackholeLatency)
	require.Equal(t, c.BlackholeLatency, SubTaskConfigsToTaskConfig(subtasks...).BlackholeLatency)
	require.Nil(t, d.TargetDB)
	require.True(t, NewTaskConfigForDowngrade(c).Blackhole)
	// Opt-in target is never inferred and cannot silently discard a real target.
	c.TargetDB = &dbconfig.DBConfig{Host: "127.0.0.1"}
	require.Error(t, c.Adjust())
	normal := NewTaskConfig()
	require.False(t, normal.Blackhole)
	require.Error(t, normal.Adjust()) // normal mode still requires target config
}

// Input transport must not imply the output target, or vice versa.
func TestIndependentInputAndOutput(t *testing.T) {
	for _, local := range []bool{false, true} {
		for _, blackhole := range []bool{false, true} {
			c := localConfigForTest()
			c.Blackhole = blackhole
			if !local {
				c.LocalBinlog = nil
			}
			require.NoError(t, c.VerifySyncerMode())
		}
	}
	c := localConfigForTest()
	c.BinlogReplay = &BinlogReplayConfig{SchemaFiles: c.LocalBinlog.SchemaFiles, End: c.LocalBinlog.End}
	require.Error(t, c.VerifySyncerMode()) // never silently choose between endpoints
	c.LocalBinlog = nil
	require.NoError(t, c.VerifySyncerMode())
	require.Equal(t, "mysql-bin.000002", c.ReplayEnd().Name)
	require.Equal(t, []string{"/tmp/schema.sql"}, c.ReplaySchemaFiles())
	c.Blackhole = false
	require.NoError(t, c.VerifySyncerMode())
	c.BinlogReplay.End.BinLogPos = 3
	require.Error(t, c.VerifySyncerMode())
	c.BinlogReplay.End.BinLogPos = 100
	c.BinlogReplay.SchemaFiles = []string{"relative.sql"}
	require.Error(t, c.VerifySyncerMode())
}

func TestRemoteReplayConfigRoundTrip(t *testing.T) {
	c := localConfigForTest()
	c.BinlogReplay = &BinlogReplayConfig{SchemaFiles: c.LocalBinlog.SchemaFiles, End: c.LocalBinlog.End}
	c.LocalBinlog = nil
	encoded, err := c.Toml()
	require.NoError(t, err)
	decoded := NewSubTaskConfig()
	require.NoError(t, decoded.Decode(encoded, false))
	require.Equal(t, c.BinlogReplay, decoded.BinlogReplay)
	source := NewSourceConfig()
	source.SourceID = c.SourceID
	source.BinlogReplay = c.BinlogReplay
	encoded, err = source.YamlForDowngrade()
	require.NoError(t, err)
	copy, err := SourceCfgFromYamlAndVerify(encoded)
	require.NoError(t, err)
	require.Equal(t, source.BinlogReplay, copy.BinlogReplay)
	clone := source.Clone()
	clone.BinlogReplay.SchemaFiles[0] = "/changed.sql"
	require.Equal(t, "/tmp/schema.sql", source.BinlogReplay.SchemaFiles[0])
	source.EnableRelay = true
	require.Error(t, source.Verify())
	// A real target must not lose online-ddl=false in compatibility export.
	c.Blackhole = false
	task := SubTaskConfigsToTaskConfig(c)
	exported, err := task.YamlForDowngrade()
	require.NoError(t, err)
	require.Contains(t, exported, "online-ddl: false")
	restored := NewTaskConfig()
	require.NoError(t, restored.FromYaml(exported))
	require.False(t, restored.OnlineDDL)
	require.False(t, restored.Blackhole)

}

func TestBlackholeLatencyConfigValidation(t *testing.T) {
	for _, tc := range []struct {
		enabled bool
		latency time.Duration
		valid   bool
	}{
		{false, 0, true}, {true, 0, true}, {true, time.Millisecond, true},
		{false, time.Millisecond, false}, {false, -time.Millisecond, false}, {true, -time.Millisecond, false},
	} {
		c := localConfigForTest()
		c.Blackhole = tc.enabled
		c.BlackholeLatency = Duration{Duration: tc.latency}
		err := c.VerifySyncerMode()
		if tc.valid {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
		task := SubTaskConfigsToTaskConfig(c)
		err = task.Adjust()
		if tc.valid {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
	}
}
