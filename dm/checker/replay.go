// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package checker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/config/dbconfig"
	"github.com/pingcap/tiflow/dm/pkg/checker"
	"github.com/pingcap/tiflow/dm/pkg/conn"
)

// checkIndependentEndpoints never substitutes a fake database for an absent
// endpoint. It checks the real endpoints independently. Cross-database table
// compatibility checks are not applicable when one endpoint is not a database;
// archive/schema files are checked on the worker, where their paths resolve.
func checkIndependentEndpoints(ctx context.Context, cfgs []*config.SubTaskConfig) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, checkTimeout)
	defer cancel()
	var dbs []*conn.BaseDB
	defer func() {
		for _, db := range dbs {
			_ = db.Close()
		}
	}()
	var checks []checker.RealChecker
	for _, cfg := range cfgs {
		if cfg.Blackhole != cfgs[0].Blackhole {
			return "", fmt.Errorf("cannot mix blackhole and database targets")
		}
		enabled := config.FilterCheckingItems(cfg.IgnoreCheckingItems)
		add := func(name string, check checker.RealChecker) {
			if _, ok := enabled[name]; ok {
				checks = append(checks, check)
			}
		}
		if cfg.LocalBinlog == nil && len(enabled) > 0 {
			dbCfg := cfg.From
			dbCfg.RawDBCfg = dbconfig.DefaultRawDBConfig().SetReadTimeout(readTimeout)
			db, err := conn.GetUpstreamDB(&dbCfg)
			if err != nil {
				return "", err
			}
			dbs = append(dbs, db)
			info := &dbutil.DBConfig{Host: dbCfg.Host, Port: dbCfg.Port, User: dbCfg.User, Password: dbCfg.Password}
			add(config.VersionChecking, checker.NewMySQLVersionChecker(db.DB, info))
			add(config.ServerIDChecking, checker.NewMySQLServerIDChecker(db.DB, info))
			add(config.BinlogEnableChecking, checker.NewMySQLBinlogEnableChecker(db.DB, info))
			add(config.BinlogFormatChecking, checker.NewMySQLBinlogFormatChecker(db.DB, info))
			add(config.BinlogRowImageChecking, checker.NewMySQLBinlogRowImageChecker(db.DB, info))
			add(config.ReplicationPrivilegeChecking, checker.NewSourceReplicationPrivilegeChecker(db.DB, info, db.Version))
			if cfg.Meta != nil {
				add(config.MetaPositionChecking, checker.NewMetaPositionChecker(db, cfg.From, cfg.EnableGTID, cfg.Meta))
			}
		}
		if !cfg.Blackhole {
			if _, ok := enabled[config.TargetDBPrivilegeChecking]; ok {
				dbCfg := cfg.To
				dbCfg.RawDBCfg = dbconfig.DefaultRawDBConfig().SetReadTimeout(readTimeout)
				db, err := conn.GetDownstreamDB(&dbCfg)
				if err != nil {
					return "", err
				}
				dbs = append(dbs, db)
				info := &dbutil.DBConfig{Host: dbCfg.Host, Port: dbCfg.Port, User: dbCfg.User, Password: dbCfg.Password}
				checks = append(checks, checker.NewTargetPrivilegeChecker(db.DB, info, db.Version))
			}
		}
	}
	result, err := checker.Do(ctx, checks)
	if err != nil {
		return "", err
	}
	detail, err := json.Marshal(result)
	if err != nil {
		return "", err
	}
	if !result.Summary.Passed {
		return "", fmt.Errorf("endpoint pre-check failed: %s", detail)
	}
	return fmt.Sprintf("endpoint pre-check passed; cross-database schema checks are not applicable; replay/schema files are checked on the worker. %s", detail), nil
}
