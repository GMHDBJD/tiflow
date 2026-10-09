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

package syncer

import (
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
	"go.uber.org/zap"
)

// finishBinlogReplay uses the normal synchronous flush/barrier and checkpoint
// path for either input and either output. A remote EOF is not required: the
// source may remain idle forever immediately after the configured final XID.
func (s *Syncer) finishBinlogReplay(lastTxnEnd, end mysql.Position) error {
	if lastTxnEnd.Compare(end) != 0 {
		return errors.Errorf("binlog replay end %s is not a transaction boundary (last transaction %s)", end, lastTxnEnd)
	}
	if err := s.flushJobs(); err != nil {
		return err
	}
	if err := s.execError.Load(); err != nil {
		return err
	}
	fields := []zap.Field{zap.Stringer("end", end), zap.String("input", s.binlogType.String())}
	if s.blackhole != nil {
		fields = append(fields, zap.Any("dataSQL", s.blackhole.Stats()), zap.Any("checkpointSQL", s.checkpoint.(*RemoteCheckPoint).blackhole.Stats()))
	}
	s.tctx.L().Info("binlog replay completed", fields...)
	return nil
}
