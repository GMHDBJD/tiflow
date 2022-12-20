// Copyright 2022 PingCAP, Inc.
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

package scheduler

import (
	"math"
	"math/rand"
	"sort"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/member"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/replication"
	"github.com/pingcap/tiflow/pkg/spanz"
	"go.uber.org/zap"
)

var _ scheduler = &rebalanceScheduler{}

type rebalanceScheduler struct {
	rebalance int32
	random    *rand.Rand

	changefeedID model.ChangeFeedID
}

func newRebalanceScheduler(changefeed model.ChangeFeedID) *rebalanceScheduler {
	return &rebalanceScheduler{
		rebalance:    0,
		random:       rand.New(rand.NewSource(time.Now().UnixNano())),
		changefeedID: changefeed,
	}
}

func (r *rebalanceScheduler) Name() string {
	return "rebalance-scheduler"
}

func (r *rebalanceScheduler) Schedule(
	_ model.Ts,
	currentSpans []tablepb.Span,
	captures map[model.CaptureID]*member.CaptureStatus,
	replications *spanz.Map[*replication.ReplicationSet],
) []*replication.ScheduleTask {
	// rebalance is not triggered, or there is still some pending task,
	// do not generate new tasks.
	if atomic.LoadInt32(&r.rebalance) == 0 {
		return nil
	}

	if len(captures) == 0 {
		return nil
	}

	for _, capture := range captures {
		if capture.State == member.CaptureStateStopping {
			log.Warn("schedulerv3: capture is stopping, ignore manual rebalance request",
				zap.String("namespace", r.changefeedID.Namespace),
				zap.String("changefeed", r.changefeedID.ID))
			atomic.StoreInt32(&r.rebalance, 0)
			return nil
		}
	}

	// only rebalance when all tables are replicating
	for _, span := range currentSpans {
		rep, ok := replications.Get(span)
		if !ok {
			return nil
		}
		if rep.State != replication.ReplicationSetStateReplicating {
			log.Debug("schedulerv3: not all table replicating, premature to rebalance tables",
				zap.String("namespace", r.changefeedID.Namespace),
				zap.String("changefeed", r.changefeedID.ID))
			return nil
		}
	}

	unlimited := math.MaxInt
	tasks := newBalanceMoveTables(r.random, captures, replications, unlimited, r.changefeedID)
	if len(tasks) == 0 {
		return nil
	}
	accept := func() {
		atomic.StoreInt32(&r.rebalance, 0)
		log.Info("schedulerv3: manual rebalance request accepted",
			zap.String("namespace", r.changefeedID.Namespace),
			zap.String("changefeed", r.changefeedID.ID))
	}
	return []*replication.ScheduleTask{{
		BurstBalance: &replication.BurstBalance{MoveTables: tasks},
		Accept:       accept,
	}}
}

func newBalanceMoveTables(
	random *rand.Rand,
	captures map[model.CaptureID]*member.CaptureStatus,
	replications *spanz.Map[*replication.ReplicationSet],
	maxTaskLimit int,
	changefeedID model.ChangeFeedID,
) []replication.MoveTable {
	tablesPerCapture := make(map[model.CaptureID]*spanz.Set)
	for captureID := range captures {
		tablesPerCapture[captureID] = spanz.NewSet()
	}

	replications.Ascend(func(span tablepb.Span, rep *replication.ReplicationSet) bool {
		if rep.State == replication.ReplicationSetStateReplicating {
			tablesPerCapture[rep.Primary].Add(span)
		}
		return true
	})

	// findVictim return tables which need to be moved
	upperLimitPerCapture := int(math.Ceil(float64(replications.Len()) / float64(len(captures))))

	victims := make([]tablepb.Span, 0)
	for _, ts := range tablesPerCapture {
		spans := ts.Keys()
		if random != nil {
			// Complexity note: Shuffle has O(n), where `n` is the number of tables.
			// Also, during a single call of `Schedule`, Shuffle can be called at most
			// `c` times, where `c` is the number of captures (TiCDC nodes).
			// Only called when a rebalance is triggered, which happens rarely,
			// we do not expect a performance degradation as a result of adding
			// the randomness.
			random.Shuffle(len(spans), func(i, j int) {
				spans[i], spans[j] = spans[j], spans[i]
			})
		} else {
			// sort the spans here so that the result is deterministic,
			// which would aid testing and debugging.
			sort.Slice(spans, func(i, j int) bool {
				return spans[i].Less(&spans[j])
			})
		}

		tableNum2Remove := len(spans) - upperLimitPerCapture
		if tableNum2Remove <= 0 {
			continue
		}

		for _, span := range spans {
			if tableNum2Remove <= 0 {
				break
			}
			victims = append(victims, span)
			ts.Remove(span)
			tableNum2Remove--
		}
	}
	if len(victims) == 0 {
		return nil
	}

	captureWorkload := make(map[model.CaptureID]int)
	for captureID, ts := range tablesPerCapture {
		captureWorkload[captureID] = randomizeWorkload(random, ts.Size())
	}
	// for each victim table, find the target for it
	moveTables := make([]replication.MoveTable, 0, len(victims))
	for idx, span := range victims {
		target := ""
		minWorkload := math.MaxInt64

		for captureID, workload := range captureWorkload {
			if workload < minWorkload {
				minWorkload = workload
				target = captureID
			}
		}

		if minWorkload == math.MaxInt64 {
			log.Panic("schedulerv3: rebalance meet unexpected min workload "+
				"when try to the the target capture",
				zap.String("namespace", changefeedID.Namespace),
				zap.String("changefeed", changefeedID.ID))
		}
		if idx >= maxTaskLimit {
			// We have reached the task limit.
			break
		}

		moveTables = append(moveTables, replication.MoveTable{
			Span:        span,
			DestCapture: target,
		})
		tablesPerCapture[target].Add(span)
		captureWorkload[target] = randomizeWorkload(random, tablesPerCapture[target].Size())
	}

	return moveTables
}

const (
	randomPartBitSize = 8
	randomPartMask    = (1 << randomPartBitSize) - 1
)

// randomizeWorkload injects small randomness into the workload, so that
// when two captures tied in competing for the minimum workload, the result
// will not always be the same.
// The bitwise layout of the return value is:
// 63                8                0
// |----- input -----|-- random val --|
func randomizeWorkload(random *rand.Rand, input int) int {
	var randomPart int
	if random != nil {
		randomPart = int(random.Uint32() & randomPartMask)
	}
	// randomPart is a small random value that only affects the
	// result of comparison of workloads when two workloads are equal.
	return (input << randomPartBitSize) | randomPart
}
