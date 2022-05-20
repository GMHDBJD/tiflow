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

package dmtask

import (
	"context"
	"testing"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/tiflow/engine/lib"
	libModel "github.com/pingcap/tiflow/engine/lib/model"
	"github.com/pingcap/tiflow/engine/lib/registry"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
)

func TestLoadWorker(t *testing.T) {
	// This test requires a TiDB running on port 4000 and dump files placed in
	// /tmp/dftest.db_ut . TestDumpWorker can generate the dump files.
	t.SkipNow()
	t.Parallel()

	require.NoError(t, log.InitLogger(&log.Config{Level: "debug"}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	workerWrapped, err := registry.GlobalWorkerRegistry().CreateWorker(
		dcontext.Background(), lib.WorkerDMLoad, workerID, masterID, mockWorkerConfig())
	require.NoError(t, err)

	worker := workerWrapped.(*LoadTask)
	worker.BaseWorker = lib.MockBaseWorker(workerID, masterID, worker)

	putMasterMeta(context.Background(), t, worker.MetaKVClient(), &libModel.MasterMetaKVData{
		ID:         masterID,
		NodeID:     nodeID,
		Epoch:      1,
		StatusCode: libModel.MasterStatusInit,
	})

	err = worker.Init(ctx)
	require.NoError(t, err)
	err = worker.Tick(ctx)
	require.NoError(t, err)
	lib.MockBaseWorkerWaitUpdateStatus(t, worker.BaseWorker.(*lib.DefaultBaseWorker))

	cancel()
	err = worker.Close(context.Background())
	require.NoError(t, err)
}