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
	"testing"

	"github.com/pingcap/tiflow/dm/config"
	"github.com/stretchr/testify/require"
)

func TestCheckIndependentEndpoints(t *testing.T) {
	cfg := config.NewSubTaskConfig()
	cfg.Blackhole = true
	cfg.LocalBinlog = &config.LocalBinlogConfig{}
	msg, err := checkIndependentEndpoints(context.Background(), []*config.SubTaskConfig{cfg})
	require.NoError(t, err)
	require.Contains(t, msg, "cross-database schema checks are not applicable")
	require.Contains(t, msg, "worker")
	cfg.LocalBinlog = nil
	cfg.IgnoreCheckingItems = []string{"all"}
	_, err = checkIndependentEndpoints(context.Background(), []*config.SubTaskConfig{cfg})
	require.NoError(t, err) // ignored checks must not connect to an absent DB
	other := *cfg
	other.Blackhole = false
	_, err = checkIndependentEndpoints(context.Background(), []*config.SubTaskConfig{cfg, &other})
	require.ErrorContains(t, err, "cannot mix")
}
