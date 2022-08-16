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

package metadata

import (
	"context"

	"github.com/pingcap/errors"

	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/config"
	"github.com/pingcap/tiflow/engine/pkg/adapter"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
)

// TaskStage represents internal stage of a task
// TODO: use Stage in lib or move Stage to lib.
type TaskStage int

// These stages may updated in later pr.
const (
	StageInit TaskStage = iota + 1
	StageRunning
	StagePaused
	StageFinished
	StageError
	StagePausing
	// UnScheduled means the task is not scheduled.
	// This usually happens when the worker is offline.
	StageUnscheduled
)

// Job represents the state of a job.
type Job struct {
	State

	// taskID -> task
	Tasks map[string]*Task

	// Deleting represents whether the job is being deleted.
	Deleting bool
}

// NewJob creates a new Job instance
func NewJob(jobCfg *config.JobCfg) *Job {
	taskCfgs := jobCfg.ToTaskCfgs()
	job := &Job{
		Tasks: make(map[string]*Task, len(taskCfgs)),
	}

	for taskID, taskCfg := range taskCfgs {
		job.Tasks[taskID] = NewTask(taskCfg)
	}
	return job
}

// Task is the minimum working unit of a job.
// A job may contain multiple upstream and it will be converted into multiple tasks.
type Task struct {
	Cfg   *config.TaskCfg
	Stage TaskStage
}

// NewTask creates a new Task instance
func NewTask(taskCfg *config.TaskCfg) *Task {
	return &Task{
		Cfg:   taskCfg,
		Stage: StageRunning, // TODO: support set stage when create task.
	}
}

// JobStore manages the state of a job.
type JobStore struct {
	*TomlStore

	id frameModel.MasterID
}

// NewJobStore creates a new JobStore instance
func NewJobStore(id frameModel.MasterID, kvClient metaModel.KVClient) *JobStore {
	jobStore := &JobStore{
		TomlStore: NewTomlStore(kvClient),
		id:        id,
	}
	jobStore.TomlStore.Store = jobStore
	return jobStore
}

// CreateState returns an empty Job object
func (jobStore *JobStore) CreateState() State {
	return &Job{}
}

// Key returns encoded key for job store id
func (jobStore *JobStore) Key() string {
	return adapter.DMJobKeyAdapter.Encode(jobStore.id)
}

// UpdateStages will be called if user operate job.
func (jobStore *JobStore) UpdateStages(ctx context.Context, taskIDs []string, stage TaskStage) error {
	state, err := jobStore.Get(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	job := state.(*Job)
	if len(taskIDs) == 0 {
		for task := range job.Tasks {
			taskIDs = append(taskIDs, task)
		}
	}
	for _, taskID := range taskIDs {
		if _, ok := job.Tasks[taskID]; !ok {
			return errors.Errorf("task %s not found", taskID)
		}
		t := job.Tasks[taskID]
		t.Stage = stage
	}

	return jobStore.Put(ctx, job)
}

// UpdateConfig will be called if user update job config.
func (jobStore *JobStore) UpdateConfig(ctx context.Context, jobCfg *config.JobCfg) error {
	state, err := jobStore.Get(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	oldJob := state.(*Job)

	// TODO: we may diff the config at task level in the future, that way different tasks will have different modify revisions.
	// so that changing the configuration of one task will not affect other tasks.
	var oldVersion uint64
	for _, task := range oldJob.Tasks {
		oldVersion = task.Cfg.ModRevision
		break
	}
	jobCfg.ModRevision = oldVersion + 1
	newJob := NewJob(jobCfg)

	for taskID, newTask := range newJob.Tasks {
		// task stage will not be updated.
		if oldTask, ok := oldJob.Tasks[taskID]; ok {
			newTask.Stage = oldTask.Stage
		}
	}

	return jobStore.Put(ctx, newJob)
}

// MarkDeleting marks the job as deleting.
func (jobStore *JobStore) MarkDeleting(ctx context.Context) error {
	state, err := jobStore.Get(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	job := state.(*Job)
	job.Deleting = true
	return jobStore.Put(ctx, job)
}
