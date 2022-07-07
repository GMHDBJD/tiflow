// Package openapi provides primitives to interact with the openapi HTTP API.
//
// Code generated by github.com/deepmap/oapi-codegen version v1.9.0 DO NOT EDIT.
package openapi

// Defines values for OperateJobRequestOp.
const (
	OperateJobRequestOpPause OperateJobRequestOp = "pause"

	OperateJobRequestOpResume OperateJobRequestOp = "resume"
)

// Defines values for SetBinlogOperatorRequestOp.
const (
	SetBinlogOperatorRequestOpInject SetBinlogOperatorRequestOp = "inject"

	SetBinlogOperatorRequestOpReplace SetBinlogOperatorRequestOp = "replace"

	SetBinlogOperatorRequestOpSkip SetBinlogOperatorRequestOp = "skip"
)

// operation error
type ErrorWithMessage struct {
	// error message
	ErrorMsg string `json:"error_msg"`
}

// OperateJobRequest defines model for OperateJobRequest.
type OperateJobRequest struct {
	Op    OperateJobRequestOp `json:"op"`
	Tasks *[]string           `json:"tasks,omitempty"`
}

// OperateJobRequestOp defines model for OperateJobRequest.Op.
type OperateJobRequestOp string

// SetBinlogOperatorRequest defines model for SetBinlogOperatorRequest.
type SetBinlogOperatorRequest struct {
	BinlogPos *string                    `json:"binlog_pos,omitempty"`
	Op        SetBinlogOperatorRequestOp `json:"op"`
	Sqls      *[]string                  `json:"sqls,omitempty"`
}

// SetBinlogOperatorRequestOp defines model for SetBinlogOperatorRequest.Op.
type SetBinlogOperatorRequestOp string

// SetBinlogSchemaRequest defines model for SetBinlogSchemaRequest.
type SetBinlogSchemaRequest struct {
	Database   string `json:"database"`
	FromSource *bool  `json:"from_source,omitempty"`
	FromTarget *bool  `json:"from_target,omitempty"`
	Sql        string `json:"sql"`
	Table      string `json:"table"`
}

// UpdateJobConfigRequest defines model for UpdateJobConfigRequest.
type UpdateJobConfigRequest struct {
	Config string `json:"config"`
}

// DMAPIDeleteBinlogOperatorParams defines parameters for DMAPIDeleteBinlogOperator.
type DMAPIDeleteBinlogOperatorParams struct {
	BinlogPos *string `json:"binlog_pos,omitempty"`
}

// DMAPIGetBinlogOperatorParams defines parameters for DMAPIGetBinlogOperator.
type DMAPIGetBinlogOperatorParams struct {
	BinlogPos *string `json:"binlog_pos,omitempty"`
}

// DMAPISetBinlogOperatorJSONBody defines parameters for DMAPISetBinlogOperator.
type DMAPISetBinlogOperatorJSONBody SetBinlogOperatorRequest

// DMAPIUpdateJobConfigJSONBody defines parameters for DMAPIUpdateJobConfig.
type DMAPIUpdateJobConfigJSONBody UpdateJobConfigRequest

// DMAPIGetSchemaParams defines parameters for DMAPIGetSchema.
type DMAPIGetSchemaParams struct {
	// database name
	Database *string `json:"database,omitempty"`

	// table name
	Table *string `json:"table,omitempty"`

	// target table
	Target *bool `json:"target,omitempty"`
}

// DMAPISetSchemaJSONBody defines parameters for DMAPISetSchema.
type DMAPISetSchemaJSONBody SetBinlogSchemaRequest

// DMAPIGetJobStatusParams defines parameters for DMAPIGetJobStatus.
type DMAPIGetJobStatusParams struct {
	// globally unique data source name
	Tasks *[]string `json:"tasks,omitempty"`
}

// DMAPIOperateJobJSONBody defines parameters for DMAPIOperateJob.
type DMAPIOperateJobJSONBody OperateJobRequest

// DMAPISetBinlogOperatorJSONRequestBody defines body for DMAPISetBinlogOperator for application/json ContentType.
type DMAPISetBinlogOperatorJSONRequestBody DMAPISetBinlogOperatorJSONBody

// DMAPIUpdateJobConfigJSONRequestBody defines body for DMAPIUpdateJobConfig for application/json ContentType.
type DMAPIUpdateJobConfigJSONRequestBody DMAPIUpdateJobConfigJSONBody

// DMAPISetSchemaJSONRequestBody defines body for DMAPISetSchema for application/json ContentType.
type DMAPISetSchemaJSONRequestBody DMAPISetSchemaJSONBody

// DMAPIOperateJobJSONRequestBody defines body for DMAPIOperateJob for application/json ContentType.
type DMAPIOperateJobJSONRequestBody DMAPIOperateJobJSONBody
