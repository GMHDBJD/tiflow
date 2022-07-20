// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/pingcap/tiflow/engine/enginepb (interfaces: ExecutorClient)

// Package mock is a generated GoMock package.
package mock

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	enginepb "github.com/pingcap/tiflow/engine/enginepb"
	grpc "google.golang.org/grpc"
)

// MockExecutorClient is a mock of ExecutorClient interface.
type MockExecutorClient struct {
	ctrl     *gomock.Controller
	recorder *MockExecutorClientMockRecorder
}

// MockExecutorClientMockRecorder is the mock recorder for MockExecutorClient.
type MockExecutorClientMockRecorder struct {
	mock *MockExecutorClient
}

// NewMockExecutorClient creates a new mock instance.
func NewMockExecutorClient(ctrl *gomock.Controller) *MockExecutorClient {
	mock := &MockExecutorClient{ctrl: ctrl}
	mock.recorder = &MockExecutorClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockExecutorClient) EXPECT() *MockExecutorClientMockRecorder {
	return m.recorder
}

// ConfirmDispatchTask mocks base method.
func (m *MockExecutorClient) ConfirmDispatchTask(arg0 context.Context, arg1 *enginepb.ConfirmDispatchTaskRequest, arg2 ...grpc.CallOption) (*enginepb.ConfirmDispatchTaskResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "ConfirmDispatchTask", varargs...)
	ret0, _ := ret[0].(*enginepb.ConfirmDispatchTaskResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ConfirmDispatchTask indicates an expected call of ConfirmDispatchTask.
func (mr *MockExecutorClientMockRecorder) ConfirmDispatchTask(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ConfirmDispatchTask", reflect.TypeOf((*MockExecutorClient)(nil).ConfirmDispatchTask), varargs...)
}

// PreDispatchTask mocks base method.
func (m *MockExecutorClient) PreDispatchTask(arg0 context.Context, arg1 *enginepb.PreDispatchTaskRequest, arg2 ...grpc.CallOption) (*enginepb.PreDispatchTaskResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "PreDispatchTask", varargs...)
	ret0, _ := ret[0].(*enginepb.PreDispatchTaskResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PreDispatchTask indicates an expected call of PreDispatchTask.
func (mr *MockExecutorClientMockRecorder) PreDispatchTask(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PreDispatchTask", reflect.TypeOf((*MockExecutorClient)(nil).PreDispatchTask), varargs...)
}