package connector

import (
	"context"
	"github.com/myrteametrics/myrtea-sdk/v5/expression"
	"testing"
)

func TestAddSink(t *testing.T) {
	sm := NewSinkManager()
	sink := &MockSink{}
	sm.AddSink(sink)
	expression.AssertEqual(t, 1, len(sm.sinks))
	expression.AssertEqual(t, sink, sm.sinks[0])
}

func TestStartAll(t *testing.T) {
	sm := NewSinkManager()
	sink := &MockSink{}
	sm.AddSink(sink)
	ctx := context.Background()
	sm.StartAll(ctx)
	sm.Wait()
	expression.AssertEqual(t, true, sink.startCalled)
}

func TestStopAll(t *testing.T) {
	sm := NewSinkManager()
	sink := &MockSink{}
	sm.AddSink(sink)
	sm.StopAll()
	expression.AssertEqual(t, true, sink.stopCalled)
}

func TestWait(t *testing.T) {
	sm := NewSinkManager()
	sink := &MockSink{}
	sm.AddSink(sink)
	ctx := context.Background()
	sm.StartAll(ctx)
	sm.StopAll()
	sm.Wait()
	expression.AssertEqual(t, true, sink.startCalled)
}

type MockSink struct {
	startCalled bool
	stopCalled  bool
	addCalled   bool
}

func (m *MockSink) Start(context.Context) {
	m.startCalled = true
}

func (m *MockSink) AddMessageToQueue(Message) {
	m.addCalled = true
}

func (m *MockSink) Stop() {
	m.stopCalled = true
}
