package connector

import (
	"context"
	"sync"
)

type SinkManager struct {
	sinks []Sink
	wg    *sync.WaitGroup
}

func NewSinkManager() *SinkManager {
	return &SinkManager{
		sinks: make([]Sink, 0),
		wg:    &sync.WaitGroup{},
	}
}

// AddSink add a sink to the manager
func (sm *SinkManager) AddSink(sink Sink) {
	sm.sinks = append(sm.sinks, sink)
}

// StartAll starts all sinks in separate go-routines
func (sm *SinkManager) StartAll(ctx context.Context) {
	for _, sink := range sm.sinks {
		sm.wg.Add(1)
		go func() {
			defer sm.wg.Done()
			sink.Start(ctx)
		}()
	}
}

// StopAll call all sinks to stop
func (sm *SinkManager) StopAll() {
	for _, sink := range sm.sinks {
		sink.Stop()
	}
}

// Wait all sinks to stop
func (sm *SinkManager) Wait() {
	sm.wg.Wait()
}
