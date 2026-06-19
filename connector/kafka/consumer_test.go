package kafka

import (
	"os"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestHandlePanicBelowBudgetContinues(t *testing.T) {
	done := make(chan os.Signal, 1)
	params := ConsumerParams{MaxPermittedPanics: 3, Done: &done}
	count := &atomic.Int64{}

	// Three recovered panics are within budget: no shutdown signal expected.
	for i := 0; i < 3; i++ {
		handlePanic("boom", params, count)
	}

	assert.Equal(t, int64(3), count.Load())
	assert.Len(t, done, 0, "no shutdown signal while within budget")
}

func TestHandlePanicExceedingBudgetSignalsDone(t *testing.T) {
	done := make(chan os.Signal, 1)
	params := ConsumerParams{MaxPermittedPanics: 1, Done: &done}
	count := &atomic.Int64{}

	handlePanic("boom", params, count) // count = 1, within budget
	assert.Len(t, done, 0)

	handlePanic("boom", params, count) // count = 2, exceeds budget -> signal

	assert.Equal(t, int64(2), count.Load())
	select {
	case sig := <-done:
		assert.Equal(t, os.Interrupt, sig)
	default:
		t.Fatal("expected shutdown signal on Done channel")
	}
}

func TestPartitionConsumerProcessRecoversPanic(t *testing.T) {
	done := make(chan os.Signal, 1)
	pc := &partitionConsumer{
		processor:  panicProcessor{},
		params:     ConsumerParams{MaxPermittedPanics: 10, Done: &done},
		panicCount: &atomic.Int64{},
	}

	// Must not propagate the panic.
	assert.NotPanics(t, func() {
		pc.process(nil)
	})
	assert.Equal(t, int64(1), pc.panicCount.Load())
}

type panicProcessor struct{}

func (panicProcessor) Process(*kgo.Record) { panic("processor exploded") }
