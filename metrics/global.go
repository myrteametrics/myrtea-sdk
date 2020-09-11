package metrics

import (
	statsd "github.com/cactus/go-statsd-client/statsd"
	"go.uber.org/zap"
)

var (
	_globalC statsd.Statter
)

// NewStatsDClient returns a pointer to a new statsd.Statter instance
func NewStatsDClient(address, prefix string) statsd.Statter {
	config := &statsd.ClientConfig{
		Address: address,
	}
	// Now create the client
	client, err := statsd.NewClientWithConfig(config)
	if err != nil {
		zap.L().Error("Couldn't init the metrics client", zap.Error(err))
		return nil
	}
	return client
}

// C returns a pointer the singleton instance of statsd.Statter
func C() statsd.Statter {
	return _globalC
}

// ReplaceGlobals replaces statsd.Statter, and returns a
// function to restore the original values. It's safe for concurrent use.
func ReplaceGlobals(metrics statsd.Statter) func() {
	prev := _globalC
	_globalC = metrics
	return func() { ReplaceGlobals(prev) }
}
