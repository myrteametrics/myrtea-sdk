package elasticsearch

import (
	"context"
	"sync"

	"github.com/myrteametrics/myrtea-sdk/v4/connection"
)

var (
	_globalMu          sync.RWMutex
	_globalC           *EsExecutor
	_gloablCredentials *Credentials
	_gloablBackoff     *connection.Backoff
)

// Credentials is used to store the elasticsearch credentials
type Credentials struct {
	URLs []string
}

// C returns the elasticsearch client singleton
func C() *EsExecutor {
	_globalMu.RLock()
	c := _globalC
	_globalMu.RUnlock()
	return c
}

// Backoff returns the elasticsearch backoff singleton
func Backoff() *connection.Backoff {
	_globalMu.RLock()
	b := _gloablBackoff
	_globalMu.RUnlock()
	return b
}

// InitializeBackoff initialize the global elasticsearch backoff policy singleton
func InitializeBackoff(backoff *connection.Backoff) {
	_globalMu.RLock()
	_gloablBackoff = backoff
	_globalMu.RUnlock()
}

// InitializeGlobal initialize the global elasticsearch client singleton
func InitializeGlobal(credentials *Credentials) error {
	_globalMu.RLock()
	_gloablCredentials = credentials
	ctx := context.Background()
	esExecutor, err := NewEsExecutor(ctx, credentials.URLs)
	if err != nil {
		_globalMu.RUnlock()
		return err
	}
	_globalC = esExecutor
	_globalMu.RUnlock()
	return nil
}

// ReplaceGlobals affect new elasticsearch credentials and connection to the global repository singleton
func ReplaceGlobals(credentials *Credentials) error {
	_globalMu.Lock()
	_gloablCredentials = credentials
	ctx := context.Background()
	esExecutor, err := NewEsExecutor(ctx, credentials.URLs)
	if err != nil {
		_globalMu.Unlock()
		return err
	}
	_globalC = esExecutor
	_globalMu.Unlock()
	return nil
}

// Reconnect build a new ES connection and replace the existing singleton with it
func Reconnect() error {
	_globalMu.Lock()
	ctx := context.Background()
	esExecutor, err := NewEsExecutor(ctx, _gloablCredentials.URLs)
	if err != nil {
		_globalMu.Unlock()
		return err
	}
	_globalC = esExecutor
	_globalMu.Unlock()
	return nil
}
