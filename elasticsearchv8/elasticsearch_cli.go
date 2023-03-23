package elasticsearchv8

import (
	"sync"

	"github.com/elastic/go-elasticsearch/v8"
)

var (
	_globalMu     sync.RWMutex
	_globalC      *elasticsearch.TypedClient
	_globalConfig elasticsearch.Config
)

// Credentials is used to store the elasticsearch credentials
type Credentials struct {
	URLs []string
}

// C returns the elasticsearch client singleton
func C() *elasticsearch.TypedClient {
	_globalMu.RLock()
	client := _globalC
	_globalMu.RUnlock()
	return client
}

// ReplaceGlobals affect new elasticsearch credentials and connection to the global repository singleton
func ReplaceGlobals(config elasticsearch.Config) error {
	_globalMu.Lock()
	defer _globalMu.Unlock()
	client, err := elasticsearch.NewTypedClient(config)
	if err != nil {
		return err
	}
	_globalConfig = config
	_globalC = client
	return nil
}
