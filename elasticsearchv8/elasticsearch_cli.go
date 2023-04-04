package elasticsearchv8

import (
	"context"
	"sync"

	"github.com/elastic/go-elasticsearch/v8"
	"go.uber.org/zap"
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
	CheckVersion()
	return nil
}

func CheckVersion() error {
	response, err := _globalC.Info().Do(context.Background())
	if err != nil {
		zap.L().Error("Cannot get elasticsearch infos")
		return err
	}
	zap.L().Info("Elasticsearch Client", zap.String("version", elasticsearch.Version))
	zap.L().Info("Elasticsearch Server", zap.String("version", response.Version.Int))
	return nil
}
