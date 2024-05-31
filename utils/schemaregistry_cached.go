package utils

import (
	"strconv"
	"time"

	ttlcache "github.com/myrteametrics/myrtea-sdk/v5/cache"
)

// CachedSchemaRegistry :
type CachedSchemaRegistry struct {
	client         Client
	cacheByID      *ttlcache.Cache
	cacheBySubject *ttlcache.Cache
}

// NewCachedSchemaRegistry :
func NewCachedSchemaRegistry(url string, ttlCacheDuration time.Duration) (*CachedSchemaRegistry, error) {
	client, err := NewClient(url)
	if err != nil {
		return nil, err
	}
	cacheByID := ttlcache.NewCache(ttlCacheDuration)
	cacheBySubject := ttlcache.NewCache(ttlCacheDuration)
	return &CachedSchemaRegistry{client, cacheByID, cacheBySubject}, nil
}

// GetSchemaByID :
func (reg CachedSchemaRegistry) GetSchemaByID(id int) (string, error) {
	idStr := strconv.Itoa(id)
	value, exists := reg.cacheByID.Get(idStr)
	if exists {
		return value.(string), nil
	}

	schema, err := reg.client.GetSchemaByID(id)
	if err != nil {
		return "", err
	}

	reg.cacheByID.Set(idStr, schema)
	return schema, nil
}

// GetSchemaBySubject :
func (reg CachedSchemaRegistry) GetSchemaBySubject(subjectStr string, version int) (Schema, error) {
	idStr := subjectStr + "/" + strconv.Itoa(version)
	value, exists := reg.cacheBySubject.Get(idStr)
	if exists {
		return value.(Schema), nil
	}

	schema, err := reg.client.GetSchemaBySubject(subjectStr, version)
	if err != nil {
		return Schema{}, err
	}

	reg.cacheBySubject.Set(idStr, schema)
	return schema, nil
}
