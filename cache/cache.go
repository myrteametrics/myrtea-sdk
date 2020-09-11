package ttlcache

import (
	"log"
	"sync"
	"time"
)

// Cache is a synchronised map of items that auto-expire once stale
type Cache struct {
	mutex        sync.RWMutex
	ttl          time.Duration
	items        map[string]*Item
	getIfMissing func(string) (interface{}, error)
}

// Dump is a thread-safe way to fully clear the cache
func (cache *Cache) Dump() {
	cache.mutex.Lock()
	log.Println("Dump cache content")
	for key, value := range cache.items {
		log.Println(key, value.data)
	}
	cache.mutex.Unlock()
}

// Set is a thread-safe way to add new items to the map
func (cache *Cache) Set(key string, data interface{}) {
	cache.mutex.Lock()
	item := &Item{data: data}
	item.touch(cache.ttl)
	cache.items[key] = item
	cache.mutex.Unlock()
}

// Delete is a thread-safe way to delete items from the map
func (cache *Cache) Delete(key string) {
	cache.mutex.Lock()
	delete(cache.items, key)
	cache.mutex.Unlock()
}

// AddToSlice implements a super dirty slice cache method
func (cache *Cache) AddToSlice(key string, data interface{}) {
	cache.mutex.Lock()
	item, exists := cache.items[key]
	if !exists || item.expired() {
		item := &Item{}
		item.touch(cache.ttl)
		d := make([]interface{}, 0)
		d = append(d, data)
		item.data = d
		cache.items[key] = item

	} else {
		item.touch(cache.ttl)
		sl := item.data.([]interface{})
		sl = append(sl, data)
		item := &Item{}
		item.touch(cache.ttl)
		item.data = sl
		cache.items[key] = item
	}
	cache.mutex.Unlock()
}

// Get is a thread-safe way to lookup items
// Every lookup, also touches the item, hence extending it's life
func (cache *Cache) Get(key string) (data interface{}, found bool) {
	cache.mutex.Lock()
	item, exists := cache.items[key]
	if !exists || item.expired() {
		data = ""
		found = false
	} else {
		item.touch(cache.ttl)
		data = item.data
		found = true
	}
	cache.mutex.Unlock()
	return
}

// Count returns the number of items in the cache
// (helpful for tracking memory leaks)
func (cache *Cache) Count() int {
	cache.mutex.RLock()
	count := len(cache.items)
	cache.mutex.RUnlock()
	return count
}

func (cache *Cache) cleanup() {
	cache.mutex.Lock()
	for key, item := range cache.items {
		if item.expired() {
			delete(cache.items, key)
		}
	}
	cache.mutex.Unlock()
}

func (cache *Cache) startCleanupTimer() {
	duration := cache.ttl
	if duration < time.Second {
		duration = time.Second
	}
	ticker := time.Tick(duration)
	go (func() {
		for {
			select {
			case <-ticker:
				cache.cleanup()
			}
		}
	})()
}

// NewCache is a helper to create instance of the Cache struct
func NewCache(duration time.Duration) *Cache {
	cache := &Cache{
		ttl:   duration,
		items: map[string]*Item{},
	}
	cache.startCleanupTimer()
	return cache
}
