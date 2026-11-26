package redis

import (
	"context"
	"testing"
	"time"
)

func TestNewRedisClient(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping redis test in short mode.")
	}
	url := "localhost:6379"

	cli, err := NewRedisClient([]string{url}, "", true, true)
	if err != nil {
		t.Fatal(err)
	}

	err = cli.Do(context.Background(), cli.B().Set().Key("k").Value("v").Build()).Error()
	if err != nil {
		t.Fatal(err)
	}
	v, err := cli.Do(context.Background(), cli.B().Get().Key("k").Build()).ToString()
	if err != nil {
		t.Fatal(err)
	} else {
		t.Log(v)
	}
}

func TestClientOption(t *testing.T) {
	opt := clientOption(Options{
		URLs:                  []string{"a:6379", "b:6379"},
		Resp3:                 true,
		Cache:                 true,
		ShardsRefreshInterval: 30 * time.Second,
		ConnWriteTimeout:      5 * time.Second,
	})
	if opt.ClusterOption.ShardsRefreshInterval != 30*time.Second {
		t.Errorf("ShardsRefreshInterval = %v, want 30s", opt.ClusterOption.ShardsRefreshInterval)
	}
	if opt.ConnWriteTimeout != 5*time.Second {
		t.Errorf("ConnWriteTimeout = %v, want 5s", opt.ConnWriteTimeout)
	}
	if opt.AlwaysRESP2 || opt.DisableCache {
		t.Errorf("RESP3 with cache: AlwaysRESP2 = %v, DisableCache = %v, want false, false", opt.AlwaysRESP2, opt.DisableCache)
	}
}

func TestClientOptionClampsNegativeDurations(t *testing.T) {
	opt := clientOption(Options{ShardsRefreshInterval: -time.Second, ConnWriteTimeout: -time.Second})
	if opt.ClusterOption.ShardsRefreshInterval != 0 || opt.ConnWriteTimeout != 0 {
		t.Errorf("got ShardsRefreshInterval = %v, ConnWriteTimeout = %v, want 0, 0",
			opt.ClusterOption.ShardsRefreshInterval, opt.ConnWriteTimeout)
	}
}

func TestClientOptionResp2DisablesCache(t *testing.T) {
	opt := clientOption(Options{Resp3: false, Cache: true})
	if !opt.AlwaysRESP2 || !opt.DisableCache {
		t.Errorf("RESP2: AlwaysRESP2 = %v, DisableCache = %v, want true, true", opt.AlwaysRESP2, opt.DisableCache)
	}
}
