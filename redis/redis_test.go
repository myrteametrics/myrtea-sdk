package redis

import (
	"context"
	"testing"
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
