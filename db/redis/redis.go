package redis

import (
	"fmt"
	"sync"

	"github.com/go-redis/redis"
)

var (
	_mutex  sync.RWMutex
	_client *redis.ClusterClient
)

// Redis used to creat new Redis instance
func Redis(addr []string, pass string) *redis.ClusterClient {
	if _client == nil {
		_mutex.Lock()
		if _client == nil {
			_client = redis.NewClusterClient(&redis.ClusterOptions{
				Addrs:    addr,
				Password: pass,
			})
			s := _client.Ping()
			r, _ := s.Result()
			fmt.Printf("redius status is %s", r)

			err := _client.ReloadState()
			if err != nil {
				panic(err)
			}
		}
		_mutex.Unlock()
	}
	return _client
}
