package dgryski

import (
	"net"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/dgryski/go-ketama"
)

func New(buckets []ketama.Bucket) memcache.ServerSelector {
	sl := &ServerList{
		Buckets: buckets,
	}

	return sl
}

func NewWithHash(buckets []ketama.Bucket, fn ketama.HashFunc) memcache.ServerSelector {
	sl := &ServerList{
		Buckets:  buckets,
		HashFunc: fn,
	}

	return sl
}

// ServerList is a simple ServerSelector. Its zero value is usable.
type ServerList struct {
	Buckets  []ketama.Bucket
	HashFunc ketama.HashFunc
}

func (sl *ServerList) PickServer(key string) (net.Addr, error) {
	c, err := ketama.NewWithHash(sl.Buckets, sl.HashFunc)
	if err != nil {
		return nil, err
	}

	server := c.Hash(key)

	tcpaddr, err := net.ResolveTCPAddr("tcp", server)
	if err != nil {
		return nil, err
	}

	return tcpaddr, nil
}

func (sl *ServerList) Each(func(net.Addr) error) error { return nil }
