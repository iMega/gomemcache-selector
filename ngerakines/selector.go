package ngerakines

import (
	"net"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/ngerakines/ketama"
	goketama "github.com/rckclmbr/goketama/ketama"
)

func New(servers []goketama.ServerInfo, spots int) memcache.ServerSelector {
	hr := ketama.NewRing(spots)

	for _, v := range servers {
		hr.Add(v.Addr.String(), int(v.Memory))
	}

	hr.Bake()

	sl := &ServerList{Ring: hr}

	return sl
}

// ServerList is a simple ServerSelector. Its zero value is usable.
type ServerList struct {
	Ring ketama.HashRing
}

func (sl *ServerList) PickServer(key string) (net.Addr, error) {
	server := sl.Ring.Hash(key)

	tcpaddr, err := net.ResolveTCPAddr("tcp", server)
	if err != nil {
		return nil, err
	}

	return tcpaddr, nil
}

func (sl *ServerList) Each(func(net.Addr) error) error { return nil }
