// Copyright © 2021 Dmitry Stoletov <info@imega.ru>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Memcache consistent hash portable for Go
// see: memcache_consistent_hash.c
//
package phpmemcache

import (
	"errors"
	"hash/crc32"
	"net"
	"sort"
	"strconv"
	"strings"

	"github.com/bradfitz/gomemcache/memcache"
)

const (
	virtualCount             = 160
	consistentBuckets uint32 = 1024
)

type ServerList struct {
	Buckets []*bucket
}

func (sl *ServerList) PickServer(key string) (net.Addr, error) {
	hashKey := hash(key)

	v := hashKey % consistentBuckets
	if v >= consistentBuckets {
		return nil, errors.New("not exist")
	}

	return sl.Buckets[v].Addr, nil
}

func (sl *ServerList) Each(func(net.Addr) error) error { return nil }

type bucket struct {
	Addr  net.Addr
	Point uint32
}

func New(servers ...string) (memcache.ServerSelector, error) {
	sort.Strings(servers)

	s := make([]net.Addr, len(servers))
	for idx, v := range servers {
		addr, err := str2NetAddr(v)
		if err != nil {
			return nil, err
		}

		s[idx] = addr
	}

	var n []*bucket
	for idx, v := range s {
		for i := 0; i < virtualCount; i++ {
			hashKey := v.String() + "-" + strconv.Itoa(i)
			n = append(n, &bucket{Addr: s[idx], Point: hash(hashKey)})
		}
	}

	sort.Slice(n, func(i, j int) bool { return n[i].Point < n[j].Point })

	step := 0xFFFFFFFF / consistentBuckets
	sl := ServerList{}

	for i := uint32(0); i < consistentBuckets; i++ {
		b := consistent_find(n, step*i, 0, uint32(len(n)-1))
		sl.Buckets = append(sl.Buckets, b)
	}

	return &sl, nil
}

func str2NetAddr(v string) (net.Addr, error) {
	if strings.Contains(v, "/") {
		addr, err := net.ResolveUnixAddr("unix", v)
		if err != nil {
			return nil, err
		}

		return newStaticAddr(addr), nil
	}

	addr, err := net.ResolveTCPAddr("tcp", v)
	if err != nil {
		return nil, err
	}

	return newStaticAddr(addr), nil
}

type staticAddr struct {
	ntw, str string
}

func (s *staticAddr) Network() string { return s.ntw }
func (s *staticAddr) String() string  { return s.str }

func newStaticAddr(a net.Addr) net.Addr {
	return &staticAddr{
		ntw: a.Network(),
		str: a.String(),
	}
}

func hash(str string) uint32 {
	h := crc32.NewIEEE()
	h.Write([]byte(str))

	return h.Sum32()
}

func consistent_find(state []*bucket, point, lo, hi uint32) *bucket {
	if point <= state[lo].Point || point > state[hi].Point {
		return state[lo]
	}

	mid := lo + (hi-lo)/2

	if mid == 0 {
		return state[mid]
	}

	if point <= state[mid].Point && point > state[mid-1].Point {
		return state[mid]
	}

	if point > state[mid].Point {
		return consistent_find(state, point, mid+1, hi)
	}

	return consistent_find(state, point, lo, mid-1)
}
